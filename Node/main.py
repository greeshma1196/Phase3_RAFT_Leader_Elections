import json
import sys
import threading
import os
import time
from random import randrange
import socket
import traceback
import socketserver

heartbeat_interval = int(os.getenv('HEARTBEAT_INTERVAL'))/1000
my_node_id = os.getenv('NODE_ID')
all_nodes = os.getenv('ALL_NODES').split(',')
port = int(os.getenv('PORT'))

election_timeout_interval = (3*heartbeat_interval) + (randrange(500,1000)/1000)

RAFT_LEADER = "LEADER"
RAFT_FOLLOWER = "FOLLOWER"
RAFT_CANDIDATE = "CANDIDATE"

log = []
prev_log_index = -1
prev_log_term = -1
leader_id = ""
current_term = 0
last_heartbeat_received = 0
voted_for = ""
votes_received = 0
current_raft_state = RAFT_FOLLOWER

REQUEST_GET_VOTE = "GET_VOTE"
REQUEST_CONVERT_FOLLOWER = "CONVERT_FOLLOWER"
REQUEST_APPEND_ENTRY = "APPEND_RPC"
REQUEST_LEADER_INFO = "LEADER_INFO"
REQUEST_TIMEOUT = "TIMEOUT"
REQUEST_SHUTDOWN = "SHUTDOWN"
RESPONSE_VOTE_DENIED = "VOTE_DENIED"
RESPONSE_VOTE_GRANTED = "VOTE_ACK"
RESPONSE_ENTRY_APPENDED = "ENTRY_APPENDED"
RESPONSE_LEADER_INFO = "RESPONSE_LEADER_INFO"
RESPONSE_SHUTDOWN_NODE = "RESPONSE_SHUTDOWN_NODE"

__wait_check = False
__request_shutdown_check = False

def send_heartbeats(socket):

    while True:
        
        if __request_shutdown_check:
            time.sleep(heartbeat_interval)
            break

        #check if the raft state is not leader
        if current_raft_state != RAFT_LEADER:
            time.sleep(heartbeat_interval)
            continue
        
        request_id = f"{my_node_id}_{current_term}"
        start_time = time.time_ns()

        #if the check fails then it will send a heartbeat to all nodes except itself
        for node in all_nodes:

            if node == my_node_id:
                continue

            message = {
                "sender_name": my_node_id,
                "request": REQUEST_APPEND_ENTRY,
                "term": current_term,
                "previous_log_index": prev_log_index,
                "previous_log_term": prev_log_term,
                "entries": log,
                "request_id": request_id,
                "start_time": start_time
            }

            message = json.dumps(message).encode()
            try:
                socket.sendto(message, (node, port))
            except OSError:
                pass
        
        time.sleep(heartbeat_interval)

def election_timeout(socket):   

    global current_term, current_raft_state, votes_received, voted_for, election_timeout_interval, __wait_check

    print(f"Node ID {my_node_id} has an election timeout of {election_timeout_interval}")   

    while True:

        time.sleep(election_timeout_interval)

        if __request_shutdown_check:
            time.sleep(election_timeout_interval)
            break

        if __wait_check: 
            __wait_check = False
            time.sleep(election_timeout_interval)
            continue

        #check if raft state is leader
        if current_raft_state == RAFT_LEADER:
            time.sleep(election_timeout_interval)
            continue
        
        #if the previous check fails, then check to see if you have received a heartbeat within the election timeout interval
        if time.time() - last_heartbeat_received < election_timeout_interval:
            time.sleep(election_timeout_interval)
            continue
        
        #if all the check fail, set the state to candidate state
        #update the current term of the node
        #make the node vote for itself and increment the votes_received
        current_raft_state = RAFT_CANDIDATE
        current_term += 1
        voted_for = my_node_id
        votes_received = 1

        request_id = f"{my_node_id}_{current_term}"
        start_time = time.time_ns()

        #send a request to get vote from all the other nodes in the cluster
        for node in all_nodes:

            #to not send message to itself
            if node == my_node_id:
                continue
            
            message = {
                "sender_name": my_node_id,
                "request": REQUEST_GET_VOTE,
                "term": current_term,
                "previous_log_index": prev_log_index,
                "previous_log_term": prev_log_term,
                "entries": log,
                "request_id": request_id,
                "start_time": start_time
            }

            message = json.dumps(message).encode()
            try:
                socket.sendto(message, (node, port))
            except OSError:
                pass

def handle_request_get_vote(candidate_term, candidate_id, socket, request_id, start_time):

    global current_term, voted_for
    
    #check if node's term if greater than or equal to the candidate node's term
    if current_term >= candidate_term:
        print(f"{my_node_id} denied vote for {candidate_id} because current term is {current_term} and candidate term is {candidate_term}")
        message = {
            "sender_name": my_node_id,
            "request": RESPONSE_VOTE_DENIED,
            "term": current_term,
            "previous_log_index": prev_log_index,
            "previous_log_term": prev_log_term,
            "entries": log,
            "request_id": request_id,
            "start_time": start_time
        } 
        
        message = json.dumps(message).encode()
        return socket.sendto(message, (candidate_id, port))
    
    #check if the node has voted for any node other than the candidate node
    if voted_for and voted_for != candidate_id:
        print(f"{my_node_id} denied vote for {candidate_id} because follower node has voted for {voted_for} and candidate id is {candidate_id}")
        message = {
            "sender_name": my_node_id,
            "request": RESPONSE_VOTE_DENIED,
            "term": current_term,
            "previous_log_index": prev_log_index,
            "previous_log_term": prev_log_term,
            "entries": log,
            "request_id": request_id,
            "start_time": start_time
        } 
        
        message = json.dumps(message).encode()
        return socket.sendto(message, (candidate_id, port))
    
    print(f"Vote granted by {my_node_id} to {candidate_id} and the term is {candidate_term}")
    
    #if the above checks fail then the node will grant a vote
    #it will update it's term to the candidate node's term
    current_term = candidate_term
    voted_for = candidate_id

    message = {
        "sender_name": my_node_id,
        "request": RESPONSE_VOTE_GRANTED,
        "term": current_term,
        "previous_log_index": prev_log_index,
        "previous_log_term": prev_log_term,
        "entries": log
    }

    message = json.dumps(message).encode()
    return socket.sendto(message, (candidate_id, port))

def handle_response_vote_granted(socket):
    global votes_received, current_raft_state, leader_id, election_timeout_interval

    votes_received += 1

    #check if the votes received is less than half of the total number of nodes
    if votes_received < len(all_nodes)/2:
        return

    #if the votes received are greater than half the total number of nodes in the cluster, then set the raft state of the candidate node to leader
    #send append entry request as the new leader
    current_raft_state = RAFT_LEADER
    leader_id = my_node_id

    print(f"{my_node_id} is the new {current_raft_state} and the term is {current_term}, votes received {votes_received} and the number of nodes are {len(all_nodes)}")

def handle_request_append_entry(leader_term, leader_node_id, socket, request_id, start_time):

    global voted_for, last_heartbeat_received, leader_id, current_raft_state, current_term
    
    #if the node's term is greater than the leader's term then it will not accept any entry request from the leader
    if current_term > leader_term:
        return
    
    leader_id = leader_node_id
    current_term = leader_term
    #current election has ended
    last_heartbeat_received = time.time()

    if current_raft_state != RAFT_FOLLOWER:
        current_raft_state = RAFT_FOLLOWER

    #reseting voted_for so that it can cast a new vote in the next election term
    voted_for = ""

    message = {
        "sender_name": my_node_id,
        "request": RESPONSE_ENTRY_APPENDED,
        "term": current_term,
        "previous_log_index": prev_log_index,
        "previous_log_term": prev_log_term,
        "entries": log,
        "request_id": request_id,
        "start_time": start_time
    } 
        
    message = json.dumps(message).encode()
    try:
        socket.sendto(message, (leader_node_id, port))
    except:
        pass

def handle_request_convert_follower():
    global current_raft_state, voted_for, last_heartbeat_received, __wait_check

    time.sleep(randrange(500,1000)/1000)
    #set the raft state of the node to follower and reset voted_for so that it can cast a new vote in the next election term
    current_raft_state = RAFT_FOLLOWER
    voted_for = ""
    __wait_check = True

    print(f"{my_node_id} is in {current_raft_state}")

def handle_request_leader_info(controller_address, socket):
    
    print(f"Node: {my_node_id} \n Leader: {leader_id} \n Term: {current_term} \n Current raft state: {current_raft_state}")
    message = {
        "sender_name": my_node_id,
        "request": RESPONSE_LEADER_INFO,
        "term": current_term,
        "previous_log_index": prev_log_index,
        "previous_log_term": prev_log_term,
        "entries": log,
        "key": "LEADER",
        "value": leader_id
    }

    message = json.dumps(message).encode()
    socket.sendto(message, controller_address)
  
def handle_request_timeout(socket):

    global current_raft_state, voted_for, votes_received, current_term

    current_raft_state = RAFT_CANDIDATE
    current_term += 1
    voted_for = my_node_id
    votes_received = 1

        #send a request to get vote from all the other nodes in the cluster
        #print(f"All nodes in election timeout:{all_nodes}")
    for node in all_nodes:

        #to not send message to itself
        if node == my_node_id:
            continue
        
        message = {
            "sender_name": my_node_id,
            "request": REQUEST_GET_VOTE,
            "term": current_term,
            "previous_log_index": prev_log_index,
            "previous_log_term": prev_log_term,
            "entries": log
        }

        message = json.dumps(message).encode()
        socket.sendto(message, (node, port))

def handle_request_shutdown(socket):

    global __request_shutdown_check, all_nodes

    all_nodes.remove(my_node_id)
    __request_shutdown_check = True
    print(f"Shutting down service {my_node_id} and the remianing nodes are {all_nodes}")
    for node in all_nodes:
        if node == my_node_id:
            continue
        message = {
            "sender_name": my_node_id,
            "request": RESPONSE_SHUTDOWN_NODE,
            "term": current_term,
            "previous_log_index": prev_log_index,
            "previous_log_term": prev_log_term,
            "entries": log
        }
        message = json.dumps(message).encode()
        socket.sendto(message, (node, port))

def handle_response_shutdown(node):

    global all_nodes

    all_nodes.remove(node)

class RequestHandler(socketserver.DatagramRequestHandler):
    def handle(self):

        #pre-process the data
        data = self.request[0].strip()
        socket = self.request[1]
        controller_address = self.client_address
       
        if len(data) == 0:
            return

        data = json.loads(data)

        sender_name = data.get("sender_name", None)
        term = data.get("term", None)
        request = data.get("request", None)
        previous_log_index = data.get("previous_log_index", None)
        previous_log_term = data.get("previous_log_term", None)
        entries = data.get("entries", None)
        request_id = data.get("request_id", None)
        start_time = data.get("start_time", None)

        if request_id is not None and request_id.startswith(str(my_node_id)):
            time_elapsed = time.time_ns() - start_time
            #print(f"Request-Response time for {my_node_id} is {time_elapsed/1000000}")


        #check for thre request being passed
        if request == REQUEST_GET_VOTE:
            return handle_request_get_vote(term, sender_name, socket, request_id, start_time)

        if request == RESPONSE_VOTE_GRANTED:
            return handle_response_vote_granted(socket)
        
        if request == REQUEST_APPEND_ENTRY:
            return handle_request_append_entry(term, sender_name, socket, request_id, start_time)
        
        if request == REQUEST_CONVERT_FOLLOWER:
            #set delay to prevent abrupt behaviour
            handle_request_convert_follower()
        
        if request == REQUEST_LEADER_INFO:
            handle_request_leader_info(controller_address, socket)

        if request == REQUEST_SHUTDOWN:
            handle_request_shutdown(socket)
        
        if request == RESPONSE_SHUTDOWN_NODE:
            handle_response_shutdown(sender_name)

        if request == REQUEST_TIMEOUT:
            return handle_request_timeout(socket)
            
if __name__ == "__main__":

    with socketserver.ThreadingUDPServer(('0.0.0.0', port), RequestHandler) as server:
        election_timeout_thread = threading.Thread(target=election_timeout, args=[server.socket])
        send_heartbeats_thread = threading.Thread(target=send_heartbeats, args=[server.socket])
        election_timeout_thread.daemon = True
        send_heartbeats_thread.daemon = True
        election_timeout_thread.start()
        send_heartbeats_thread.start()
        server.serve_forever()