from concurrent.futures import thread
from email import message
import json
from logging import shutdown
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

RAFT_LEADER = "leader"
RAFT_FOLLOWER = "follower"
RAFT_CANDIDATE = "candidate"

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


def handle_request_get_vote(candidate_term, candidate_id, socket):

    global current_term, voted_for
    
    if current_term >= candidate_term:
        print(f"{my_node_id} denied vote for {candidate_id} because current term is {current_term} and candidate term is {candidate_term}")
        message = {
            "sender_name": my_node_id,
            "request": RESPONSE_VOTE_DENIED,
            "term": current_term,
            "previous_log_index": prev_log_index,
            "previous_log_term": prev_log_term,
            "entries": log
        } 
        
        message = json.dumps(message).encode()
        return socket.sendto(message, (candidate_id, port))
    
    if voted_for and voted_for != candidate_id:
        print(f"{my_node_id} denied vote for {candidate_id} because follower node has voted for {voted_for} and candidate id is {candidate_id}")
        message = {
            "sender_name": my_node_id,
            "request": RESPONSE_VOTE_DENIED,
            "term": current_term,
            "previous_log_index": prev_log_index,
            "previous_log_term": prev_log_term,
            "entries": log
        } 
        
        message = json.dumps(message).encode()
        return socket.sendto(message, (candidate_id, port))
    
    print(f"Vote granted by {my_node_id} to {candidate_id} and the term is {candidate_term}")
    
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
    global votes_received, current_raft_state

    votes_received += 1

    if votes_received < len(all_nodes)/2:
        return

    current_raft_state = RAFT_LEADER

    print(f"{my_node_id} is the new {current_raft_state} and the term is {current_term}, votes received {votes_received} and {len(all_nodes)/2}")


    message = {
        "sender_name": my_node_id,
        "request": REQUEST_APPEND_ENTRY,
        "term": current_term,
        "previous_log_index": prev_log_index,
        "previous_log_term": prev_log_term,
        "entries": log
    }

    message = json.dumps(message).encode()
    return socket.sendto(message, (my_node_id, port))

def handle_request_append_entry(leader_term, leader_id, socket):

    global voted_for, last_heartbeat_received
    
    if current_term > leader_term:
        return
    
    last_heartbeat_received = time.time()

    #current election has ended
    #reseting voted_for so that it can cast a new vote in the next election term
    voted_for = ""

    message = {
        "sender_name": my_node_id,
        "request": RESPONSE_ENTRY_APPENDED,
        "term": current_term,
        "previous_log_index": prev_log_index,
        "previous_log_term": prev_log_term,
        "entries": log
    } 
        
    message = json.dumps(message).encode()
    return socket.sendto(message, (leader_id, port))

def handle_request_convert_follower():
    global current_raft_state, voted_for

    current_raft_state = RAFT_FOLLOWER
    voted_for = ""

    print(f"{my_node_id} is in {current_raft_state}")

def handle_request_leader_info():

    if current_raft_state != RAFT_LEADER:
        return 
    
    return {"Raft_State":current_raft_state, "Node":my_node_id}
  
def handle_request_timeout():

    global current_raft_state, voted_for

    current_raft_state = RAFT_FOLLOWER
    voted_for = ""


class RequestHandler(socketserver.DatagramRequestHandler):
    def handle(self):
        data = self.request[0].strip()
        socket = self.request[1]
        if len(data) == 0:
            return

        data = json.loads(data)

        if data["request"] == REQUEST_CONVERT_FOLLOWER:
            print(f"Data: {data}")

        sender_name = data.get("sender_name", None)
        term = data.get("term", None)
        request = data.get("request", None)
        previous_log_index = data.get("previous_log_index", None)
        previous_log_term = data.get("previous_log_term", None)
        entries = data.get("entries", None)

        if request == REQUEST_GET_VOTE:
            return handle_request_get_vote(term, sender_name, socket)

        if request == RESPONSE_VOTE_GRANTED:
            return handle_response_vote_granted(socket)
        
        if request == REQUEST_APPEND_ENTRY:
            return handle_request_append_entry(term, sender_name, socket)
        
        if request == REQUEST_CONVERT_FOLLOWER:
            #set a delay to prevent abrupt behaviour
            time.sleep(election_timeout_interval)
            return handle_request_convert_follower()
        
        if request == REQUEST_LEADER_INFO:
            leader_info = {}
            leader_info = handle_request_leader_info()
            print (f"{leader_info['Node']} is the new {leader_info['Raft_State']}")

        if request == REQUEST_SHUTDOWN:
            print(f"Shutting down service {my_node_id}")
            self.server.shutdown()

        if request == REQUEST_TIMEOUT:
            return handle_request_timeout()
            

def send_heartbeats(socket):

    while True:
        
        if current_raft_state != RAFT_LEADER:
            continue

        for node in all_nodes:

            if node == my_node_id:
                continue

            message = {
                "sender_name": my_node_id,
                "request": REQUEST_APPEND_ENTRY,
                "term": current_term,
                "previous_log_index": prev_log_index,
                "previous_log_term": prev_log_term,
                "entries": log
            }

            

            message = json.dumps(message).encode()
            socket.sendto(message, (node, port))
        
        time.sleep(heartbeat_interval)

def election_timeout(socket):

    global current_term, current_raft_state, votes_received, voted_for

    print(f"Node ID {my_node_id} has an election timeout of {election_timeout_interval}")
    

    while True:
        
        time.sleep(election_timeout_interval)


        if current_raft_state == RAFT_LEADER:
            continue
        
        if voted_for:
            continue
        
        print(f"Last Heartbeat Received: {last_heartbeat_received}")
        if time.time() - last_heartbeat_received < election_timeout_interval:
            continue
        
        current_raft_state = RAFT_CANDIDATE
        current_term += 1
        voted_for = my_node_id
        votes_received = 1

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


if __name__ == "__main__":

    with socketserver.ThreadingUDPServer(('0.0.0.0', port), RequestHandler) as server:
        threading.Thread(target=election_timeout, args=[server.socket]).start()
        threading.Thread(target=send_heartbeats, args=[server.socket]).start()
        server.serve_forever()


    