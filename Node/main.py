import json
import threading
import os
import time
from random import randrange
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
commit_index = 0
leader_id = ""
next_index = {}
match_index = {}
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
REQUEST_STORE = "STORE"
REQUEST_RETRIEVE = "RETRIEVE"
REQUEST_RESTART = "RESTART"
RESPONSE_VOTE_DENIED = "VOTE_DENIED"
RESPONSE_VOTE_GRANTED = "VOTE_ACK"
RESPONSE_ENTRY_APPENDED = "ENTRY_APPENDED"
RESPONSE_APPEND_ENTRY_FAILED = "ENTRY_APPEND_FAILED"
RESPONSE_LEADER_INFO = "LEADER_INFO"
RESPONSE_SHUTDOWN_NODE = "RESPONSE_SHUTDOWN_NODE"
RESPONSE_RETRIEVE = "RETRIEVE"
RESPONSE_RESTART_NODE = "RESPONSE_RESTART_NODE"

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

        #if the check fails then it will send a heartbeat to all nodes except itself
        
        for node in all_nodes:

            if node == my_node_id:
                continue

            if node not in next_index:
                next_index[node] = len(log)

            if node not in match_index:
                match_index[node] = -1

            if len(log) == 0:
                prev_log_index = -1
                prev_log_term = -1
            else:
                prev_log_index = next_index[node]-1
                prev_log_term = log[next_index[node]-1]['term']

            message = {
                "sender_name": my_node_id,
                "request": REQUEST_APPEND_ENTRY,
                "term": current_term,
                "previous_log_index": prev_log_index,
                "previous_log_term": prev_log_term,
                "entries": log[next_index[node]:next_index[node]+5],
                "commit_index": commit_index
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

        if len(log) > 0:
            last_log_index = len(log)-1
            last_log_term = log[-1]['term']
        else:
            last_log_index = -1
            last_log_term = -1

        #send a request to get vote from all the other nodes in the cluster
        for node in all_nodes:

            #to not send message to itself
            if node == my_node_id:
                continue

            message = {
                "sender_name": my_node_id,
                "request": REQUEST_GET_VOTE,
                "term": current_term,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            }

            message = json.dumps(message).encode()
            try:
                socket.sendto(message, (node, port))
            except OSError:
                pass

def handle_request_get_vote(candidate_term, candidate_id, socket, last_log_index, last_log_term):

    global current_term, voted_for
    
    #check if node's term if greater than or equal to the candidate node's term
    if current_term >= candidate_term:
        print(f"{my_node_id} denied vote for {candidate_id} because current term is {current_term} and candidate term is {candidate_term}")
        message = {
            "sender_name": my_node_id,
            "request": RESPONSE_VOTE_DENIED,
            "term": current_term
        } 
        
        message = json.dumps(message).encode()
        return socket.sendto(message, (candidate_id, port))
    
    #check if the node has voted for any node other than the candidate node
    if voted_for and voted_for != candidate_id:
        print(f"{my_node_id} denied vote for {candidate_id} because follower node has voted for {voted_for} and candidate id is {candidate_id}")
        message = {
            "sender_name": my_node_id,
            "request": RESPONSE_VOTE_DENIED,
            "term": current_term
        } 
        
        message = json.dumps(message).encode()
        return socket.sendto(message, (candidate_id, port))
    
    if len(log) > 0:
        follower_last_log_index = len(log)-1
        follower_last_log_term = log[-1]['term']
    else:
        follower_last_log_index = -1
        follower_last_log_term = -1

    if last_log_index < follower_last_log_index or last_log_term < follower_last_log_term:
        print(f"{my_node_id} denied vote for {candidate_id} because follower node has more log entries than leader node")
        message = {
            "sender_name": my_node_id,
            "request": RESPONSE_VOTE_DENIED,
            "term": current_term
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
        "term": current_term
    }

    message = json.dumps(message).encode()
    return socket.sendto(message, (candidate_id, port))

def handle_response_vote_granted(socket):
    global votes_received, current_raft_state, leader_id, election_timeout_interval, next_index, match_index

    votes_received += 1

    #check if the votes received is less than half of the total number of nodes
    if votes_received < len(all_nodes)/2:
        return

    #if the votes received are greater than half the total number of nodes in the cluster, then set the raft state of the candidate node to leader
    #send append entry request as the new leader
    current_raft_state = RAFT_LEADER
    leader_id = my_node_id
    next_index = {}
    match_index = {}

    print(f"{my_node_id} is the new {current_raft_state} and the term is {current_term}, votes received {votes_received} and the number of nodes are {len(all_nodes)}")

def handle_request_append_entry(leader_term, leader_node_id, socket, previous_log_index, previous_log_term, entries, leader_commit_index):

    global voted_for, last_heartbeat_received, leader_id, current_raft_state, current_term, commit_index, log
    
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
    
    if previous_log_index > -1 and previous_log_term > -1:

        #check if follower had a log entry at previous_log_index
        if previous_log_index > len(log)-1:
            print(f'Previous Log Index: {previous_log_index} \n {len(log)-1}')
            #print(f"1. Follower node id {my_node_id} \n Log: {log} \n Entries: {entries}")
            message = {
                "sender_name": my_node_id,
                "request": RESPONSE_APPEND_ENTRY_FAILED,
                "term": current_term,
                "success": False,
                "next_index": len(log),
                "match_index": len(log)-1
            } 
            message = json.dumps(message).encode()
            try:
                socket.sendto(message, (leader_node_id, port))
            except:
                pass
            return 

        #log entry exists at previous_log_index
        if log[previous_log_index]['term'] != previous_log_term:  
            print(f"2. Follower node id {my_node_id} \n Log: {log} \n Entries: {entries}") 
            del log[previous_log_index:]
            message = {
                "sender_name": my_node_id,
                "request": RESPONSE_APPEND_ENTRY_FAILED,
                "term": current_term,
                "success": False,
                "next_index": len(log),
                "match_index": len(log)-1
            } 
            message = json.dumps(message).encode()
            try:
                socket.sendto(message, (leader_node_id, port))
            except:
                pass
            return 

    log = log + entries
    #print(f"Follower node id {my_node_id} \n Log: {log} \n Entries: {entries}")
    follower_next_index = len(log)
    follower_match_index = len(log)-1
    commit_index = min(leader_commit_index, len(log)-1)    

    message = {
        "sender_name": my_node_id,
        "request": RESPONSE_ENTRY_APPENDED,
        "term": current_term,
        "success": True,
        "next_index": follower_next_index,
        "match_index": follower_match_index,
        "commit_index": commit_index
    } 
        
    message = json.dumps(message).encode()
    try:
        socket.sendto(message, (leader_node_id, port))
    except:
        pass

def handle_response_entry_appended(follower_next_index, follower_nodeid, follower_match_index):

    global commit_index
    
    next_index[follower_nodeid] = follower_next_index
    match_index[follower_nodeid] = follower_match_index

    #greatest common log index; common is defined as being present on at least half of all nodes
    for match_index_i in match_index.values():
        counter = 1
        N = match_index_i
        for match_index_j in match_index.values():
            if N <= match_index_j:
                counter += 1
        if counter >= len(all_nodes)/2 and N > commit_index and log[N]['term']==current_term:
            commit_index = N
            break
        
def handle_response_entry_failed(follower_next_index, follower_nodeid):
    global next_index

    next_index[follower_nodeid] = follower_next_index

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

    if len(log) > 0:
        last_log_index = len(log)-1
        last_log_term = log[-1]['term']
    else:
        last_log_index = -1
        last_log_term = -1

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
            "last_log_index": last_log_index,
            "last_log_term": last_log_term
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
            "term": current_term
        }
        message = json.dumps(message).encode()
        socket.sendto(message, (node, port))

def handle_response_shutdown(node):

    global all_nodes

    all_nodes.remove(node)

def handle_request_restart(node, socket):

    global all_nodes,  __request_shutdown_check

    print(f"Restarting node: {node}")
    all_nodes.append(node)

    __request_shutdown_check = False

    for node in all_nodes:
        if node == my_node_id:
            continue
        message = {
            "sender_name": my_node_id,
            "request": RESPONSE_RESTART_NODE,
            "term": current_term
        }
        message = json.dumps(message).encode()
        socket.sendto(message, (node, port))

def handle_response_restart(node):

    global all_nodes

    all_nodes.append(node)

def handle_request_store(socket, entries, controller_address):
    global log

    if current_raft_state == RAFT_LEADER:
        for i in range(len(entries)):
            log.append({'term':current_term, 'value':entries[i]})
        print(f'Leader id is {my_node_id} and entries are {log}')
        message = {
            "sender_name": my_node_id,
            "success": True
        }

        message = json.dumps(message).encode()
        socket.sendto(message, controller_address)

    else:
        print(f"Node: {my_node_id} \n Leader: {leader_id} \n Term: {current_term} \n Current raft state: {current_raft_state}")
        message = {
            "sender_name": my_node_id,
            "request": RESPONSE_LEADER_INFO,
            "term": None,
            "key": "LEADER",
            "value": leader_id
        }

        message = json.dumps(message).encode()
        socket.sendto(message, controller_address)
    
def handle_request_retrieve(controller_address, socket):
    if current_raft_state == RAFT_LEADER:
        print(f"Leader: {leader_id} \n Term: {current_term} \n Current raft state: {current_raft_state} \n Logs: {log}")

        message = {
            "sender_name": my_node_id,
            "request": RESPONSE_RETRIEVE,
            "term": current_term,
            "key": "COMMITTED_LOGS",
            "value": log
        }

        message = json.dumps(message).encode()
        socket.sendto(message, controller_address)
    
    else:
        print(f"Node: {my_node_id} \n Leader: {leader_id} \n Term: {current_term} \n Current raft state: {current_raft_state} \n Logs: {log}")
        message = {
            "sender_name": my_node_id,
            "request": RESPONSE_LEADER_INFO,
            "term": None,
            "key": "LEADER",
            "value": leader_id
        }

        message = json.dumps(message).encode()
        socket.sendto(message, controller_address)

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
        leader_commit_index = data.get("commit_index", None)
        success = data.get("success", None)
        next_index = data.get("next_index", None)
        match_index = data.get("match_index", None)
        last_log_index = data.get("last_log_index", None)
        last_log_term = data.get("last_log_term", None)

        #check for thre request being passed
        if request == REQUEST_GET_VOTE:
            if last_log_index is None or last_log_term is None:
                print(self.request[0].strip())
            return handle_request_get_vote(term, sender_name, socket, last_log_index, last_log_term)

        if request == RESPONSE_VOTE_GRANTED:
            return handle_response_vote_granted(socket)
        
        if request == REQUEST_APPEND_ENTRY:
            return handle_request_append_entry(term, sender_name, socket, previous_log_index, previous_log_term, entries, leader_commit_index)
        
        if request == RESPONSE_APPEND_ENTRY_FAILED:
            return handle_response_entry_failed(next_index, sender_name)
        
        if request == RESPONSE_ENTRY_APPENDED:
            return handle_response_entry_appended(next_index, sender_name, match_index)
        
        if request == REQUEST_CONVERT_FOLLOWER:
            #set delay to prevent abrupt behaviour
            return handle_request_convert_follower()
        
        if request == REQUEST_LEADER_INFO:
            return handle_request_leader_info(controller_address, socket)

        if request == REQUEST_SHUTDOWN:
            return handle_request_shutdown(socket)
        
        if request == RESPONSE_SHUTDOWN_NODE:
            return handle_response_shutdown(sender_name)

        if request == REQUEST_TIMEOUT:
            return handle_request_timeout(socket)

        if request == REQUEST_STORE:
            return handle_request_store(socket, entries, controller_address)
        
        if request == REQUEST_RETRIEVE:
            return handle_request_retrieve(controller_address, socket)     
        
        if request == REQUEST_RESTART:
            return handle_request_restart(sender_name, socket)
        
        if request == RESPONSE_RESTART_NODE:
            return handle_response_restart(sender_name)
            
if __name__ == "__main__":

    with socketserver.ThreadingUDPServer(('0.0.0.0', port), RequestHandler) as server:
        election_timeout_thread = threading.Thread(target=election_timeout, args=[server.socket])
        send_heartbeats_thread = threading.Thread(target=send_heartbeats, args=[server.socket])
        election_timeout_thread.daemon = True
        send_heartbeats_thread.daemon = True
        election_timeout_thread.start()
        send_heartbeats_thread.start()
        server.serve_forever()