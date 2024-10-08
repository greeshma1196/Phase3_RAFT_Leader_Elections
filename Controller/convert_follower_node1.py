import json
import socket
import traceback
import time

# Wait following seconds below sending the controller request
time.sleep(5)

# Read Message Template
msg = json.load(open("Message.json"))

# Initialize
sender = socket.gethostbyname('localhost')
target = "Node1"
port = 5555

# Request
msg['sender_name'] = sender
# Developer's note: Update the request to either of the following:
    # CONVERT_FOLLOWER
    # TIMEOUT
    # SHUTDOWN
    # LEADER_INFO
    # STORE
    # RETRIEVE
msg['request'] = "SHUTDOWN" 
msg['entries'] = ['7']

# Socket Creation and Binding
skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
skt.bind((sender, port))


# Send Message
try:
    # Encoding and sending the message
    # Developer's note: Comment/Uncomment any of the following nodes with the request message
    # skt.sendto(json.dumps(msg).encode('utf-8'), (sender, 5001)) #node1
    # skt.sendto(json.dumps(msg).encode('utf-8'), (sender, 5002)) #node2
    skt.sendto(json.dumps(msg).encode('utf-8'), (sender, 5003)) #node3
    # skt.sendto(json.dumps(msg).encode('utf-8'), (sender, 5004)) #node4
    # skt.sendto(json.dumps(msg).encode('utf-8'), (sender, 5005)) #node5

    if msg['request'] == "STORE" or msg['request'] == "RETRIEVE" or msg['request'] == "LEADER_INFO":
        try:
            msg, addr = skt.recvfrom(1024)
            decoded_msg = json.loads(msg.decode('utf-8'))
            print(f"Message Received : {decoded_msg} From : {addr}")
        except:
            print(f"ERROR while fetching from socket : {traceback.print_exc()}")

    
except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

