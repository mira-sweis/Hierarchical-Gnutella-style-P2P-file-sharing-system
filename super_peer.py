# CS 485 PA3 - UIC Fall 2024
# Mira Sweis - ECE MS
#
# super_peer.py creates a super peer class responsible for handling queries
# and communication with leaf nodes mostly regarding queries
#
import socket
import json
import threading
import traceback
import os
HOST = 'localhost'

#
# Super Peer class
#
class SuperPeer:
    def __init__(self, peer_id, port, mode):
        self.peer_id = peer_id
        self.port = port
        self.mode = mode
        self.leaf_nodes = []  # {leaf_id: LeafNode}
        self.neighbor_peers = []  # [SuperPeer]
        self.registered_files = {}  # {file_name: [leaf_ids]}
        self.back_prop = []
        self.message_log = {}  # Tracks message IDs to prevent duplicate processing
        # ensures thread handling for multiple queries
        self.lock = threading.Lock()
        threading.Thread(target=self.start_server).start()

    # Initilizes server on port defined in JSON config file
    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((HOST, self.port))
        server_socket.listen()
        print(f"Super-peer {self.peer_id} server started on port {self.port}")

        while True:
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_connection, args=(client_socket, address)).start()

    # This is specifically for handling requests from leaf nodes
    # Either queries or file transfers
    def handle_connection(self, client_socket, address):
        data = client_socket.recv(1024).decode()
        request = json.loads(data)
        print(f'Request recieved at {self.peer_id}: {request}')
        response = []
        huh = []
        back_prop = []
        # print(f'message log: {self.message_log}')

        if request["type"] == "file_query":
            self.handle_query(request, response, back_prop)
            client_socket.send(json.dumps(response).encode())  
        elif request["type"] == "queryhit_back":
            huh = self.handle_backprop(response, back_prop)
        elif request["type"] == "INVALIDATION":
            self.handle_invalidation(request, client_socket)
        elif request["type"] == "register_files":
            self.registered_files.clear()
            for f in request["files"]:
                self.registered_files[f] = request["node_id"]
            # print(f'self.registered_files for {self.peer_id}: {self.registered_files}')
        elif request["type"] == "PULL":
            self.handle_pull_request(request, client_socket)
        elif request["type"] == "CLEANUP":
            self.cleanup(request)
        client_socket.close()

    # responsible for cleaning up super peer after a file removal
    def cleanup(self, message):
        file_name = message["file_name"]
        # print(f'message ID: {self.message_log}')
        msg_id = message["msg_id"]

        if msg_id in self.message_log:
            del self.message_log[msg_id]
            print(f"Super-peer {self.peer_id} removed {msg_id} for {file_name} from message log.")
        
        else:
            print(f"Message ID {msg_id} not found in message log. No cleanup needed.")
        self.propagate_cleanup(message)
        
    def propagate_cleanup(self, message):
        msg_id = message["msg_id"]
        for neighbor in self.neighbor_peers:
            if msg_id in neighbor.message_log:
                del neighbor.message_log[msg_id]
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((HOST, neighbor.port))
                        s.send(json.dumps(message).encode())
                    print(f"Super-peer {self.peer_id} propagated CLEANUP for {message['file_name']} to neighbor {neighbor.peer_id}.")
                except Exception as e:
                    print(f"Error propagating CLEANUP to neighbor {neighbor.peer_id}: {e}")



    def handle_pull_request(self, message, client_socket):
        filename = message["file_name"]
        cached_version = message["cached_version"]

        # Check if the file is registered
        origin_leaf_node = message["origin_node"]

        if not origin_leaf_node:
            # File not found in registry
            response = {"status": "STALE"}
            client_socket.send(json.dumps(response).encode())
            return

        # Query the origin leaf node for the current version
        origin_port = 6000 + int(origin_leaf_node[1:])  # Assuming ports are derived from leaf node IDs
        # print(f'origin port: {origin_port}')
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                pull_query = {
                    "type": "VERSION_REQUEST",
                    "file_name": filename
                }
                s.connect((HOST, origin_port))
                s.send(json.dumps(pull_query).encode())
                
                # Receive the response from the origin leaf node
                origin_response = json.loads(s.recv(1024).decode())
                origin_version = origin_response.get("version")

            # Compare the cached version with the origin version
            if cached_version < origin_version:
                response = {"status": "STALE", "file_name": filename}
            else:
                response = {"status": "VALID", "file_name": filename}

        except Exception as e:
            print(f"Error querying origin leaf node for {filename}: {e}")
            response = {"status": "STALE"}
            traceback.print_exc()

        # Send the response back to the requesting leaf node
        client_socket.send(json.dumps(response).encode())

    # responsible for handling invalidation requests
    def handle_invalidation(self, message, client_socket):
        origin = message["origin_server_id"]

        # Avoid processing duplicate invalidation messages
        if message["msg_id"] in self.message_log:
            return
        self.message_log[message["msg_id"]] = True

        filename = message["file_name"]

        # Check if the file is registered
        if filename in self.registered_files and self.registered_files[filename] != origin:
            # Get the list of leaf nodes holding this file
            leaf_nodes_with_file = self.registered_files[filename]
            # Notify leaf nodes to discard the file
            self.send_invalidation_to_leaf_node(leaf_nodes_with_file, message, client_socket)

        # Propagate invalidation to neighbors
        self.propagate_invalidation(message)

    # tell leave nodes to discard file
    def send_invalidation_to_leaf_node(self, leaf_node_id, message, client_socket):
        if leaf_node_id in self.leaf_nodes:
            port = 6000 + int(leaf_node_id[1:])
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((HOST, port))
                    s.send(json.dumps(message).encode())
                print(f"Super-peer {self.peer_id} sent invalidation for {message['file_name']} to Leaf-node {leaf_node_id}.")
            except ConnectionRefusedError:
                print(f"Super-peer {self.peer_id} could not connect to Leaf-node {leaf_node_id}. Skipping invalidation.")
                traceback.print_exc()
            except Exception as e:
                print(f"Error while sending invalidation to Leaf-node {leaf_node_id}: {e}")
                traceback.print_exc()

    # send invalidation message to neighboring peers
    def propagate_invalidation(self, message):
        for neighbor in self.neighbor_peers:
            # print(f'sending to neighbor: {neighbor.port}')
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST, neighbor.port))
                s.send(json.dumps(message).encode())
        print(f"Super-peer {self.peer_id} propagated invalidation for {message['file_name']} to neighbors.")

        # recursivly check super nodes for the specific query
        # with the TTL restraint

    def handle_query(self, query, response, back_prop):
        ttl = query["TTL"]
        message_id = query["message_id"]
        leaf_node, file_name = query["message_id"].split("_")

        # Check locally for file
        back_prop.append(self.peer_id)
        # print(f'self.backprop: {self.back_prop}')
        # print(f"self.registered_files: {self.registered_files}")

        if file_name in self.registered_files:
            # print(f'queryhit found at {self.peer_id}')
            response.append({"type" : "QueryHit",
                             "leaf_node": self.registered_files[file_name], 
                             "file_name": file_name,
                             "Done": False})
            back_prop.append('Query_Hit!')
        # print(f'reponse in handle query{response}')

        # Forward query if TTL >= 0
        if ttl >= 0 and message_id not in self.message_log:
            self.message_log[message_id] = query["origin"]
            query["TTL"] -= 1
            self.propagate_query(query, response, back_prop)

    # looks for next super peer that hasnt been checked yet in neighbor peers
    def propagate_query(self, query, response, back_prop):
        m_id = query["message_id"]
        for neighbor in self.neighbor_peers:
            if m_id not in neighbor.message_log:
                # neighbor.message_log[m_id] = query["origin"]
                neighbor.handle_query(query, response, back_prop)

    # back propagation functionality
    def handle_backprop(self, query, back_prop):
        # m_id = query["message_id"]
        sp = query["super_node"]
        if back_prop[-1] == "Query_Hit!":
            for n in self.neighbor_peers:
                if n.peer_id == sp:
                    query["Done"] == True
                    break
                                     

    # adds leaf nodes to superpeers
    def add_leaf_node(self, leaf_node):
        self.leaf_nodes[leaf_node.node_id] = leaf_node
        for file in leaf_node.files:
            if file not in self.registered_files:
                self.registered_files[file] = []
            self.registered_files[file].append(leaf_node.node_id)
        print(f"Leaf-node {leaf_node.node_id} registered with Super-peer {self.peer_id}.")
