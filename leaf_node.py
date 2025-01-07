#
# CS 485 PA3 - UIC Fall 2024
# Mira Sweis - ECE MS
#
# leaf_node.py responsible for initializing leafnode sockets, and handling all communication with
# super peers and other leaf nodes for downloads and file validity
#
import socket
import json
import threading
import time
import sys
import os
import traceback
from file import File

HOST = 'localhost'

#
# leaf node class for handling nodes
#
class LeafNode:
    def __init__(self, node_id, connected_super_peer_port, sp_name, files, file_names, port, mode):
        self.node_id = node_id
        self.connected_super_peer_port = connected_super_peer_port
        self.connected_super_peer_name = sp_name
        self.files = files
        self.file_names = file_names
        self.port = port
        self.mode = mode
        self.directory = os.path.join(os.getcwd(), node_id)  # Directory for this leaf node
        threading.Thread(target=self.start_server).start()
        if self.mode == 'pull':
            threading.Thread(target=self.poll_for_updates, args=(30,), daemon=True).start()

    # starts server socket on number given in config file
    def start_server(self):
        print(f'Starting leaf node {self.node_id}')
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((HOST, self.port))
        server_socket.listen()
        print(f"Leaf-node {self.node_id} server started on port {self.port} in {self.mode} mode")

        while True:
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_connection, args=(client_socket, address)).start()
    
    # handles file transfers to other leaf nodes if requested in message
    def handle_connection(self, client_socket, address):
        data = client_socket.recv(1024).decode()
        request = json.loads(data)
        # print(f'request recieved from super peer: {request}')

        file_path = os.path.join(self.directory, request["file_name"])  # Use directory for the file path

        if request["type"] == "file_transfer" and request["file_name"] in self.file_names:
            response = f"Sending file {request['file_name']} to {request['requester']}"
            client_socket.send(response.encode())
            print(f"Leaf-node {self.node_id} sent '{request['file_name']}' to Leaf-node {request['requester']}")
            
            # Open the file in the node's directory and send the actual file content
            with open(file_path, "rb") as file:
                while True:
                    file_data = file.read(1024)
                    if not file_data:
                        break
                    client_socket.send(file_data)
        elif request["type"] == "INVALIDATION" and request["file_name"] in self.file_names:
            # print('In handle invallidation')
            self.handle_invallidation(request)
            print(f'registered files at L{self.node_id}: {self.files} and {self.file_names}')
        elif request["type"] == "VERSION_REQUEST":
            self.handle_version_request(client_socket, request)
        else:
            client_socket.send("File not found".encode())
        client_socket.close()

    def handle_version_request(self, client_socket, request):
        filename = request["file_name"]
        if filename in self.files:
            file_struct = self.files[filename]
            response = {"version": file_struct.version}
            client_socket.send(json.dumps(response).encode())
            print(f"Leaf-node {self.node_id} sent version {file_struct.version} for {filename}.")
        else:
            response = {"version": 0}  # File not found or invalid
            client_socket.send(json.dumps(response).encode())
            print(f"Leaf-node {self.node_id} could not find {filename}.")

    # function to handle invalildation requests from superpeers
    def handle_invallidation(self, message):
        
        filename = message["file_name"]
        f = self.files[filename]

        if filename in self.file_names and f.copy:
            # Invalidate the file and update its metadata
            file_struct = self.files[filename]
            file_struct.invalidate()  # Mark the file as invalid

            # Optionally, remove the file from the directory if necessary
            file_path = os.path.join(self.directory, filename)
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Leaf-node {self.node_id} discarded invalid file {filename}.")

            # Deregister the file
            self.file_names.remove(filename)
            del self.files[filename]
            self.register_files()
            print(f"Leaf-node {self.node_id} deregistered {filename}.")

            cleanup_message = {
                "type": "CLEANUP",
                "file_name": filename,
                "msg_id": f"{self.node_id}_{filename}",
                "super_peer": self.connected_super_peer_name
            }
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST, self.connected_super_peer_port))
                s.send(json.dumps(cleanup_message).encode())
                
        else:
            print(f"Leaf-node {self.node_id} received invalidation for {filename}, but it does not exist locally.")
        return


    # registers files in leaf nodes at startup of leafnode
    def register_files(self):
        query = {"type": "register_files", "node_id": self.node_id, "files": self.file_names}
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, self.connected_super_peer_port))
            s.send(json.dumps(query).encode())
        print(f"Leaf-node {self.node_id} registered its files with super-peer at port {self.connected_super_peer_port}.")
  
    # for sending queries to super peers or file download requests
    def query_file(self, file_name, TTL=17):
        
        if file_name in self.file_names:
            print(f"Leaf-node {self.node_id} already has '{file_name}' locally.")
            return

        start_time = time.time()
        query = {
            "type": "file_query",
            "file_name": file_name,
            "TTL": TTL,
            "message_id": f"{self.node_id}_{file_name}",
            "origin": self.node_id,
            "super_node": self.connected_super_peer_name
        }
        reponse = []
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print('sending file query\n')
            s.connect((HOST, self.connected_super_peer_port))
            s.send(json.dumps(query).encode())
            # print(query)
            response = json.loads(s.recv(1024).decode())
            # print(response)

        # if response:
        # print(f'response from leaf node query {response}')

        duration = 0
        if response:
            duration = time.time() - start_time
            print(f"QueryHit! Leaf-node {self.node_id} found '{file_name}' on the following leaf nodes:")
            i = 0
            for hit in response:
                i += 1
                print(f"{i} - Leaf-node {hit['leaf_node']}")
            n = input("Select a Leaf Node to download from (e.g. 1, 2, 3): ")
            self.retrieve_file(response[int(n)-1]["leaf_node"], file_name)
            self.register_files()

            print(f"Query for '{file_name}' took {duration:.4f} seconds.")
       
       
    # function to request download from other leaf nodes
    def retrieve_file(self, leaf_node_id, file_name):
        print(f"Leaf-node {self.node_id} attempting to retrieve '{file_name}' from Leaf-node {leaf_node_id}")
        target_port = 6000 + int(leaf_node_id[1:]) 
        # print(f'Target port: {target_port}')
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, target_port))
            request = {"type": "file_transfer", "file_name": file_name, "requester": self.node_id}
            s.send(json.dumps(request).encode())

            # Receive the initial response
            response = s.recv(1024).decode()
            # print(f"Response from leaf node: {response}")

            if "Sending file" in response:
                # Save the file in the node's directory
                file_path = os.path.join(self.directory, file_name)
                with open(file_path, 'wb') as file:
                    while True:
                        data = s.recv(1024)
                        if not data:
                            break
                        file.write(data)

                self.file_names.append(file_name)
                self.files[file_name] = File(file_name, self.node_id, valid = True, og=leaf_node_id)
                self.files[file_name].is_copy()

                print(f"Leaf-node {self.node_id} successfully downloaded '{file_name}' from Leaf-node {leaf_node_id}.")

    # broadcasts invalidate message
    def push(self, filename, version):
        # message
        message = {
            "type": "INVALIDATION",
            "msg_id": f"{self.node_id}_{filename}_{version}",
            "origin_server_id": self.node_id,
            "file_name": filename,
            "version_number": version
        }

        # send message
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, self.connected_super_peer_port))
            s.send(json.dumps(message).encode())
        print(f"Leaf-node {self.node_id} pushed invalidation for {filename}.\n")

    def poll_for_updates(self, ttr=30):

        while True:
            time.sleep(ttr)
            for filename, file_struct in self.files.items():
                if file_struct.valid and file_struct.copy:
                    pull_request = {
                        "type": "PULL",
                        "node_id": self.node_id,
                        "file_name": filename,
                        "cached_version": file_struct.version,
                        "origin_node": file_struct.origin_node
                    }
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.connect((HOST, self.connected_super_peer_port))
                            s.send(json.dumps(pull_request).encode())
                            response = json.loads(s.recv(1024).decode())
                        if response["status"] == "STALE":
                            print(f"File {filename} is stale. Invalidating locally.")
                            self.handle_invallidation(response)
                            reply = input("would you like to re-download the newest version?")
                            if reply == 'yes':
                                self.query_file(filename)
                            else:
                                continue
                        elif response["status"] == "VALID":
                            print(f"File {filename} is up-to-date. No action needed.")
                    except Exception as e:
                        print(f"Error polling for updates for file {filename}: {e}")


    # function to allow usser to edit file
    def edit_file(self, name, input):
        if name in self.file_names:
            file = self.files[name]

            # check if file can be edited 
            # no if it is a copy and no if it is invalid
            if file.copy == True:
                print("You can not edit this file, it is not a master copy.\n")
                return
            if file.valid == False:
                print("You cannot edit this file, it is invalid.\n")
                return
            
            print(f"Editing file: {name}")
            file_path = os.path.join(self.directory, name)
            try:
                with open(file_path, "a") as f:
                    f.write(f"\n{str(input)}")
                
                # Update file version number
                file.increment_version()
                
                print(f"File {name} has been updated. Version is now {file.version}.\n")
                # broadcast change to rest of network (PUSH Method)
                if self.mode == 'push':
                    self.push(file.name, file.version)
            
            except Exception as e:
                print(f"An error occurred while editing the file: {e}")
                traceback.print_exc()


        # file is not in node        
        else:
            print(f"File {name} does not exist in this node.\n")


#
# main
#
if __name__ == "__main__":
    
    if len(sys.argv) < 5:
        print("Usage: python leaf_node.py <node_id> <super_peer_port> <node_port> <file1> [file2 ...] --mode=<push/pull>")
        sys.exit(1)
    
    # Parse mode
    mode_arg = [arg for arg in sys.argv if arg.startswith("--mode=")]
    if mode_arg:
        mode = mode_arg[0].split("=")[1].lower()
        sys.argv.remove(mode_arg[0])
    else:
        mode = "pull"  # Default mode

    # Validate mode
    if mode not in ["push", "pull"]:
        print("Invalid mode. Defaulting to pull.")
        mode = "push"

    # Parse remaining arguments
    node_id, connected_super_peer_port, port, *files = sys.argv[1:]
    file_in_node = {f: File(f, node_id, True, node_id) for f in files}

    sp_name = "SP" + str(int(str(connected_super_peer_port)[2:]))
    
    # Initialize the leaf node with the chosen mode
    leaf_node = LeafNode(
        node_id,
        int(connected_super_peer_port),
        sp_name,
        file_in_node,
        files,
        int(port),
        mode
    )
    sp_name = "SP" + str(int(str(connected_super_peer_port)[2:]))
    # leaf_node = LeafNode(node_id, int(connected_super_peer_port), sp_name, file_in_node, files, int(port), mode)
    leaf_node.register_files()
    while True:
        command = input("Enter a command (query [filename], edit [filename] [text], or exit): ")
        if command.startswith("query"):
            _, file_name = command.split()
            leaf_node.query_file(file_name)
        elif command.startswith("edit"):
            _, file_name, text = command.split()
            leaf_node.edit_file(file_name, text)
        elif command == "exit":
            break
