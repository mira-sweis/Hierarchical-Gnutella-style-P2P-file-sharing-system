#
# CS 485 PA3 - UIC Fall 2024
# Mira Sweis - ECE MS
#
# network_setup.py - starts up all super peers and links them together and with leaf nodes
# using a config.json file. must be run before any testing.
#
import json
from super_peer import SuperPeer
from leaf_node import LeafNode
from file import File

def load_config(config_file):
    with open(config_file, 'r') as f:
        return json.load(f)

# setup network creates super peers with connected leaf nodes and registered files
def setup_network(config_file, mode):
    if config_file == 'linear.json':
        print('Starting network with a Linear Topology\n')
    else:
        print('Starting network with All to All Topology\n')
    config = load_config(config_file)
    super_peers = {}
    leaf_nodes = {}

    # Initialize SuperPeers
    for sp_config in config["super_peers"]:
        peer_id = sp_config["peer_id"]
        port = sp_config["port"]
        leafs = sp_config["leaf_nodes"]
        super_peers[peer_id] = SuperPeer(peer_id, port, mode)
        super_peers[peer_id].leaf_nodes = leafs

    # Set up neighbor relationships for SuperPeers
    for sp_config in config["super_peers"]:
        peer_id = sp_config["peer_id"]
        neighbors = sp_config["neighbors"]
        for neighbor_id in neighbors:
            if neighbor_id in super_peers:
                super_peers[peer_id].neighbor_peers.append(super_peers[neighbor_id])

    for ln_config in config["leaf_nodes"]:
        leaf_id = ln_config["node_id"]
        connected_sp_id = ln_config["connected_super_peer"]

        files = ln_config["files"]


        # Register each file with the connected super-peer
        if connected_sp_id in super_peers:
            for file in files:
                # print(f'file: {file}')
                super_peers[connected_sp_id].registered_files[file] = leaf_id
 
    print("Network initialized with super-peers and leaf nodes.")
    # print(f'super peers: {super_peers}')
    return super_peers, leaf_nodes

# main
if __name__ == "__main__":
    config = input('Input either "all_to_all.json" or "linear.json": ')
    # config = 'linear.json'
    mode = input('Input mode: push or pull: ')
    super_peers, leaf_nodes = setup_network(config, mode)
    print("Run each leaf node in a separate terminal to initiate queries.")
