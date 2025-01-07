# Hierarchical-Gnutella-style-P2P-file-sharing-system

This project implements a hierarchical Gnutella-style peer-to-peer (P2P) file-sharing system that 
supports both push-based and pull-based consistency mechanisms for maintaining file validity 
across nodes. The system simulates two network topologies and enables testing of the trade-offs 
between these consistency methods:
All-to-All Topology: Each super-peer is connected to every other super-peer.
Linear Topology: Super-peers are connected in a chain-like structure.
The project utilizes Python to create a distributed system where leaf nodes communicate through 
super-peers to query, share, and validate files. File consistency is ensured by propagating 
invalidation messages in push mode or periodic polling in pull mode
