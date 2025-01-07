#
# CS 485 PA3 - UIC Fall 24'
# Mira Sweis ECE MS
#
# file.py is basically a struct for maintiaing file information
# including version number, if it is a master copy or not, and validity

class File:
    def __init__(self, name, node, valid, og):
        self.name = name
        self.connected_node = node
        self.valid = valid
        self.origin_node = og
        self.copy = False
        self.version = 1
        self.unique = ''

    def invalidate(self):
        self.valid = False
    
    def is_copy(self):
        self.copy = True
    
    def increment_version(self):
        self.version += 1
    
    def create_id(self):
        if self.copy:
            s = "_is_copy"
        # create a unique identifier for each file saved
        self.unique = str(self.name) + str(self.connected_node) + "_V" + str(self.version) + s
