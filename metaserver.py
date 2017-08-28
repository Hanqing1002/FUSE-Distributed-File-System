#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.03

"""

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary

# Presents a HT interface
# trust client; leave the checking of key to clients
class metaserver:
  def __init__(self,port):
    self.data = {} # key is the path; value is the metadata information
    print '----------metaserver',port,'starts serving--------------'
  def count(self):
    return len(self.data)

  # Retrieve something from the HT
  def get(self, key):
    # Default return value
    rv = False
    key = key.data
    # If the key is in the data structure, return properly formated results
    if key in self.data:
      rv = Binary(pickle.dumps(self.data[key],2))
    return rv

  # Insert something into the HT
  def put(self, key, value):
    # Remove expired entries
    self.data[key.data] = pickle.loads(value.data)
    return True

  # Pop
  def pop(self,key):
    key = key.data
    if key in self.data:
      key = self.data.pop(key)
      return Binary(pickle.dumps(key,2))
    else:
      return False

  # retrieve all keys
  def pathList(self):
    keys = self.data.keys()
    return Binary(pickle.dumps(keys,2))

def main():
  if len(sys.argv) != 2:
    print('usage: %s <metaserver port>' % sys.argv[0])
    exit(1)
  port = int(sys.argv[1])

  print 'Use Control-C to exit'
  serve(port)

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('localhost', port)) # localhost
  file_server.register_introspection_functions()
  sht = metaserver(port)
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.pop)
  file_server.register_function(sht.pathList)

  try:
    file_server.serve_forever()
  except KeyboardInterrupt:
    print 'Exiting'
  

if __name__ == "__main__":
  main()
