#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.03

"""
# changes: 
# 1)a file is a dictionary; the block number is the key passed as arguments
# 2)Redundancy is a global varible
# 3)All keys in self.data is string

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
import shelve,binascii,socket
import numpy as np

REDUNDANCY = 2 # global invariant, shared by both servers and clients
# some maps are required

class dataserver:
  def __init__(self,*argv):

    # configure 2 lists:
    # 1) one list for the ports of backup server
    # 2) the other is for recovery
    
    index = argv[0] # the index of current dataserver
    numServers = len(argv)-1 # the number of total dataservers
    serverPorts = argv[1:] # the ports of all dataservers
    port = str(serverPorts[index]) # the port of current dataserver
    print '----------dataserver',port,'starts serving--------------'
    # Retrieve history data
    # self.data is a dict of dicts that has REDUNDANCY ports:data pairs
    # Actually, each server has its own data and the copy of last server's own data
    # {}, the filename is dataserver_port 
    
    self.data = shelve.open('dataserver_'+port, writeback=True) # not efficient, can further be improved
    # Note: can't use integer as key

    # set up connection with next server: used for recovery
    nextPort = str(serverPorts[(index+1)%numServers])
    self.nextServer = xmlrpclib.ServerProxy('http://localhost:' + nextPort)

    # set up a list of ports(string) whose data is kept in the current server
    self.ports = {} 
    self.servers = {}
    for i in range(REDUNDANCY):
      self.ports[i] = str(serverPorts[(index-i)%numServers]) # ports[0] is the port of current dataserver
      if i!= 0:
        self.servers[self.ports[i]] = xmlrpclib.ServerProxy('http://localhost:' + self.ports[i])

    if self.data!={}:
      print("Loaded previous data from server backup file")
    else:
      # Initialize self.data
      for i in range(REDUNDANCY):
        # for each port, initialize a dictionary in the self.data
        self.data[self.ports[i]] = {}
      # 2 cases: 
      # 1)No history records:
      # 2)Hard disk fails
      # Anyway, recover previous data and the copies of other servers kept by this dataserver
      for i in self.ports:
        # Communicate with the next server to retrieve its own data
        # 1)recover self.data[port]: talk with next server
        p = self.ports[i]
        if p == port: 
          # Assume only one server crash at a time. 
          # No need to retry if failed to connect with server 
          try:
            replica = self.nextServer.getReplica(Binary(port))
            self.data[p] = pickle.loads(replica.data)
            print 'Successfully loaded previous data from dataserver:', nextPort
          except socket.error:
            print 'Recovering previous data is passed'
            pass # Based on the assumption, this only happens when starting to run all dataservers at the beginning.In this case, skip getReplica by ingore the socket error
        else:
          # 2)recover other copies from the original dataservers
          try:
            replica = self.servers[p].getReplica(Binary(p))
            self.data[p] = pickle.loads(replica.data)
            print "Successfully loaded previous copy from dataserver:", p
          except socket.error:
            print "Recovering previous copy is passed"
            pass           

  # Interfaces are changed.
  # All arguments are passed within a dict
  # port is added into arguments, to indicate the server's port
  # message = data + checksum
  # checksum should be computed when data is put and stored as a part of data
  # ??? why arguments and return have to be string not integer???
  # New argument: version - the version of block
  # The data structure for each block is a list: indexed by version number
 
  def get(self,argv):
    rv = False
    datagram = pickle.loads(argv.data)
    port = datagram['port']
    path = datagram['path']
    ID = datagram['blockID']
    version = datagram['version']
    # If the key is in the data structure, return properly formated results
    if port in self.data and path in self.data[port] and \
        ID in self.data[port][path] and \
        version in self.data[port][path][ID]:
      rv = Binary(self.data[port][path][ID][version])
    return rv

  # Put a new version of block
  # return the length of received data
  def put(self,argv):
    datagram = pickle.loads(argv.data)
    port = datagram['port']
    path = datagram['path']
    ID = datagram['blockID']
    version = datagram['version']
    value = datagram['data']
    length = str(len(value))
    if path not in self.data[port]: # initialize a dict for a new file
      self.data[port][path] = {}
    if ID not in self.data[port][path]:
      self.data[port][path][ID] = {} # initialize a dict for a new block
    # the version ID is not necessarily continuous
    self.data[port][path][ID][version] = value + self.checksum(value)
    return Binary(length)

  # Pop entire block: delete all data including history
  # or pop a single version of block
  def pop(self,argv):
    rv = False
    datagram = pickle.loads(argv.data)
    port = datagram['port']
    path = datagram['path']
    ID = datagram['blockID']
    if port in self.data and path in self.data[port] and ID in self.data[port][path]:
      if 'version' in datagram  and datagram['version'] in self.data[port][path][ID]:
        data = self.data[port][path][ID].pop(datagram['version'])
        rv = Binary(data)
      else:
        data = self.data[port][path].pop(ID)
        rv = Binary(data)
    return rv

  def keyRename(self,argv):
    rv = False
    datagram = pickle.loads(argv.data)
    port = datagram['port']
    old = datagram['old']
    new = datagram['new']
    ID = datagram['blockID']
    if port in self.data and old in self.data[port] and ID in self.data[port][old]:
      if new not in self.data[port]:
        self.data[port][new] = {}
      self.data[port][new][ID] = self.data[port][old].pop(ID)
      if self.data[port][old] == {}:
        self.data[port].pop(old)
  
  def getReplica(self,port):
    port = str(port.data)
    replica = self.data[port]
    # serializing and marshalling
    replica = pickle.dumps(replica,2)
    return Binary(replica)

# -------------------for test only-------------
  def corrupt(self,path): # corrupt the latest data
  # This function will corrupt at least one byte of any blocks of the file
    rv = False
    p = []
    for i in self.ports:
      if path in self.data[self.ports[i]]:
        p.append(i)
    if len(p)>0:
      s = np.random.randint(0,len(p))
      s = p[s]

      # randomly pick up a block 
      IDlist = self.data[self.ports[s]][path].keys()
      ID = np.random.randint(0,len(IDlist))
      ID = IDlist[ID]
      # randomly pick up a byte
      version = len(self.data[self.ports[s]][path][ID])-1
      mes = self.data[self.ports[s]][path][ID][version]
      byte = np.random.randint(0,len(mes)-1)
      i = 1
      while(mes[byte]==str(i)):
        i = i+1
      # string is immutable
      mes = mes[:byte] + str(i) + mes[byte+1:]
      self.data[self.ports[s]][path][ID][version] = mes
      rv = True
      print('Information:')
      print 'Corrupted file:', path
      print 'Corrupted blockID: ', ID
    else:
      print("Path is unfound.")
    return rv
      
    
  

#----------internal functions-----------
  # The following functions are not available to clients

  # calculate checksum for message: CRC32
  def checksum(self,data):
    crc = binascii.crc32(data) & 0xffffffff
    return "{0:x}".format(crc).rjust(8,'0') # take it as a 8-byte string
 
#----------------------------   

def main():
  if len(sys.argv) < 4:
    print('usage: %s <indexed server number> <ports for all dataservers sperated by spaces>' % sys.argv[0])
    exit(1)
  # the number of dataservers should not be smaller than the replicas
  if len(sys.argv)-2 < REDUNDANCY:
    print('Need more dataservers for the current redundancy:')
    print(REDUNDANCY)
    exit(1)
  
  sys.argv[1:] = map(int, sys.argv[1:]) # convert the arguments into integers

  print 'Use Control-C to exit'
  serve(*sys.argv)

# Start the xmlrpc server
def serve(*argv):

  # The inital arguments for dataserver:
  # a series of ports: starting with its own port and followed by its replicas
  

  index = argv[1] # the index of current dataserver
  port = argv[index+2] # the port of current dataserver
  argv = argv[1:] # only the index and list of ports are passed to dataserver

  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('localhost', port),allow_none=True)
  file_server.register_introspection_functions()
  sht = dataserver(*argv)
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.pop)
  file_server.register_function(sht.getReplica)
  file_server.register_function(sht.corrupt)
  file_server.register_function(sht.keyRename)

  try:
    file_server.serve_forever()
  except KeyboardInterrupt:
    print 'Exiting'

if __name__ == "__main__":
  main()
