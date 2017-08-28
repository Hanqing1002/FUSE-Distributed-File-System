#!/usr/bin/env python

# DistributedFS
# Modification:
# 1) Instead of a seperate dictionary, parent and children become fields in files;
#        parent is a string: the absolute path of parent dir; 
#        children is a list: the relative pathes of children


from __future__ import print_function, absolute_import, division

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import time as time1
import os.path 
import stat
import xmlrpclib,pickle, hashlib
from xmlrpclib import Binary
import math, socket, binascii
import numpy as np
import errno

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

BLOCKSIZE = 512 # bytes 
REDUNDANCY = 2
TIMEOUT = 30

if not hasattr(__builtins__, 'bytes'):
    bytes = str
# changes:
# get, put, pop for data

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports hierarchical files.'
    
    def __init__(self,*ports):
        # self.files, self.data have been deleted
        self.ports = ports
        # 4 class global fileds
        self.N = len(ports)-1  # the number of data servers
        # only been used in function: __init__, metaWrapper and dataPut,dataPop,dataGet 
        # connecting to servers   
        self.metaserver = xmlrpclib.ServerProxy('http://localhost:' + ports[0])
        dataservers = {} # a dict
        self.dataPorts = ports[1:]
        for i in range(0,self.N):
            curPort = self.dataPorts[i]            
            dataservers[curPort] = xmlrpclib.ServerProxy('http://localhost:' + curPort)  # no need to be global any more
        # Need a table indexed by each server' port, recording the list of servers we need to access/updata at the same time
        self.table = {} # should be a N * REDUNDANCY table: a dict of lists
        for i in range(0,self.N):   
            p = self.dataPorts[i]
            self.table[p] = [] # each line is represented by a list: a list of servers
            for j in range(REDUNDANCY):
                temp_p = self.dataPorts[(i+j)%self.N]
                self.table[p].append(dataservers[temp_p])


        # initlize the root folder
        self.fd = 0
        now = time()
        path = '/'
        metainfo = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2,children = []) # no parent
        self.metaWrapper(path, metainfo, 'put')
        
		
 

    # Introduce 2 functions as the interface of the communication between client and servers
    # Modify every where the previous self.files and self.data are used
    # Instead, Access them using the wrapper

    # the function of Wrapper: 
    # 1)Arrange the arguments into right format: 
    # 2)Call the functions in server side
    # 3)Arrange the results into right format and pass them back to client  
    # 4)Leave it to client to address any problem if there is any unexpected behavior: e.g.keyError 
    #   in which case, the result is False, instead of system error

    # metaWrapper: 3 arguement
    # path: the key
    # metainfo: the value (None when func is not 'put')
    # func: the function to be call
    #   1)get: return metainfo of path or False
    #   2)pop: return metainfo of path or False
    #   3)put: return True or False
    #   4)pathList: return the list of the keys

    def metaWrapper(self,path,metainfo,func): 
        key = Binary(path) # marshalling
        if func == 'get': 
            rv = self.metaserver.get(key)
            if rv == False:
                return False      
            else:
                metainfo = self.metaserver.get(key)
                return pickle.loads(metainfo.data)
        elif func == 'pop': 
            rv = self.metaserver.pop(key)
            if rv == False:
                return False
            else:
                return pickle.loads(rv.data)                
        elif func == 'put':
            value = Binary(pickle.dumps(metainfo,2))        
            rv = self.metaserver.put(key,value)
            return rv
        else: # pathList
            pathList = self.metaserver.pathList()
            return pickle.loads(pathList.data)
                       
    # change: dataWrapper is deleted; instead, dataGet, dataPop, dataPut
    # all-or-nothing actomicity for block
    # port: to communicate with dataservers[port]
    # path: as the key
    # blockID: to indicate blockID
    # data: the value (None when func is not 'put')
    # verison: the version of block 
    # func: the function to be call
    #   1)get: return data of path or False
    #   2)pop: return data of path or False
    #   3)put: return True or False
    
    
    
    # Handle:
    # 1) server crash
    # 2) data corruption

    
    def dataGet(self,port,path,blockID,version):
        datagram = {} # use a dict to contain all the argument
        port = self.dataPorts[port]
        servers = self.table[port] # A list: the servers we need to read from
        if len(servers) != REDUNDANCY:
            raise Exception('The number of vailable servers is not equal to REDUNDANCY!')
        # Marshalling
        datagram['blockID'] = str(blockID)
        datagram['path'] = path
        datagram['port'] = str(port)
        datagram['version'] = version
        argv = Binary(pickle.dumps(datagram,2))
        # 2 flags for each server
        access = np.array([False] * REDUNDANCY) # accessible or not
        correct = np.array([False] * REDUNDANCY) # correct or not
        data={}
        blockTime = 0
        while(True not in access): # keep retry if all servers are not accessible
            if(blockTime >= TIMEOUT):
                print("Read failed: Connection refused.")
                raise OSError(errno.ECONNREFUSED)
                break
            for i in range(REDUNDANCY):
                try:
                    mes = servers[i].get(argv)
                    if mes is False:
                        return ''
                    access[i] = True
                    message = mes.data
                    # verify checksum
                    data[i] = message[:len(message)-8]
                    crc1 = message[len(message)-8:]
                    crc = self.checksum(data[i])
                    if(crc==crc1):
                        correct[i] = True
                except socket.error:
                    pass
            # There are serveral cases:
            # 1) both connections failed: retry
            if (True not in access):
                print("Connection for all servers failed. Retry after 3 seconds.")
                time1.sleep(3)
                blockTime = blockTime + 3
                print("Blocking time: " + str(blockTime) + " (Time out after " + str(TIMEOUT-blockTime) + ")")
            # 2) write back data to servers that don't pass the checksum varification
            else:
                # find the correct return value
                for i in range(REDUNDANCY):
                    if access[i] & correct[i]:
                        value = data[i]
                        break
                # if no accessible servers pass the checksum
                if not (True in (access & correct)):
                    raise Exception("Unhandled case!")
                for i in range(REDUNDANCY):
                    if access[i]:
                        if not correct[i]:
                            print("Some data is corrupted. Need to write back.")
                            print("Path: " + path + " BlockID: " + str(blockID))
                            print("Corrupted server: " + str(servers[i]))
                            datagram['data'] = value
                            argv = Binary(pickle.dumps(datagram,2))
                            servers[i].put(argv) 
        return value
            

    def dataPop(self,port,path,blockID,version):
        # not so good: test connection
        try:
            dataGet(port,path,blockID,version)
        except OSError, e:
            if e.args[0] == errno.ECONNREFUSED:
                raise OSError(errno.ECONNREFUSED) # raise again, pass it to caller
        # All servers are available:
        datagram = {}
        port = self.dataPorts[port]
        servers = self.table[port] # the servers we need to read from
        if len(servers) != REDUNDANCY:
            raise Exception('The number of vailable servers is not equal to REDUNDANCY!')
        datagram['blockID'] = str(blockID)
        datagram['path'] = path
        datagram['port'] = str(port)
        if version is not None:
            datagram['version']
        argv = Binary(pickle.dumps(datagram,2))
        # same behaviour as 'put': keep retry until both succeed
        mes = {}
        for i in range(REDUNDANCY):
            mes[i] = servers[i].pop(argv)

        # verify checksum: similar but not the same behaviour with "get" (no need to write back)
        # 1)All return false
        if all(mes[i]==False for i in range(REDUNDANCY)): 
            return False
        # 2)None returns false: return value from any server that passes the checksum verification   
        elif all(mes[i]!=False for i in range(REDUNDANCY)): 
            value = []
            for i in range(REDUNDANCY):
                message = mes[i].data
                value =message[:len(message)-8]
                crc1 = message[len(message)-8:]
                crc = self.checksum(value)
                if(crc==crc1):
                    return value
            raise Exception("Unexpected case!!")
        # Any other case is unexpected
        else: # 
            raise Exception("Unexpected case!!")
                        
    def dataPut(self,port,path,blockID,data,version):
        datagram = {} 
        datagram1 = {}
        port = self.dataPorts[port]       
        servers = self.table[port] # the servers we need to read from
        if len(servers) != REDUNDANCY:
            raise Exception('The number of vailable servers is not equal to REDUNDANCY!')
        length = len(data) # to be compared with returned acknowledgement
        datagram['blockID'] = str(blockID)
        datagram['path'] = path
        datagram['port'] = str(port)
        datagram['data'] = data
        datagram['version'] = version
        argv = Binary(pickle.dumps(datagram,2))
        access = np.array([False] * REDUNDANCY) # accessible and acknowledged or not
        blockTime = 0
        while(False in access):  # keep retry unless all servers are updated
            if(blockTime >= TIMEOUT):
                # revert the modification
                for i in range(REDUNDANCY):
                    if access[i]:
                        # assume no unavailable system
                        servers[i].pop(argv)
                raise OSError(errno.ECONNREFUSED)
            for i in range(REDUNDANCY):
                if not access[i]:
                    try:
                        servers[i].put(argv)
                        access[i] = True
                    except socket.error:
                        print("One server Connection failed. Retry after 3 seconds.") # how to send this to FUSE clients????
            if False in access:
                time1.sleep(3)
                blockTime = blockTime + 3
                print("Blocking time: " + str(blockTime) + " (Time out after " + str(TIMEOUT-blockTime) + ")")
        return True

    def dataRename(self, port, old, blockID, new): # non-blocking
        datagram = {} 
        datagram1 = {}
        port = self.dataPorts[port]       
        servers = self.table[port] # the servers we need to read from
        if len(servers) != REDUNDANCY:
            raise Exception('The number of vailable servers is not equal to REDUNDANCY!')

        datagram['blockID'] = str(blockID)
        datagram['old'] = old
        datagram['port'] = str(port)
        datagram['new'] = new
        argv = Binary(pickle.dumps(datagram,2))
        # for revert modifiction use only
        datagram1['blockID'] = str(blockID)
        datagram1['old'] = new
        datagram1['port'] = str(port)
        datagram1['new'] = old
        argv1 = Binary(pickle.dumps(datagram1,2))
        access = np.array([False] * REDUNDANCY) # accessible or not
        for i in range(REDUNDANCY):
            try:
                servers[i].keyRename(argv)
                access[i] = True
            except socket.error:
                print("One server Connection failed.") # how to send this to FUSE clients????
        if False in access:
            for i in range(REDUNDANCY):
                if access[i]:
                    servers[i].keyRename(argv1)
            raise OSError(errno.ECONNREFUSED)
        return True
        

    # calculate checksum for message: CRC32
    def checksum(self,data):
        crc = binascii.crc32(data) & 0xffffffff
        return "{0:x}".format(crc).rjust(8,'0') # take it as a 8-byte string
             
    #-------------Operations-------------
    def chmod(self, path, mode):
        metainfo = self.metaWrapper(path,None,'get')
        metainfo['st_mode'] &= 0o770000
        metainfo['st_mode'] |= mode
        self.metaWrapper(path, metainfo, 'put')


    def chown(self, path, uid, gid):
        metainfo = self.metaWrapper(path,None,'get')
        metainfo['st_uid'] = uid
        metainfo['st_gid'] = gid
        self.metaWrapper(path, metainfo, 'put')

    def create(self, path, mode):    
        metainfo = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time()) 
        parentdir = os.path.abspath(os.path.join(path,os.pardir))  
        metainfo['parent'] = parentdir 
        metainfo['startPortID'] = hash(path)%self.N 
        metainfo['version'] = [] # a list of block version: length = numBlocks
        self.metaWrapper(path, metainfo, 'put')
        # modify parent meta
        pa_metainfo = self.metaWrapper(parentdir, None, 'get')
        pa_metainfo['children'].append(os.path.basename(path))
        self.metaWrapper(parentdir, pa_metainfo, 'put') 
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        metainfo = self.metaWrapper(path,None,'get')
        if metainfo == False:
            raise FuseOSError(ENOENT)
        return metainfo

    def getxattr(self, path, name, position=0):
        metainfo = self.metaWrapper(path,None,'get')
        attrs = metainfo.get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        metainfo = self.metaWrapper(path,None,'get')
        attrs = metainfo.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        metainfo = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(),children = [])
        parentdir = os.path.abspath(os.path.join(path,os.pardir))
        metainfo['parent'] = parentdir
        self.metaWrapper(path, metainfo, 'put')
        # modify the parent metainfo
        
        pa_metainfo = self.metaWrapper(parentdir, None, 'get')     
        pa_metainfo['children'].append(os.path.basename(path))
        pa_metainfo['st_nlink'] += 1  
        self.metaWrapper(parentdir, pa_metainfo, 'put')  

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh): 
        content = self.readBlock(path,size,offset,fh)
        return content
    
    # Assistant function for read
    # Read distributed the blocks,in round-robin fashion, into N dataservers. 
    # Use the hash value of 'st_ctime' as the start port
    def readBlock(self, path, size, offset, fh):
        metainfo = self.metaWrapper(path, None, 'get')
        content = ''
        # 2 special cases
        if offset >= metainfo['st_size']:
            return content
        if (offset+size) > metainfo['st_size']:
            size = metainfo['st_size']-offset

        firstBlockID = int(offset/BLOCKSIZE) # offset determine the first blockID to be read
        firstOffset = offset%BLOCKSIZE 

        # the first data server to be accessed
        start_port = metainfo['startPortID']
        start_port = (start_port + firstBlockID)%self.N

        endBlockID = int(math.ceil((offset+size)/BLOCKSIZE))
        endOffset = (offset+size)%BLOCKSIZE
		
        # Traverse the dataservers to read data
        port = start_port # port, assistant varieble to traverse the dataservers
        state = [0] * (endBlockID-firstBlockID) # record each block's state: 0-have not started; 1-finished; -1 failed
        for i in range(firstBlockID, endBlockID): # traverse the blocks to the last
            if i != firstBlockID and i != endBlockID-1:
                content = content + self.dataGet(port,path,i,metainfo['version'][i])   
            elif i == firstBlockID: # for the first Block, start reading from the firstOffset
                content = self.dataGet(port,path,i,metainfo['version'][i])
                content = content[firstOffset:]
            else: # for the last block
                rv = self.dataGet(port,path,i,metainfo['version'][i])
                content = content + rv[:(size-len(content))]
            port = (port+1)%self.N # update port for reading the next block
        
        return content

    def readdir(self, path, fh):
        metainfo = self.metaWrapper(path, None, 'get')
        return ['.', '..'] + metainfo['children'] 
     
    # For links, assume one and only one block for data. 
    # So, readlink only needs to access one dataserver
    def readlink(self, path):
        metainfo = self.metaWrapper(path, None, 'get')
        start_port = hash(path)%self.N
        data = self.dataGet(start_port,path,0,metainfo['version']) # index = 0
        return data

    def removexattr(self, path, name):
        metainfo = self.metaWrapper(path, None, 'get')
        attrs = metainfo.get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new): # [Notes]: rename is used for both rename and move!
        
        parentdir_old = os.path.abspath(os.path.join(old,os.pardir))
        parentdir_new = os.path.abspath(os.path.join(new,os.pardir))
        # access metainfo: 4- old, new, oldpa, newpa
        old_metainfo = self.metaWrapper(old, None, 'get')
        oldpa_metainfo = self.metaWrapper(parentdir_old, None, 'get')
        newpa_metainfo = self.metaWrapper(parentdir_new, None, 'get')
        new_metainfo = old_metainfo
        print(newpa_metainfo)
        # Note!:update the children of oldpa, newpa 
        if os.path.basename(old) in oldpa_metainfo['children']:  
            oldpa_metainfo['children'].remove(os.path.basename(old))
        if parentdir_old == parentdir_new:
            newpa_metainfo['children'].remove(os.path.basename(old))
        if os.path.basename(new) not in newpa_metainfo['children']:
            newpa_metainfo['children'].append(os.path.basename(new))

        
        # update new's parent    
        new_metainfo['parent'] = parentdir_new

        self.metaWrapper(new,new_metainfo,'put')
        if parentdir_old != parentdir_new:
            self.metaWrapper(parentdir_old,oldpa_metainfo,'put')
        self.metaWrapper(parentdir_new,newpa_metainfo,'put')

        # address the problem for directory and non-directory seperately
        if stat.S_ISDIR(old_metainfo['st_mode']): # not all or nothing!!

            # Step 1: update 'st_nlink' in parent
            if parentdir_old != parentdir_new:
                oldpa_metainfo['st_nlink'] -=1   
                newpa_metainfo['st_nlink'] +=1

            # Step 2: update the path (the key)
            old_dir = old + '/'
            new_dir = new + '/'
            length = len(old_dir)
            pathList = self.metaWrapper(None, None, 'pathList')

            for path in pathList: # access the metainfo of path:  temp_metainfo(old) & temp_new_metainfo(new)
                if path.startswith(old_dir):
                    new_path = new_dir + path[length:]
                    
                    # update data if it's a regular file
                    temp_metainfo = self.metaWrapper(path,None,'get')
                    temp_new_metainfo = temp_metainfo
                    if stat.S_ISREG(temp_metainfo['st_mode']):
                        self.rename(path,new_path)                            
           
                    # update path for self.files: modify all the old path's children with new parent  
                    new_parent = new + temp_metainfo['parent'][len(old):] 
                    temp_new_metainfo['parent'] = new_parent
                    
                    self.metaWrapper(path,None,'pop')
                    if temp_metainfo!=False:
                        self.metaWrapper(new_path,temp_new_metainfo,'put')         
                  
        else: # for rename regular files: all or nothing
            # update new's data: pop all the content and put them back with new path and new ports
            new_metainfo['startPortID'] = old_metainfo['startPortID'] # to avoid data read and rewrite
            numBlocks = int(math.ceil(old_metainfo['st_size']/BLOCKSIZE))
            port_new = new_metainfo['startPortID']
            # all or nothing
            state = [0]*numBlocks
            for i in range(0,numBlocks):
                try:
                    self.dataRename(port_new, old, i, new)
                    state[i] = 1
                except OSError, e:
                    if e.args[0] == errno.ECONNREFUSED:
                        state[i] = -1
                        break
                port_new = (port_new+1)%self.N
            if -1 in state:
                port_new = new_metainfo['startPortID']
                for i in range(0,numBlocks):
                    if state[i] == 1:
                        self.dataRename(port_new, new, i, old)
                    port_new = (port_new+1)%self.N
                print('Connection Refused: reverted the previous modification...')
                raise OSError(errno.ECONNREFUSED)
        # save metainfo  
        # print newpa_metainfo, parentdir_new
        self.metaWrapper(old,None,'pop')
       
    def rmdir(self, path): 
        parentdir = os.path.abspath(os.path.join(path,os.pardir))   
        # access metainfo
        metainfo = self.metaWrapper(path, None, 'get')
        pa_metainfo = self.metaWrapper(parentdir, None, 'get')
        # update parent of path 
        pa_metainfo['st_nlink'] -= 1 
        pa_metainfo['children'].remove(os.path.basename(path)) 
  
        # delete all the children
        path_dir = path + '/'
        length = len(path_dir)
        if len(metainfo['children'])>0:
            children_list = metainfo['children']
            for i in range(1,len(children_list)):
                sub_path = path_dir + children_list[i]
                sub_path_metainfo = metaWrapper(sub_path, None, 'get')
                if stat.S_ISDIR(sub_path_metainfo['st_mode']):
                    self.rmdir(sub_path) # recursively
                else:
                    # pop the metainfo from metaserver
                    self.metaWrapper(sub_path,None, 'pop')
                    # pop the content from dataservers
                    sub_path_metainfo = metaWrapper(sub_path, None, 'get')
                    start_port = sub_path_metainfo['startPortID']
                    numBlocks = int(math.ceil(sub_path_metainfo['st_size']/BLOCKSIZE))
                    port = start_port
                    for i in range(0, numBlocks):                     
                        self.dataPop(port,sub_path,i,None)
                        port = (port+1)%self.N
                    
        self.metaWrapper(path,None, 'pop')
        self.metaWrapper(parentdir, pa_metainfo,'put')

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        metainfo = self.metaWrapper(path, None, 'get')
        attrs = metainfo.setdefault('attrs', {})
        attrs[name] = value
        self.metaWrapper(path, metainfo, 'put')

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    # Again: for a link, save the data in one dataserver
    def symlink(self, target, source):
        target_metainfo = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source), version=0) # for link, version is a single number instead of a list
        self.metaWrapper(target,target_metainfo,'put')
        # save the data
        start_port = hash(target)%self.N
        self.dataPut(start_port, target, 0, source, 0)

    def truncate(self, path, length, fh=None): 
        # shrink or extend the file size to length
        pathList = self.metaWrapper(None,None,'pathList')
        metainfo = self.metaWrapper(path,None,'get')
        if path in pathList:
            # case 1
            if length >metainfo['st_size']: #!!!!!
                #content = self.read(path, length, 0, fh)
                #self.write(path, content[:length], 0, fh)
            #else:
                content = '\x00'*(length-metainfo['st_size'])
                self.write(path,content, metainfo['st_size'],fh)
        else:
            content = '\x00'*length
            self.write(path,content,0,fh)
        metainfo = self.metaWrapper(path,None,'get') # metainfo has been changed. read again
        metainfo['st_size'] = length
        self.metaWrapper(path,metainfo,'put')

            

    def unlink(self, path):
        self.metaWrapper(path, None, 'pop')
        parentdir = os.path.abspath(os.path.join(path,os.pardir)) 
        pa_metainfo = self.metaWrapper(parentdir, None, 'get') 
        pa_metainfo['children'].remove(os.path.basename(path)) 
        self.metaWrapper(parentdir, pa_metainfo, 'put')

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        metainfo = self.metaWrapper(path, None,'get')
        metainfo['st_atime'] = atime
        metainfo['st_mtime'] = mtime
        self.metaWrapper(path, metainfo, 'put')

    # non blocking write: All or nothing
    def write(self, path, data, offset, fh): 
        # set some variables to prepare for write
	    # 2 special cases: 1) the new size of file is less than the existing file's size 
	    #			        the data after (offset+len(data)) should be retained
	    #		           2) the offset is larger than the existing size
        # Read the entire previous data, form the new complete data and write it back into blocks
        metainfo = self.metaWrapper(path, None, 'get')
        data1 = data
        if offset > metainfo['st_size']:
            data1 = '\x00'*(offset-metainfo['st_size']) + data
            offset = metainfo['st_size']
        flag = self.writeBlock(path, data1,offset,fh) 
        if flag:
            # write into file successfully  
            metainfo = self.metaWrapper(path, None, 'get')
            if offset+len(data) > metainfo['st_size']:
                metainfo['st_size'] = offset+len(data)
                self.metaWrapper(path,metainfo,'put')
            return len(data)
        else:
            print("Write failed: Connection refused.")
            raise OSError(errno.ECONNREFUSED)
    
    # Assistant function for write
    # write the file into blocks in round-robin fashion.
    # Tips:
    # -index each block starting from 0
    # -the first dataserver's port determine by the hash('st_ctime')
    # -always rewrite the entire file(delete offset)
    # -So the version of blocks in one file is always the same
    def writeBlock(self, path,data,offset,fh): # only this function modifies 'version'
        
        metainfo = self.metaWrapper(path,None, 'get')

        firstBlockID = int(offset/BLOCKSIZE) # offset determine the first blockID to be read
        firstOffset = offset%BLOCKSIZE 

        endBlockID = int(math.ceil((offset+len(data))/BLOCKSIZE))
        endOffset = (offset+len(data))%BLOCKSIZE

        # the first data server to be accessed
        start_port = metainfo['startPortID']
        start_port = (start_port + firstBlockID)%self.N
        port = start_port
        state = [0] * (endBlockID-firstBlockID) # record each block's state: 0-have not started; 1-finished; -1 failed
        index = 0
        version = metainfo['version']
        newVersion = version
        
        for i in range(firstBlockID,endBlockID):
            if i in range(len(version)):
                newVersion[i] = version[i] + 1
            else:
                newVersion.append(0)
        current = 0 # how many byte of data has been put
        for i in range(firstBlockID,endBlockID):
            if i!=firstBlockID and i!=endBlockID-1: 
                try:
                    self.dataPut(port,path,i,data[current:current+BLOCKSIZE], newVersion[i])
                    current = current+BLOCKSIZE
                    state[index] == 1
                except OSError, e:
                    if e.args[0] == errno.ECONNREFUSED:
                        state[index] = -1
                        print("Connection refused. Reverting the modification...")
                        break  
            elif i==firstBlockID: # for the first block
                try:
                    temp = ''
                    if newVersion[i]!=0:
                        temp = self.dataGet(port,path,i,version[i])
                        temp = temp[:firstOffset]
                    temp = temp + data[:BLOCKSIZE-firstOffset]
                    current = BLOCKSIZE-firstOffset
                    self.dataPut(port,path,i,temp, newVersion[i])
                    state[index] == 1
                except OSError, e:
                    if e.args[0] == errno.ECONNREFUSED:
                        state[index] = -1
                        print("Connection refused. Reverting the modification...")
                        break
            elif i==endBlockID-1: # for the last block
                try:
                    temp = ''
                    if newVersion[i]!=0:
                        temp = self.dataGet(port,path,i,version[i])
                        temp = temp[endOffset:]
                    temp = data[current:]+temp
                    current = current + len(data[current:])
                    self.dataPut(port,path,i,temp, newVersion[i])
                    if current != len(data):
                        print("Write error! (for debug)")
                    state[index] == 1
                except OSError, e:
                    if e.args[0] == errno.ECONNREFUSED:
                        state[index] = -1
                        print("Connection refused. Reverting the modification...")
                        break
            port = (port+1)%self.N
            index = index+1
        # revert the modification
        if -1 in state:
            port = metainfo['startPortID']
            for i in range(firstBlockID,endBlockID):
                if state[i]==1:
                    self.dataPop(port,path,i,newVersion[i])  # assume no another broken down server
                port = (port+1)%self.N
            return False # write failed
        else:
            # update the version list
            metainfo['version'] = newVersion
            self.metaWrapper(path,metainfo,'put') 
            return True
            
         

if __name__ == '__main__':
    if len(argv) < 4: # at least 3 arguements
        print('usage: %s <mountpoint> <metaserver port> <dataserver1 port> .. <dataserverN port>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    ##fuse = FUSE(Memory(), argv[1], foreground=True)
    # **kwarg(keyward arguments): pass a dictionary to class constructor
    # *list: pass a list
    ports = argv[2:]
    fuse = FUSE(Memory(*ports), argv[1], foreground=True, debug = True)
