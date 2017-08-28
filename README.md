# FUSE-Distributed-File-System
A distributed file system built on FUSE library


This project implements a distributed fault-tolerate FUSE file system, with all the data stores in multiple remote servers.  Generally, we use 2 different storage methods for the file system data. The metadata, for each file, link and directory, is stored in a single meta-server, which, we assume, is extremely reliable and never fails. The storage pattern for file data is more robust, designed under the consideration of both fault tolerance (including data corruption and server crashes) and all-or-nothing atomicity. Basically, data is stored across multiple data-servers following a round-robin fashion, in the unit of 512-byte block. And for each block, there is one or more than on redundant copies stored in the following data-server (or data-servers, in the ID space). More detailed implementation will be explained in the next section.

  
# Implementation of data-server
There are several changes of data-server class compared with last version.  
1.	Redundancy

A global constant REDUNDANCY is preset, a redundancy factor agreed by both FUSE file system and data-servers. It indicates the number of stored copies for each blocks, with 2 set as the default value.  Thus, combined with the round-robin fashion, for a file having block list [1, 2, 3, …], its data will be stored in the following way.

    Replica 1: [x % N, (x+1) % N, (x+2) % N, (x+3) % N ...] where x is the hash, N is the number of data-servers 
    Replica 2: [(x+1) % N, (x+2) % N, (x+3) % N, (x+4) % N ...]
    The two replicas mean that the redundancy factor is 2
    
When REDUNDANCY is 2, there is 2 sets of blocks’ data in each data-servers, one is its original data which refers to the data stored in it when redundancy factor is set as 1, the other is an exact complete copy of its’ former data-server (in the ID space), as shown in the following table. 

More generally, each data-server has multiple sets of data, which is determined by the redundancy factor. For each set, we use a dictionary to contain it. The entire data of each data-server needs a single container to keep all sets of data. Dictionary is chosen in this design. For description simplification, we call it DATA, and use DATAID[port] to denote the data of data-server port stored in data-server ID. Both port and ID are used as the data-servers’ identifier. To be able to recover from system crash, a file is created by each data-server and used to store DATAID. This file is named after the data-server’s port. Every modification of DATAID will be reflected in this file immediately, writing though into memory.
Table 2: Data layout in data-servers

2.	checksum

In this design, we assume data error happens exclusively in the hardware storage. In other words, the data received by server side is considered as flawless: no data error happens in the network during the message transfer. 

To detect data corruption in the storage, whenever a new block of data arrives at data-server, a 32-bit checksum, computed based on the data block, is appended behind the block as a suffix, and then, all together, stored into DATA. Also, whenever some block is requested by FUSE file system, the data-server send the requested block together with the pre-computed 32-bit checksum to the FUSE file system. Notably, data corruption could happen, either in the block data or in its checksum, during the interval of data put and data get. So in the FUSE file system side, by comparing the checksum sent by data-server with the checksum computed based on the received block data, we can detect any data corruption.

CRC32 is adopted for checksum computation in this project.

3.	Function implementation

  1).	Arguments of functions
   All the values of function arguments are wrapped in a single dictionary, with the name of arguments used as the keys, passing between data-servers and file system. It makes not only the serializing and marshaling process uniform among all function calls but also makes any adjustment to arguments simpler.
   
  2).	Functions for internal use only: checksum(data)
  Checksum(data), an internal function for test use only, not registered in the server, is not available from the outside of the data-servers. We adopt CRC32 as the algorithm to compute checksum.
  
  3).	Functions for FUSE file system (distributedFS.py)
  There are 3 major changes of the basic ‘interface’ functions in data-server.  
  First, each block data used to be kept in a single key-value pair. Instead of singe string, the ‘value’ in the key-value pair for each block becomes a directory, keeping all the history versions of that block. Function ‘put’ and ‘get’ would only ‘put’ a new version or ‘get’ a latest version of the requested blocks. Thus for the most basic 3 function: put, get, pop, a new argument, version, is added to adjust this modification, which indicate the version of the current block that is transferring.
  This modification leads to the second major change, which is the behavior of function ‘pop’. For the argument version, it has an extra optional value for function ‘pop’: None. When it gets a None as the version, ‘pop’ would remove the whole block, including all the history version, and return it to the file system. If not, ‘pop’ would only remove one version of block and return the content back.
  The third change is based on the consideration for file rename. To avoid passing the entire history version of blocks among data-servers, when renaming a file, the file system could send a request to data-server, to modify the key (or path) of some block. It is handled by function keyRename().
  
  4).	Initiation procedure
  To achieve fault tolerance, the initiation procedure becomes extremely critical in data-server. It takes the responsibility of recovering, either from back-up file stored in disk or from adjacent data-server. The recovery procedure residents in the initiation function, which means every time when the data-servers starts to server, no matter last time it exited normally or crashed, it will go through the recovery procedure to make sure data consistency. (Notably, it also makes it possible for sharing corrupted data among data-servers) The initiation procedure is shown as follows.
  
  5).	Functions for test only: corrupt(path)
  To simulate the data corruption in hardware storage, we define function corrupt(path). It modifies one randomly-chosen byte in a randomly-chosen block of that file stored in that server.
Apart from the above, the usage of dataserver.py has been changed too. To run dataserver.py, type the following command, which is exactly as what is required in the project instruction. 
python dataserver.py <0 indexed server number> <ports for all data-servers separated by spaces>


# Fault-tolerant and all-or-nothing atomic file system operations module
To achieve the all-or-nothing atomicity, the general idea is that, every modification on block would generate a new version and is appended to the old versions. To implement this, a new field ‘version’ is added into file’s metadata dictionary, which keeps a list that records the file’s every block’s version.

To simplify the maintenance of this variable, writeBlock() is the only function who modifies block versions once initialized. Thus it also indicates the times that blocks have been successfully updated. Notably, the update of file’s version list could be considered as a commit point of any file modification operation. 

The major modification in this version according to the fault-tolerance and all-or-nothing atomicity designs, are writeBlock(), truncate() and rename(), whose changes we would describe later one by one. Also, when create() is called to create a new file, matainfo[‘version’] is initialized as an empty list. And in the later accesses, blocks’ version number in this list should be provided to data-servers, to indicate which version of block file system wants to access. Specifically, for reading files, we should provide the current block number to access it. While for writing file, we should increment the current block number by 1 and provide it to append a new version of block. 

Notably, since the file’s size is always changing, which leads to the number of file blocks inconstant. We can’t maintain a version list, whose length is neither constant nor equal to the number of current file blocks, if we want to keep all the history version of file. So, instead of extending or shrinking the version list every time we modify files, we always extend it and keep the list as the largest length it has ever been, to keep a record of every, both historic and latest, blocks. In some situations, where we may need to pop a block to revert the modification, we still do not need to truncate the version list to keep block record consistent with the dictionary in data-servers. Since it could only occur before the commit point, the new version list would be discards entirely, leaving the current version list unchanged.
 

Basically, most of functionalities of writing files in all-or-nothing atomic way is implemented in writeBlock() function. Function write() does nothing but call the writeBlock() and update the file’s size if writeBlock() return True, otherwise it raises ECONNREFUSED error to notify upper system the failure of writing operation.

# Test cases for data-server
We use python command window to test the functionality of data-servers. After connecting with all data-servers, we call the following functions one by one and check the return values.
1.	Basic functions 
1)	put a new block A1
2)	get the block ---> A1
3)	put an exist block A2, get the block --> A2
4)	pop the latest version of the block A2, get the block --> A1
5)	rename the block A to B, get the block --> B
6)	pop B, get the block --> False
2.	System recovery
1)	Start all data-servers, input some data
2)	Restart one server and access its data --> available
3)	Shut down one server, delete the backup file and restart it, access its data --> available

Test cases for distributed FUSE file system
Here we omit the test cases for basic file system operations behavior. In this section, we focus on the fault-tolerance, both system crash and data corruption, and all-or-nothing atomicity, and design several typical test scenarios to check the correctness of functionality.
1.	System crashes and all-or-nothing atomicity
1)	Read file when one server is down --> succeed
2)	Write file when one server is down --> failed after 30 seconds
3)	Write file when one server is down, truncate the file into original size and read it again --> get the original file data
4)	Rename file when one server is down, read the file with old name --> rename failed, file is available with old name 
5)	Read file when server 1,3 is down --> succeed
2.	Data corruption
1)	Corrupt one same file in server 1,3 and read it again --> correct content
2)	Corrupt 2 files in server 2, 4 and read it again --> correct content

# Summary and Limitation
The all-or-nothing implemented in this file system has some intrinsic limitations. So far, almost all the functions in distributedFS.py that modify the content in data-servers are individually all-or-nothing atomic, including write, truncate, rename (file only). Rename dictionary and ‘rmdir’ are not all-or-nothing atomic. However, even if all operations are individually all-or-nothing atomic, it’s not as powerful as expected and could have some surprising behavior in practice. Notably, when the OS is handling the command from Linux terminal, usually more than one FUSE file system operations would be called. Atomic behavior of each command cannot be guaranteed since multiple functions could be called when processing a single command. Here are some examples of limits and unhandled cases in this design, under which situations, either it would raise exceptions or the program’s behavior is non-deterministic.
1.	If system crashes in the middle of operations, the behavior of some function is non-deterministic.
2.	If renaming/deleting a directory happens when there is system crash, directories will be modified correctly without blocking, yet the file still remain the old version. 
3.	The data consistency between meta-server and data-servers are not guaranteed if some command calls multiple FUSE file system operations when there is system crash. For instance, in case of a new file creation, the update of meta-server will succeed while the update in the data-servers side will not, having the all-or-nothing atomic. In case of existing file, the file size will be firstly changed to 0 without any blocking but the following write will fail. Both cases will cause the non-consistency between meta-server and data-servers. 


