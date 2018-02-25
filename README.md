## Distributed File System
Key features:
#### Parallel retrievals:
large files are split into multiple chunks. Client applications retrieve these chunks in parallel.
#### Interoperability: 
the DFS uses Google Protocol Buffers to serialize messages. This allows other applications to easily implement the wire format.
#### Fault tolerance: 
Detects and withstands two concurrent storage node failures and continue operating normally. It also recovers corrupted files.

The implemention is done in java. Communication between components are implemented via sockets.

The components included are:

    * Controller
    * Storage Node
    * Client

#### Controller
The Controller is responsible for managing resources in the system, somewhat like an HDFS NameNode. When a new storage node joins the DFS, the first thing it does is contact the Controller. The Controller manages a few data structures:

* A list of active nodes
* A list of files
* For each file, a list of its chunks and where they are located

When clients wish to store a new file, they send a storage request to the controller, and it replies with a list of destination storage nodes to send the chunks to. The Controller itself does not see any of the actual files, only their metadata.

The Controller is also responsible for detecting storage node failures and ensuring the system replication level is maintained. In this DFS, every chunk is replicated twice for a total of 3 duplicate chunks. This means if a system goes down, we can re-route retrievals to a backup copy.

#### Storage Node
Storage nodes are responsible for storing and retrieving file chunks. When a chunk is stored, it is checksummed so on-disk corruption can be detected.

Some messages that storage node accept:

    * Store chunk [File name, Chunk Number, Chunk Data]
    * Retrieve chunk [File name, Chunk Number]

The storage nodes send a heartbeat to the controller periodically. The heartbeat includes chunk metadata to keep the Controller up to date, while also letting it know that the node is still alive. It also includes the amount of free space available at the node so that the Controller has an idea of resource availability.

#### Client
The client’s main functions include:

    * Breaking files into chunks, asking the Controller where to store them, and then sending them to the appropriate storage node(s). 
    * Retrieving files in parallel.
    * Print out a list of files (retrieved from the Controller), and the total available disk space in the cluster (in GB).
