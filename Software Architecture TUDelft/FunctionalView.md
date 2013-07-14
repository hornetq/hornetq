##Functional View

In their [book](http://www.viewpoints-and-perspectives.info/), 	Rozanski and Woods define the Functional view as being: _"the system's runtine functional elements and their responsibilities, interfaces and primary interactions"_.

By reading through the code, we can quickly distinguish the main subsystems that work together in order to carry out the fundamental use case of HornetQ, namely message passing.  

The diagram below introduces the entities, along with connections in case of any kind of collaboration.

![image-functional](https://f.cloud.github.com/assets/2643634/455700/5c7ce240-b358-11e2-9719-8bfbb6f691b1.png)

Furthermore, we provide an overview of the responsibilities of each subsystem:

* Server 
  * represents the central point of the system
	* controls the life cycle of the other sub systems 
	* serve connections (session management, requested functionality)

* Post Office
	* maintains bindings (queues) information
	* maintains address information
	* maintains information about which bindings belong to which address
	* functionality to route received messages at an address to the corresponding queues (bindings) that belong to that address
	* maintain and employ a message expiration policy

* Storage
	* temporarily store the messages
	* store information about the addresses and queues in use
	* provide functionality to manipulate the stored data, such as delete
	* offer transactional behavior

* Security 
	* maintain a set of boundaries between the type of actions one user can do
	* enforce checking whether the user is allowed to do the requested action or not

* Remoting
	* provide the infrastructure that allows the server to send/receive remote messages
	* provide the means to encode/decode the messages if necessary 

* Management
	* provide the means of interaction with the server when it is running
	* interaction means: 
		* manipulating internal data
		* get statistical information about internal data
		* various operations on the server itself, such as starting/stopping
* Node management
	* offer support mechanism for the node that the current instance of the server is executing, such as memory overflow protection and alerts

* Clustering
	* provide the means for servers to discover other servers in the network
	* perform server-side load balancing for messages 

* Replication
	* provide a replication scheme such that messages that are persistent are replicated on other machines, not just the one that received the message
