#Concurrency View

Describes the concurrency structure of the system, mapping functional elements to concurrency units to clearly identify the parts of the system that can execute concurrently, and shows how this is coordinated and controlled. [Rozanski and Woods]

##At server start-up

![concurrency](https://f.cloud.github.com/assets/2643634/676567/12e14ac8-d909-11e2-9781-1d667897f790.png)

The diagram above represents the concurrency view on server start-up.  When the server starts, it initializes a series of components that contribute to handling the functional requirements.  Each of these components start a new thread of their own.  Therefore, right at the start of the server we will have as active threads at least the following:


* the main HornetQServerImpl thread
* In the [PostOfficeImpl](https://github.com/hornetq/hornetq/blob/master/hornetq-server/src/main/java/org/hornetq/core/postoffice/impl/PostOfficeImpl.java), we have the "Reaper" thread that expires messages from all queues, if they have reached their "expiration deadline"
* [ResourceManagerImpl](https://github.com/hornetq/hornetq/blob/master/hornetq-server/src/main/java/org/hornetq/core/transaction/impl/ResourceManagerImpl.java) has the TxTimeOutHandler task that rollbacks the transactions for which the timeout has gone.  This task is executed periodically under the governance of a Scheduled Executor. 
* In [ManagementService](https://github.com/hornetq/hornetq/blob/master/hornetq-server/src/main/java/org/hornetq/core/server/management/impl/ManagementServiceImpl.java), a thread is keeping track of message statistics.  The thread's purpose is to update the statistic values repeatedly. 
* The [MemoryManager](https://github.com/hornetq/hornetq/blob/master/hornetq-server/src/main/java/org/hornetq/core/server/MemoryManager.java) is responsible for starting a daemon thread that overlooks the memory consumption in the server, and signals alerts in case it is too low

##Disk access

If HornetQ is used on an underlying Linux machine, then we can issue asynchronous file read/write commands using the [AIO](http://lse.sourceforge.net/io/aio.html) library, through native calls. In the HornetQ codebase, we can observe the use of this functionality present in the [AsynchronousFileImpl](https://github.com/hornetq/hornetq/blob/master/hornetq-journal/src/main/java/org/hornetq/core/asyncio/impl/AsynchronousFileImpl.java) class.

Each native "write" call is issued in a new thread, as can be seen from the write method of AsynchronousFileImpl class.  An important thing to note is that this call does not block, it does not wait for the operation to be finished.  The write command is just issued, and with the command we also pass a callback that is invoked when the opeartion will eventually finish. 

The events from the native subsystem are collected with the help of a "Poller" thread, which is started at the first invocation of either the write, or the read method.  

## Remoting

The [RemotingServiceImpl](https://github.com/hornetq/hornetq/blob/master/hornetq-server/src/main/java/org/hornetq/core/remoting/server/impl/RemotingServiceImpl.java) class is responsible with handling the remote connections to the server. It maintains it's own thread pool for handling incoming connections.

* if the server is running in AIO mode (if we have Linux and AIO library), then each connection will receive it's own thread
* if not and the server is running in NIO mode (based on Java NIO), then the thread number will have a limit at "nio-remoting-threads" in the configuration.  The default value is: number of cores multiplied by 3.
