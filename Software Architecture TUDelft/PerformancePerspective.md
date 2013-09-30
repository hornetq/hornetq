#Performance Perspective

From [Rozanski and Woods](http://www.viewpoints-and-perspectives.info/), the performance perspective is related to the ability of the system to predictably execute within its mandated performance profile and to handle increased processing volumes. 

###Concerns

* Response Time
* Throughput
* Scalability
* Predictability
* Hardware Resource Requirements
* Peak Load Behavior

Below we will present the _throughput_ aspect as we were able to find "hard facts" related to it. 


###Throughput
Among these concerns, we will apply the _Throughput_ concern to the HornetQ system.  According to the book, throughput is defined as the amount of workload the system is capable of handling in a unit time period. The throughput of HornetQ was measured in 2011 using an industry-standard benchmark called [specJms2007](http://www.spec.org/jms2007/). In a [blog post](http://planet.jboss.org/post/8_2_million_messages_second_with_specjms) written by Clebert Suconic, we can find out details about the approach of measuring and the physical set-up.  The analysis concluded that: **HornetQ can handle 8.2 million messages per second**, thus making it the leader in enterprise messaging systems.

###Architectural tactics

The book also presents some ways that the architectural decisions that can improve performance: 

* Optimize Repeated Processing
* Reduce Contention via Replication
* Prioritize Processing
* Consolidate Related Workload
* Distribute Processing in Time
* Minimize use of Shared Resources
* Partition and Parallelize
* Use Asynchronous Processing
* Make Design Compromises

####Optimize Repeated Processing

The application of this architectural tactic can be traced at the fact that the developers of HornetQ have accorded special attention to the persistence aspect with regards to optimization.  This is due to the fact that the most common use-case in the system is message processing, and within this function, we have a persistence step.

Persistence is done by temporarily saving messages on the disk so that in case of server fault, message sending can be re-initiated such that the receivers will get the messages. 

_The first step towards "Optimize Repeated Processing" is deciding that the persistence module should be optimized._ After this, the next decision was to not use a database system for persistence, but to create a custom solution that is tailored for the messaging use-cases.

####Use Asynchronous Processing

We will use an example from Rozanski and Woods, as it is relevant for how the persistence module in HornetQ works: 

_Several desktop computers share access to a single high-quality color printer by means of a shared printer server.  Desktop applications send print requests to the print server, which manages them through a set of queues.  Print requests for even the largest jobs complete as soon as the request has been received and acknowledged by the print server, rather than having to wait for the print to physically complete. 
  Once a document has been printed, the server notifies the originating PC, and a small message box is displayed to the user. Of course, this may happen some time later - possibly not until he next day in the case of very large print jobs.  If printing fails for any reason, notification is also sent to the desktop, and it is the user’s responsibility to resubmit the job._

Internally, HornetQ uses the same approach to handling persistence.  It follows an asynchronous model that is based on [libAIO](http://lse.sourceforge.net/io/aio.html) on Linux.  This way, the system is not blocking until the “write” system call returns, but rather just sends to write command to the kernel and also provides a callback interface that will be used when the command is carried out.  The advantage of this method is that the server can also do some other processing and not wait for the I/O operations to complete. 

To understand the two different ways of handling I/O, the [blog post on developerWorks](http://www.ibm.com/developerworks/linux/library/l-async/index.html) provides two very helpful diagrams: 

* **Synchronous blocking I/O**

![image](https://f.cloud.github.com/assets/2643634/681444/aefa1c70-d9b1-11e2-84dd-1aec9f3331e2.png)

As we can see, between the sync to disk command is issued and when the operation is done, the CPU is blocked.  For some applications this is a good thing, but the developers of HornetQ wanted the CPU to do other processing while the kernel writes to disk, so they have chosen the alternative: 

* **Asynchronous non-blocking I/O**

![image](https://f.cloud.github.com/assets/2643634/681458/0a7ab8f2-d9b2-11e2-865d-99dec23664db.png)

In the HornetQ codebase, we can see these concepts present in the [AsynchronousFileImpl](https://github.com/hornetq/hornetq/blob/master/hornetq-journal/src/main/java/org/hornetq/core/asyncio/impl/AsynchronousFileImpl.java), which is the "wrapper" around the native calls that are used to send commands to the kernel.

####Make Design Compromises

The book suggests that many of the design quality attributes also have drawbacks, one of them which is performance.  For example, a loosely coupled, highly modular and coherent system tends to spend more time communicating between its modules than a tightly coupled, monolithic one does.

The POJO architecture of HornetQ could fit this category.  For one thing, the design is tightly coupled, as we could observe from the [analysis](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/inFusionAnalysis.md) we carried out with inFusion.  

#References
[1] Google AIO User Guide - http://code.google.com/p/kernel/wiki/AIOUserGuide, accessed 20 June 2013   
[2] Boost application performance with asynchronous I/O - http://www.ibm.com/developerworks/linux/library/l-async/index.html, accessed 20 June 2013   
[3] Asynchronous I/O on GNU webpage - http://www.gnu.org/software/libc/manual/html_node/Asynchronous-I_002fO.html, accessed 20 June 2013
