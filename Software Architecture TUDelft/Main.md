# Main

## Table of contents
* [Introduction to HornetQ](Main.md#introduction---the-need-for-messaging-systems-in-the-business-context)
* [Architectural Documentation](Main.md#architectural-documentation)
  * [Sketches](Main.md#sketches)
  * [Stakeholder Analysis](Main.md#stakeholder-analysis)
  * [Goal Question Metrics](Main.md#goal-question-metric-gqm)
  * [Context View](Main.md#context-view)
  * [Concurrency View](Main.md#concurrency-view)
  * [Information View](Main.md#information-view)
  * [Operational View](Main.md#operational-view)
  * [Security Pespective](Main.md#security-perspective)
  * [Peformance Pespective](Main.md#performance-perspective)
 
* [Contributions](Main.md#contributions)
* [Architectural Reflections](Main.md#architectural-reflections)
* [Conclusion](Main.md#conclusion)
* [About us](Main.md#about-us)

## Introduction - the need for messaging systems in the business context
In the business environment there are many legacy applications that still offer value.  In contrast to this, new applications come into the picture in the context of the organization.  For example, in the case of a company acquisition, the acquirer company has to integrate the applications that come with the acquisition.  As another example, we can mention the situation when we want to add a new frontend solution to an existing backend, without changing the backend. What is needed in these situations is a solution to integrate these systems in an easy way.   

The messaging system is used in this context to introduce decoupled communication among the target applications. So, by using a messaging system as a component that fits in between the target applications , the developer can concentrate on  the business rules without being concerned with the communication details or differences in platforms. 

HornetQ is one of the possible solutions to the integration problem. It is a ["multi-protocol, embeddable, very high performance, clustered, asynchronous messaging system"](http://hornetq.sourceforge.net/docs/hornetq-2.0.0.GA/user-manual/en/html/preface.html). It is used by several organizations such as TomTom or Last.fm in order to cover the business requirement described above. 


## Architectural Documentation
This section gives a brief overview of the accomplishments in past few weeks, with regards to **architectural documentation**. 

### Sketches
The [sketches](https://github.com/delftswa/ReportingRepo/blob/master/HornetQ/sketches.md) were inspired by the viewpoints of [Rozanski and Woods](http://www.viewpoints-and-perspectives.info). Our team could depict 6 sketches representing [feature overview](https://f.cloud.github.com/assets/2844949/453749/0db6e7c0-b324-11e2-9faf-29630756c5dd.png), [core architectural diagram](https://f.cloud.github.com/assets/2643634/437473/db032e5c-b0a2-11e2-98c6-9283a320e901.png), [contextual diagram](https://f.cloud.github.com/assets/2643634/454786/e075fb90-b33f-11e2-915b-725ab1f599b0.png), [sequence diagram](https://f.cloud.github.com/assets/2643634/458244/7a65f87e-b3cf-11e2-9673-b057e08afa62.png), [functional](https://f.cloud.github.com/assets/2643634/455700/5c7ce240-b358-11e2-9719-8bfbb6f691b1.png) and [development diagrams](https://f.cloud.github.com/assets/950121/459372/0a656736-b400-11e2-8639-28f8b238c7ad.jpg). These diagrams were then mapped to the [Andre van Hoek's 4 design types](https://github.com/delftswa/lectures/blob/master/02-vanDerHoekSketching.pdf)(where possible): 

* sequence diagrams -> interaction design 
* functional diagrams -> application design 
* development diagrams -> implementation design.

### Stakeholder Analysis

In the subsequent week, we analyzed the project from the [stakeholders](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/stakeholders.md)'s perspective based on the material from  [Software Systems Architecture](http://www.viewpoints-and-perspectives.info/home/stakeholders/). 

* As acquirers we found Red Hat, TomTom, Last FM, ShopZilla and OpenShift but also "minor" acquirers like Habari and Movile. 

* We analyzed the [issue tracker](https://issues.jboss.org/browse/HORNETQ) and the [latest 20 resolved or closed issues _as of the 8th of may 2013_](https://issues.jboss.org/issues/?jql=project%20%3D%20HORNETQ%20AND%20status%20in%20\(Resolved%2C%20Closed\)%20AND%20created%20%3E%3D%202013-02-14%20AND%20created%20%3C%3D%202013-05-08%20ORDER%20BY%20created%20DESC%2C%20updated%20DESC%2C%20summary%20ASC) to understand key people involved with this project. 

* We presented a [table](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/stakeholders.md#table-1) that indicated the most active people with large number of bugs/issues resolved. Based on this analysis we also classified members into [developers](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/stakeholders.md#developers), [maintainers](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/stakeholders.md#maintainers) and [testers](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/stakeholders.md#testers).

* At this point, we observed that the HornetQ project was very [well documented](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/stakeholders.md#communicators) and had a [multiple channels](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/stakeholders.md#support-staff) for support.

* Also, we could observe that hornetQ has minimum dependencies on [suppliers](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/stakeholders.md#suppliers).

* We did not manage to carry out an in-depth analysis, but we raised the signal of [possible conflicts](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/stakeholders.md#interestsconflicts) between the various stakeholders that might hinder the life cycle of the project. The stakeholder analysis should go beyond merely identification. We provide an [example](https://www.google.nl/url?sa=t&rct=j&q=&esrc=s&source=web&cd=15&ved=0CI8BEBYwDg&url=http%3A%2F%2Fwww.pacificwater.org%2Fuserfiles%2Ffile%2FIWRM%2FToolboxes%2FSTAKEHOLDER%2520Engagement%2FSTAKEHOLDERS_ANALYSIS.pdf&ei=yX_EUYCbNYGktAbC5YHADg&usg=AFQjCNFI6nIh465tQvFy26n8KIIyhd6mFQ&sig2=qvB3qVvuSD_KjQcalAQdJg&bvm=bv.48293060,d.Yms) of this important extension to the Stakeholder Analysis model.

![stakeholders](https://f.cloud.github.com/assets/950121/688185/dd205ea0-da8f-11e2-9a04-5a7d647f3ffd.png)

### Goal Question Metric (GQM)

In following week, we learned about [Goal-driven Question-based Metrics](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/Metrics.md) from Eric Bouwers. 

* Based on team consensus and observation of the development process, we set the goal - _Reduce the duration of the [development cycles](http://userwebs.serv.net/~steve/Cycle%20Time.htm) from the project managers perspective._
 
* We identified 5 question that aligned with our goal
 * _What's the velocity of issue handling?_
 * _What's the velocity of feature completion?_
 * _When does code quality indicators influence cycle time reduction?_
 * _How much is changed in a release?_
 * _What's the distribution of knowledge of the project components over developers?_

* We have created a [table](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/Metrics.md#metrics-1)  that summarizes our approach


### Context View

The [Context view](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/ContextView.md) was based on the chapter 16 of [Rozanski and Woods](http://www.viewpoints-and-perspectives.info/)

* We highlighted the key functional requirement of the system - handle the messaging needs of applications
 
* Key non-functional requirements of the system were _**reliability**_ , _**high performance**_ , _**multi-protocol (variability)**_ and _**usability**_.

* We identified and classified various External Entities. We identified typical users, hard drive, JEE framework, JBoss Application server, Twitter and Spring as external entities
 
* We identified exhaustive list of External Interfaces. We enlisted various protocols (like core, REST, STOMP, JMS) that HornetQ provides as external interfaces. Apart from this, we also found that HornetQ provides Service Provider interfaces for logging, remoting etc.  As an example of implementor, Netty is used to provide the functionality for Remoting.
 
* Next, we drew a [context diagram](https://f.cloud.github.com/assets/2643634/569465/52143fe2-c703-11e2-9dc0-7ea2d92f732b.jpg) representing all the interfaces interacting with each other.
 
* Finally, we identified two [use cases](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/ContextView.md#use-cases-of-hornetq-in-practice) which provided us with insights about how is HornetQ deployed and used in a real scenario. 

### Functional View

We have created a [Functional View](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/FunctionalView.md#functional-view) of the HornetQ Server which is composed of:
* A [diagram](https://f.cloud.github.com/assets/2643634/455700/5c7ce240-b358-11e2-9719-8bfbb6f691b1.png) showing the entities that are working together to carry out the functional requirements and the collaboration arcs
* A list of each component along with their responsibilities 

### Development View

Our Development View consists of: 

* Guide to the HornetQ [modules](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/HornetQModules.md)
* [Results](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/ExtractedDesignPatterns.md) of running [Pinot](http://www.cs.ucdavis.edu/~shini/research/pinot/) (a tool for Automatic Design Pattern Extraction from source code) - showed heavy use of the Strategy design pattern

### Concurrency View

In the [view](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/ConcurrencyView.md), we map functional elements to Concurrency units based on the guidelines in [Rozanski and Woods](http://www.viewpoints-and-perspectives.info/).
We presented three aspects of the concurrency built in HornetQ.

* The HornetQ server starts independent processes in a series of modules: PostOfficeImpl, ResourceManagerImpl, ManagementService and MemoryManager

* By studying AsynchronousFileImpl class, we see that HornetQ issues disk write requests in new threads

* By studying RemotingServiceImpl class, we can see that each connection is served in a new thread (if the configuration used is AIO), if NIO then the number of threads is bounded. 

### Information View

In [Information View](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/InformationView.md),  we identified the components responsible for storing, manipulating and distributing information in HornetQ.

* HornetQ **does not** use any database as an underlining system for storing data as it affects performance

* It uses its high-performance Journal which is based on flat files optimized for messaging use-case.
 
* There are two possible instances of Journal - Journal for Messages (responsible for messages) and Journal for binding (responsible for queue information)
 
![entities](https://f.cloud.github.com/assets/950121/681081/40f9c360-d9a7-11e2-9ed5-460f54290d68.jpg)



### Operational View

In [Operational View](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md), we identify and describe how HornetQ can be operated, administered and supported.

* There are two ways HornetQ can be managed - using [Management API](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#management-apis) or using [JMX](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#management-via-jmx) 
 
* Again, in [Management APIs](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#management-apis) there are two aspects - [Core Management API](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#core-management-apis)(for managing core resources) and [JMS Management API](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#jms-management-apis) (for managing hornetQ from JMS objecs perspective)
 
* [Core Management API](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#core-management-apis) can manage following core resources - [Server](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#core-server-management), [Address](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#core-address-management), [Queue](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#core-queue-management) and [others](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#other-core-resource-management) (Acceptors, bridge etc.)
 
* [JMS Management APIs](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#jms-management-apis) can manage following JMS resources - [JMS server](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#jms-server-management), [JMS connection factory](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#jms-connectionfactory-management), [queues](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#jms-queue-management), [topics](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#jms-topic-management).

![operationview](https://f.cloud.github.com/assets/950121/687643/9a21232c-da81-11e2-8c0c-96e5986b93ed.png)
 
* Alternately, we can use [JMX](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/OperationalView.md#management-via-jmx)

### Security Perspective

As HornetQ can be implemented in clusters as well as it allows remote access, Security is a key concern. We have derived [security pespective](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/SecurityPerspective.md) based on Rozanski and Woods.

* Basically, the security model in HornetQ is [Role based Security Model](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/SecurityPerspective.md#role-based-security-model).
 
* HornetQ allows you to assign action privileges to roles as presented [here](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/SecurityPerspective.md#security-for-queue)
 
* HornetQ ships with a default security manager: [HornetQSecurityManagerImpl](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/SecurityPerspective.md#default-security-manager-implementation).  On the other hand, HornetQ offers extension point for other security manager implementations.
 
### Performance Perspective

In [Performance Perspective](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/PerformancePerspective.md), we study the performance aspect of the project. 

* We presented some concrete, "[hard facts](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/PerformancePerspective.md#throughput)" related to the throughput of HornetQ, as part of the performance perspective. 
 
* As architectural tactic, we identified that the developers have used [Optimizing on Repeated Processing](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/PerformancePerspective.md#optimize-repeated-processing) and targeted the persistence module for optimization as it is heavily used in messaging use-case.
 
* Another architectural tactic from Rozanski and Woods was identified, namely HornetQ uses asynchronous processing using libAIO on Linux for all non-blocking writes. 

![nonblockingIO](https://f.cloud.github.com/assets/2643634/681458/0a7ab8f2-d9b2-11e2-865d-99dec23664db.png)
 
* Rozanski and Woods also suggests that there is trade-off between design and performance. The POJO design of HornetQ fit this category, in the sense that it improves performance but increases coupling.

###Other views/perspectives

There are 3 other aspects more that we would like to acknowledge.

The first one is the Deployment View.  In this regard, it would have been interesting to study how HornetQ can be deployed as stand-alone server, but also in as a cluster and the necessary configuration.  

The second aspect would be to approach the Evolution Perspective.  The developers of HornetQ have created the Service Provider Interfaces to create extension points to introduce variability, similar to the Software Product Line concept.  The analysis could start from the [spi](http://docs.jboss.org/hornetq/2.3.0.CR2/docs/api/hornetq-server/) package where the interfaces are located.  As an example of implementation, we can inspect the one for Remoting, which uses Netty.  

Finally, it would have been interesting to study the Availability Perspective, which would cover HornetQ's availability quality attribute, which is done via [Data Replication](http://docs.jboss.org/hornetq/2.2.5.Final/user-manual/en/html/ha.html).  Basically, the messages that have to be persisted are replicated to a backup server in an asynchronous manner when a node is executing correctly.  When it fails, the backup server takes over in a transparent manner from the user's perspective.  

## Contributions

Our contribution deliverables can be summarized as: 

* [code](https://github.com/hornetq/hornetq/pull/1093) - merged to the HornetQ master by Clebert Suconic
* [How maintainable is HornetQ?](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/How%20maintainable%20is%20the%20HornetQ%20codebase.md) - a study based on 5 metrics of the maintainability of HornetQ using 8 of the recent releases.  The approach was inspired by the approach employed on the Mozilla codebase and the SIG Maintainability Model. 
* [How well-tested is the HornetQ codebase?](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/TestingCoverage.md)
* [How much code duplication does the HornetQ codebase contain?](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/CodeDuplication.md)
* [Quality Assessment using inFusion](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/inFusionAnalysis.md) - using [object-oriented metrics](http://link.springer.com/book/10.1007/3-540-39538-5/page/1) to detect problems in the design of HornetQ codebase.  Among the findings we have Intensive Coupling and the presence of God classes, such as ClusterManager (and other classes that end with the "Manager" suffix). As tooling for eliminating the God Classes we recommend [jDeodorant](http://www.jdeodorant.com/) which provides good support for the [Extract Class](http://www.refactoring.com/catalog/extractClass.html) refactoring.  On the other hand, refactoring the God classes would break the existing tests and make the design less straightforward. 

##Architectural Reflections

A summary of the [Architectural Reflection](https://github.com/delftswa/ReportingRepo/blob/HornetQ-final/HornetQ/ArchitecturalReflection.md) document is provided:

* due to the fact that HornetQ is a complex enterprise system with a non-trivial underlying architecture, all of the views/perspectives apply
* the book does not provide framework for documenting architectural decisions, which we think are very important in the life cycle of the project.  We have found [literature](http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=1620096&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D1620096) that focuses on this aspect
* the Design Types of Andre van der Hoek do not map to the goal of program understanding, which was our goal in the beginning of the project, when the lecture was given
* the sketches provided an end-goal that promoted a good initial understanding of the architecture of HornetQ.  
* the Context View helped us to get “the big picture” with regards to our project.  Analyzing the context of the software project clearly distinguishes the “real-world” professional applications.  It was a very important exercise for us which lead to a deeper understanding of the functionality, because that functionality depends on who will use it and how.
* the Development View helped us get an understanding about what is the “present and future” of the project, because if the project can build easily, is well-tested, was implemented in a clean way and provides well-defined extension points, then we can conclude that the project has a future. It was interesting to apply this train of thought and understand the situation of HornetQ from this angle.
* With regards to the Stakeholder Analysis, we can affirm two things.  We think that the book by Rozanski and Woods lack an important part, namely the _analysis_ (possible conflicts, influence, timeliness).  The framework present in the book only deals with identification.  Furthermore, it was hard to map the definitions present in the book to the context of an open-source project. Other [literature](http://web.idrc.ca/en/ev-27971-201-1-DO_TOPIC.html) was identified.
* The GQM is a good approach also for program understanding, not only for quality assessment.  It basically provides the procedure to ask a question about the product and arrive at a set of significant metrics that can answer the question. 
* It was interesting to see the balance between the performance perspective and the evolution perspective, as applications that are built for performance can be tightly coupled which would make extensibility harder

## Conclusion

Based on the Viewpoints and Perspectives defined in Rozanski and Woods and other sources, we have created architectural documentation for HornetQ ranging from initial sketches to operational view. This documentation would provide a good starting point for any beginner who wishes to understand the architecture of HornetQ. During this process, we also evaluated the guidelines described in Rozanski and Wood, hence we suggested improvements in the Architectural Reflections document. Also, we contributed back to the HornetQ community with code and quality analysis. With our quality oriented analysis, we hope to provide useful information for the HornetQ team from a different perspective than the most important one - performance. 

We agree that Software Architecture is best learned by applying the concepts in a practical setting.  Moreover, it is a good idea to study the architecture of an existing successful project, rather than us creating one "out of thin air".  The combination of the approach presented in the book combined with the straightforward architectural design of HornetQ has enabled us to have a very good learning experience for which we are grateful. 

## About Us 

The people involved in this report are students of the Master Programme in Computer Science at [TU Delft](http://www.tudelft.nl/), The Netherlands:

* [Debarshi Basak](https://github.com/debarshri) 
* [Mircea Cadariu](https://github.com/mcadariu)
* [Hylke Hendriksen](https://github.com/HyHend)


