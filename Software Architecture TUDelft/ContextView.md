# Context View
## Introduction - the need for messaging systems in the business context
In the business environment there are many legacy applications that still offer value.  In contrast to this, new applications come into the picture in the context of the organization.  For example, in the case of a company acquisition, the acquirer company has to integrate the applications that come with the acquisition.  As another example, we can mention the situation when we want to add a new frontend solution to an existing backend, without changing the backend. What is needed in these situations is a solution to integrate these systems in an easy way.   

The messaging system is used in this context to introduce decoupled communication among the target applications. So, by using a messaging system as a component that fits in between the target applications , the developer can concentrate on  the business rules without being concerned with the communication details or differences in platforms. 

HornetQ is one of the possible solutions to the integration problem. It is a "multi-protocol, embeddable, very high performance, clustered, asynchronous messaging system"[[1]](http://hornetq.sourceforge.net/docs/hornetq-2.0.0.GA/user-manual/en/html/preface.html). It is used by several organizations such as TomTom or Last.fm in order to cover the business requirement described above. 


##Contents
In the following sections we present an important part of the architectural documentation, namely the Context View. First the approach of creating it is explained. After that, first, the System Scope is given. Then the External Entities and Interfaces are defined and related. In the end, we present a series of graphics that encompass the Context View and some interaction diagrams. 

##The approach
To create the Context view, chapter 16 of [Rozansky and Woods](http://www.viewpoints-and-perspectives.info/) was consulted. In this chapter, [Rozansky and Woods](http://www.viewpoints-and-perspectives.info/) list a number of steps with which the Context view can be created. We followed them in solving the assignment, as they promoted a systematic approach. These steps are as follows:

* Review the goals of the system
* Review the key functionality requirements
* Identify the external entities
* Define responsibilities of external entities
* Identify the interfaces between this system and each external entity
* Identify and validate the interface definitions
* Walk through key requirements
* Walk through scenarios or use cases

Furthermore, we present the information sources that we used in the current assignment:
* [HornetQ codebase](https://github.com/hornetq) -- for lower-level informational requirements
* [HornetQ User Manual](http://docs.jboss.org/hornetq/2.2.14.Final/user-manual/) - used as a roadmap


##System Scope
### Goals
The goals of HornetQ are presented as a fragment of the interview that Tim Fox (the initiator of the project) has given to the online magazine [DZone](http://architects.dzone.com/articles/hornetq-tim-fox):

_What is the goal of HornetQ and how does it fit into the overall portfolio of JBoss projects?_

**Tim Fox**:  Sure. So HornetQ is a project to create an extremely high performance messaging system. It's an example of message‑oriented middleware. I'm sure, most of you guys, you're familiar with what message‑oriented middleware is. If you're not, our competitors in this space, in the closed source side it's, for instance, WebSphereMQ, TIBCO EMS, SonicMQ, Weblogic JMS. On the open source side, we have, for instance, ActiveMQ and OpenMQ. So those are the other messaging systems. So that's basically what the project has to do.

Two of the main design goals, from the beginning with HornetQ, are extreme performance and ease of use. So, right from the beginning we've tried to design it with those two things in mind. Currently, HornetQ is actually an unsupported community project.

Moving forward, the plan is to integrate HornetQ into Red Hat and JBoss products and provide support through our usual subscription model.



### Key Functionality Requirements
The key requirement from the messaging system is to offer the means for applications to communicate in an asynchronous manner. This is done by placing itself in the middle of the communication logical channel as indirection.  The sender application sends a message to a queue that is governed entirely by the messaging system.  The receiver application can obtain the message from the queue when it is available. 


Given the goals of the HornetQ project, the following Quality Attributes can be defined:

* HornetQ must be reliable
* HornetQ must have a high performance
* HornetQ must implement many communication protocols for interoperability



##External Entities and Interfaces

###External Entities
In this section, the internal and external systems, users, data stores and roles that may interact with HornetQ are defined. These defined systems, "entities", are: the typical user, development team and hard drive.  In addition, we note that another entity would be **any application that uses HornetQ to send/receive messages by communicating through the supported protocols**.

#### Typical user
The typical users of HornetQ are the people and or companies (organisations) who implement HornetQ in their software. Some of these organisations are defined in the [acquirers section](https://github.com/delftswa/ReportingRepo/blob/HornetQ-week/HornetQ/stakeholders.md#acquirers) of the Stakeholders document. In short these acquirers are: Red Hat, Last.fm, TomTom, Habari, Movile, OpenShift, ShopZilla and Clarity Services. Except for Red Hat, these organisations all use HornetQ in one of their core services, and depend on the workings of HornetQ to keep their users/customers satisfied. Red Hat invests in the HornetQ project and obtains money from facilitating support services.

Organisation | Usage of HornetQ
----|----
[Red Hat](http://en.wikipedia.org/wiki/Red_Hat) | Invester, Obtains money from HornetQ
[Last.fm](http://en.wikipedia.org/wiki/Last.fm) | Uses HornetQ for their streaming infrastructure.
[TomTom](https://en.wikipedia.org/wiki/TomTom) | Uses HornetQ in their Fleet management and vehicle tracking products.
[Habari](http://en.wikipedia.org/wiki/Habari) | Provides a HornetQ client, using the HornetQ API.
[Movile](http://www.movile.com/en/) |  Uses HornetQ as their messaging infrastructure.
[OpenShift](https://www.openshift.com/) | Free, auto-scaling Platform as a Service (PaaS) for applications. Openshift provides support for HornetQ.
ShopZilla | A leading source for connecting buyers and sellers online. ShopZila integrates HornetQ to meet performance and scalability requirements.
Clarity Services | Provides products like [JRuby interface for Hornetq] (https://github.com/ClarityServices/jruby-hornetq) and [Hyperic-Hornetq integration] (https://github.com/ClarityServices/hyperic-hornetq)

##### Responsibilities of the typical user
These typical users mainly have responsibilities to their own organisations. They have the obligation to their customers to deliver sufficient products and or services. These organisations use HornetQ for their core services and to keep up their products, they expect HornetQ to deliver a equal or better quality as their own services.

Exceptions to this are Red Hat, which invests in HornetQ and keeps services for the team to develop on or with. Other users are not obliged, but welcome to provide feedback to improve HornetQ. With this feedback HornetQ can improve their software with which these using organisations can improve or keep their products up to quality standards.

Thus, HornetQ has obligations to their users. These users expect a stable, fast, non error prone system which can be used in their products. They also expect HornetQ to keep these values up to current or improving standards, and they might expect HornetQ to implement functionality which they require.

####Development team
The development team is formed by the developers, testers and maintainers.  We have included a description and analysis of these categories of contributers in the [Stakeholders document](https://github.com/delftswa/ReportingRepo/blob/master/HornetQ/stakeholders.md).  

**Responsibilities of the development team**

* the developers need to have the technical oversight of the project that enables them to make decisions regarding the architecture or design of the project
* the developers have to implement new features as per the submitted feature requests
* the testers should take care of the QA aspect of software development and report found bugs
* the maintainers should fix bugs that as per the issue requests


####The hard drive

Within HornetQ, the hard drive is used to temporarily store messages for reliable delivery.  In case the server that holds the current instance of the application breaks down, upon reinitiation we can re-transmit the saved messages.  HornetQ handles its own persistence using files on the disk.  The decision to not use databases or other 3rd party tools for persistence is based on performance considerations.  This way the solution is highly optimized for the messaging scenario.

Internally in the HornetQ code, the hard drive handler is named **"journal"**.  It is implemented in Java, and the actual interactions with the hard drive have been abstracted out in order to allow for different pluggable implementations.  "From the box", HornetQ comes with 2 implementations

* Java NIO -- this provides performant interfacing with the hard drive by using functionality from the Java NIO package.

* Linux AIO native library -- HorneQ also implements a wrapper around the nativesystem calls provided by the Linux library AIO for asynchronous interaction with the file system.  The workflow is based on DMA write requests to the kernel and a callback interface that is used to signal the operation is done. This solution is more performant than NIO because the system does not block during the length of operations on the disk.    

Internally, the journal is used to persist the following components:

* Bindings -- information about the queues such as address.  This journal is always based on NIO technology, because the performance demands are not as high as in messages transfer. The actual files on the disk that are created for this journal have "hornetq-bindings" as prefix.

* JMS Data -- in this journal, HornetQ maintains all the information related to the JMS standard, such as Topics, JNDI bindings etc.  The actual files on the disk that are correspondent for this journal have the prefix "hornetq-jms"

* Messages -- this is the messaging journal that maintains the messages themselves and related information.  By default the implementation is based on Linux AIO and if this is not available NIO is used.  The actual files on the disk that are correspondent for this journal have the prefix "hornetq-data"

####JEE Application Servers
Due to the fact that HornetQ has a fully compatible [JCA](http://en.wikipedia.org/wiki/Java_EE_Connector_Architecture) adaptor, it can be integrated in application servers.  Basically, the server can receive messages from the exterior through HornetQ.  After a message is received, the JCA adaptor is used in order for the message to reach the [Message Driven Bean](http://en.wikipedia.org/wiki/Message_Driven_Bean#Message_driven_beans) where it is consumed.  

HornetQ, through the JCA adaptor, can also be used to send messages, not only to receive them and be consumed by the MDBs.  HornetQ can send messages coming from Servlets or EJBs that reside within the application server.

A graphic that is descriptive of the role of JCA and HornetQ within the Application Servers is presented below, taken from the [User Guide](http://docs.jboss.org/hornetq/2.2.2.Final/user-manual/en/html_single/#d0e606):


![architecture2](https://f.cloud.github.com/assets/2643634/569480/e1cdd684-c703-11e2-835a-f9e4b5f4a94c.jpg)

The reason there is a prohibited sign that bans the arrow between EJB and HornetQ is that if we want to send messages from an EJB we will have to create sessions and connections at each message exhange operation, which is an "anti-pattern".

#### JBoss AS

JBoss AS is a jboss trademark application server (thus AS).  The class that provide APIs for bundling JBoss logging messages to HornetQ is [HornetQJSBundle](https://github.com/hornetq/hornetq/blob/master/integration/hornetq-jboss-as-integration/src/main/java/org/hornetq/integration/jboss/HornetQJBossBundle.java).

#### Twitter 

In hornetQ, [TwitterIncomingConnectorServiceFactory](https://github.com/hornetq/hornetq/blob/master/integration/hornetq-twitter-integration/src/main/java/org/hornetq/integration/twitter/TwitterIncomingConnectorServiceFactory.java) and [TwitterOutgoingConnectorServiceFactory](https://github.com/hornetq/hornetq/blob/master/integration/hornetq-twitter-integration/src/main/java/org/hornetq/integration/twitter/TwitterOutgoingConnectorServiceFactory.java) from the package org.hornetq.integration.twitter provides external connector service APIs for [twitter](http://en.wikipedia.org/wiki/Twitter) via handlers ([IncomingTweetHandler](https://github.com/hornetq/hornetq/blob/master/integration/hornetq-twitter-integration/src/main/java/org/hornetq/integration/twitter/impl/IncomingTweetsHandler.java) and [OutgoingTweetHandler](https://github.com/hornetq/hornetq/blob/master/integration/hornetq-twitter-integration/src/main/java/org/hornetq/integration/twitter/impl/OutgoingTweetsHandler.java)). The purpose of this integration to publish or subcribe message from twitter and also manipulate them.

#### Spring

[Spring](http://en.wikipedia.org/wiki/Spring_Framework) provides configurative registration of message listener objects for transparent message-consumption from message queues via JMS, improvement of message sending over standard JMS APIs.  As HornetQ is protocol agonstic, similar services that can be utilized by Spring using HornetQ spring integration. HornetQ provides [SpringJmsBootstrap](https://github.com/hornetq/hornetq/blob/master/integration/hornetq-spring-integration/src/main/java/org/hornetq/integration/spring/SpringJmsBootstrap.java) (in the package org.hornetq.integration.spring) that allows a user to bind a message to Spring's [BeanFactory](http://static.springsource.org/spring/docs/2.5.x/api/org/springframework/beans/factory/BeanFactory.html) which is the root interface for accessing a Spring bean container.


###External Interfaces

The fundamental question that has lead us in finding the External Interfaces is the following: _how do Client Applications "speak" with HornetQ in order to carry out their messaging use-cases?_, in other words, what would be the "gateways" and how are they defined? 

#### Typical user to HornetQ
Users of HornetQ have multiple options to communicate with HornetQ, it's parent organisation and or it's developers. Though users do not have to communicate with any of these, because the HornetQ software and source code is openly available on the [JBoss website](http://www.jboss.org/hornetq/downloads) and the source is both on [JBoss](https://community.jboss.org/wiki/HornetQSourceLocation) and [GitHub](https://github.com/hornetq/hornetq). The available communication channels are the HornetQ [Wiki](https://community.jboss.org/wiki/HornetQ), [IRC channel](http://irc.lc/freenode/hornetq/), [forum](https://community.jboss.org/en/hornetq?view=discussions) and [Twitter](https://twitter.com/hornetq). HornetQ also maintains a [blog](http://hornetq.blogspot.nl/) on which anyone can monitor the current status of the project.

HornetQ is also well documented. The project maintains a [QuickStart manual](http://docs.jboss.org/hornetq/2.3.0.Final/docs/quickstart-guide/html/index.html), [User manual](http://docs.jboss.org/hornetq/2.3.0.Final/docs/user-manual/html/index.html) and a [JavaDoc page](http://docs.jboss.org/hornetq/2.3.0.CR2/docs/api/). The QuickStart manual provides a few chapters on how to set up project implementing a HornetQ client and server. The User manual extensively explains HornetQ, the architecture, the workings of HornetQ and provides many explaining code snippets. The JavaDoc page contains information about functionality and usage of all classes and functions HornetQ implements. At last a book, the [HornetQ developers guide](http://www.amazon.com/HornetQ-Messaging-Developers-Guide-Giacomelli/dp/1849518408) is being sold. This book contains for example a large part of the User manual.

####Transport providers - Netty
External communication of HornetQ is enabled by the transport layer.  It defines its own Service Provider Interface that enables plugging in a new service provided that enables communication relatively straightforward.  The default transport provider in HornetQ is [Netty](http://netty.io/). In the transport layer, there are two important concepts that HornetQ are used to handle networking concerns:

* acceptor -- they are described in xml in the hornetq-configuration.xml.  Acceptors define a way for HornetQ servers to accept connections.  We provide an example: 

```xml
<acceptors>                
    <acceptor name="netty">
        <factory-class>
org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory
        </factory-class>
        <param key="port" value="5446"/>
    </acceptor>
</acceptors>    
```
In this example, we specify that the functionality for the acceptor will be based on Netty and it will receive requests on port 5446.  Additionally, we define the factory class that will create the implementation necessary to handle this type of connection.  In this case, we will use the NettyAcceptorFactory.  

* connectors -- they are also described in the hornetq-configuration.xml and they define how do the clients connect to 
HornetQ server.  As with acceptors, an example is provided: 

```xml
<connectors>
    <connector name="netty">
        <factory-class>
            org.hornetq.core.remoting.impl.netty.NettyConnectorFactory
        </factory-class>
        <param key="port" value="5446"/>
    </connector>
</connectors>  
```
The explanation is similar to the one above for acceptors, we define a connector that will be provided by Netty that will use port 5446. 

The connectors need to be also defined on the server besides the acceptors because sometimes the server plays the role of a client, for example in the case of clustering. 

####HornetQ JMS Support
[JMS](http://en.wikipedia.org/wiki/Java_Message_Service) is a messaging standard that is part of the JEE specification.  It is a popular API that is implemented by most messaging systems.  The problem with this API is that it only offers a programmatic standard, but it does not ensure interoperability between clients and servers if they are based on different vendors which have different wire formats.

The API of HornetQ is fully compliant with JMS version 1.1

####HornetQ Core API
The JMS standard might not cover all the functionality needs of a client application.  Therefore, HornetQ also proposes a direct API to access all the functionality, which represents a superset of the features standardized by JMS. 


#### HornetQ REST Interface

_"The HornetQ [REST](http://www.jboss.org/hornetq/rest.html) interface allows you to leverage the reliability and scalability features of HornetQ over a simple REST/HTTP interface. Messages are produced and consumed by sending and receiving simple HTTP messages containing the XML or JSON document you want to exchange."_

This interface can be exposed to other external entities that wish to call APIs remotely over HTTP. It makes HornetQ programming language agnostics, scalable and inter-operable as all the method invocations will be web service calls.

HornetQ REST interface allows:

* Duplicate detection of messages while producing messages
* Pull and Push of message consumers
* Use and acknowledgement of various Protocols
* Create new queues and topics
* Combination of JMS and REST producers and consumers
* Simple Message transformations

#### HornetQ STOMP Support
[STOMP](http://en.wikipedia.org/wiki/Streaming_Text_Oriented_Messaging_Protocol) is a text-based protocol that enables clients that "speak" this protocol connect to any Message Broker that handles it.  The benefit of using this protocol is that it is language-agnostic which promotes interoperability.

In order for HornetQ to handle STOMP message exchanges, we have to define a Netty acceptor with "stomp" as attribute in the following way: 

```xml
<acceptor name="stomp-acceptor">
   <factory-class>org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory</factory-class>
   <param key="protocol"  value="stomp"/>
   <param key="port"  value="61613"/>
</acceptor>
```
This way HornetQ server will accept STOMP connections on the port 61613.  This port number is predefined in STOMP brokers. 

###Service Provider Interfaces
####Interfacing disk access
The interface that is used to abstract out the interaction with the file system is _SequentialFile_ in the package org.hornetq.core.journal.  The [javadoc entry](http://hornetq.sourceforge.net/docs/hornetq-2.0.0.BETA5/api/org/hornetq/core/journal/SequentialFile.html) presents all the methods that have to be implemented by disk handlers in order to be successfully used by HornetQ.  Among the methods we can distinguish two pair of methods with different arguments, namely:

* read(java.nio.ByteBuffer bytes)

* read(java.nio.ByteBuffer bytes, IOCallback callback)

and

* write(java.nio.ByteBuffer bytes, boolean sync)
* write(java.nio.ByteBuffer bytes, boolean sync, IOCallback callback) 

The difference of arguments in both cases signify that there are two kinds of file access, synchronous (first method declaration in both cases) and asynchronous (second method in both cases) -- in the asynchronous version we also have to give a callback interface so that the operation can signal the successful/unsuccessful operation.  

####Service Provider Interface packages

The HornetQ implementation contains a series of packages which have as contents **interfaces** that define the methods that can be implemented by pluggable service providers for logging, naming, protocol, remoting and security concerns. 

* **org.hornetq.spi.core.logging** - interfaces for logging implementations
    * LogDelegate - logging operations that are delegated to the logging framework
    * LogDelegateFactory - creates LogDelegate instances
        

* **org.hornetq.spi.core.naming** - naming used for binding functionality
    * BindingRegistry - abstract interface for a registry to store endpoints like connection factories into
        

* **org.hornetq.spi.core.protocol** - abstractions for protocols such as STOMP,AMQP,etc
    * ConnectionEntry - starting point in creating a connection on a specific protocol
    * ProtocolManager - manages communication across a network following a protocol (e.g receives buffer)
    * ProtocolManagerFactory - creates ProtocolManager instances
    * ProtocolType - enum type with following contents: CORE, STOMP, STOMP_WS, AMQP, AARDVARK
    * RemotingConnection - represents a connection between a client and a server
    * SessionCallback - send messages/large messages across the network
        


* **org.hornetq.spi.core.remoting** - this package contains interfaces that have to be implemented by remoting providers in order to be used by HornetQ.
    * Acceptor - an Acceptor is used by the Remoting Service to allow clients to connect. It should also take care of the dispatching to the Remoting Service's Dispatcher. 
    * AcceptorFactory - responsible for creating Acceptors
    * BufferDecoder - operations carried out before the call to bufferReceived method
    * BufferHandler - handles buffers received by the Acceptor
    * Connection - used by a channel to write data to
    * ConnectionLifeCycleListener - an object of this type is called by the remoting implementation to notify of connection events.
    * Connector - objects of this type are used by the client for creating and controlling a connection.
    * ConnectorFactory - it is used by the clients for creating connectors
    * ReadyListener - signals that the channel is ready for writing

* **org.hornetq.spi.core.security** - this is the Service Provider Interface that security providers can implement for being used with HornetQ
    * HornetQSecurityManager - validate a user when connecting to the server and issue actions




##Context Diagrams
As an introduction to this section we present a diagram where we have the HornetQ project in the middle of its context and is represented as a black-box.

![deb1](https://f.cloud.github.com/assets/2643634/569465/52143fe2-c703-11e2-9dc0-7ea2d92f732b.jpg)

####Messaging Scenarios (Models)
HornetQ provides two models of messaging (point-to-point and publish-subscribe). This fact is important with regards to the context view because it describes the way in which applications work with HornetQ.  The applications always need 
to know the "messaging model" before engaging in successful message exchange. 

The communication involves the senders, the messaging system, and the specified receivers. With regards to the environment, the responsibilities of HornetQ is receiving the message, determine the message recipients and perform the routing, 
and send the message to the recipients.  The collaborators are the sender (applications that send messages to the messaging system) and the receivers (applications that receive messages from the messaging system). 

![hornetq-context1](https://f.cloud.github.com/assets/2643634/454786/e075fb90-b33f-11e2-915b-725ab1f599b0.png)

The second model (publish-subscribe) addresses the case when we can have types of messages and destination applications are interested in receiving combinations of types of messages.  The sender (also called a publisher in this case) creates topics that define types. 

The messaging system responsibility is maintaining the subscribers subscription information and transporting the message to the subscribed applications.  It enables the publisher to publish messages and the subscriber to subscribe to topics and receive 
messages.  

![hornetq-context2](https://f.cloud.github.com/assets/2643634/454788/e9ae6b5c-b33f-11e2-843f-faa9e99c27ae.png)

##Use cases of HornetQ in practice 

*  **Last.fm**

[Last.fm](http://java.dzone.com/articles/case-study-how-lastfm-uses) is an online music service that tracks the music people listen to and generates recommendations and custom online radio stations based on this information. It allows user to track the song they listen to from Internet radio station, music player or portable devices. 

Because they were having issues with their current messaging server, they were interested in integrating HornetQ in their infrastructure to improve performance and availability while keeping hardware resources under control.

Their infrastructure basically consists of standalone Java applications (the streamers) whose purpose is to stream music. When a user starts listening to a song, all streamers are informed of that and the streamer the user is streaming from stores the id. If the user then attempts to open a second stream, the second streamer will put the id on the JMS topic, the first streamer will then pick that up and see the user is already connected to it and kick them off. This effectively limits users to one stream at a time.
The streamers use a publish/subscribe JMS topology so that each streamer receives connection control messages sent by all of them. For this purpose, JMS scalability is a main requirement. By switching to HornetQ, they except its performance and scalability to meet their needs as their streaming capacity grows.

![deb3](https://f.cloud.github.com/assets/2643634/569509/07e31e5e-c706-11e2-9163-3ac6a92c83d4.jpg)
  
The figure presents a walkthrough across a typical use case wherein a user who connects to one of the streamers should not be allowed to connected to another streamer as it is aware of this fact because other streamers have communicated this information. Therefore, there is only streamer per user which is very efficient.    


* **SEDA**

[SEDA](http://www.eecs.harvard.edu/~mdw/proj/seda/), is a design for highly-concurrent servers based on a hybrid of event-driven and thread-driven concurrency. The core idea is to break the server logic into a series of stages connected with queues; each stage has a (small and  dynamically-sized) thread pool to process incoming events, and passes events to other stages.

One of the pre-conditions of SEDA mode is to give reliable return results. This requirement poses a challenge to find a feasible data communication channel for data transferring when stage/state in the JBPM swaps. Therefore, it uses HornetQ as it provides a reliable asynchronous messaging processing system. Also, it gives a built-in scalability to the existing SEDA model. 

The figure gives an overview of how hornetQ is utilized along with other products in the ecosystem. Both JBPM and the execution engine depends upon the queue exposed by the HornetQ for maintaining queue for each stages as a result of splitting the server logic.

![deb4](https://f.cloud.github.com/assets/2643634/569565/091ef31c-c709-11e2-8751-cd3d5b0aeca1.jpg)

