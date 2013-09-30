# Sketches
## HornetQ
HornetQ is a multi-protocol, embeddable, high performance, clustered, asynchronous messaging system. HornetQ is written in Java and can be used stand-alone or be integrated with other Java systems (e.g:Spring,JBoss AS). The developers state a "high availability" and "suburb performance".

## Sketches
This section shows various sketches of the HornetQ system from different angles. Where possible, we map the perspective from which we have created the sketch to the four design types presented by Andre van der Hoek.  
The chosen views that determine the sketches show the entire system from its goals, functionality to high-level internal structure and actual use.


### Feature overview
The feature overview is a representation of all the features the system has. From a business perspective, it is important to have the overview of the services provided by the system in order to reason about whether it meets the initial functional/non-functional requirements.  

For HornetQ, a wiki and user manual is available that covers the aspects of the system as seen from a user perspective. By studying this documentation, the features could be extracted. We have grouped these features. The result is a structured overview of the system showing its features.  

The creation of a feature overview sketch was helpful for getting an initial understanding of the system. 

![Application Design Sketch](https://f.cloud.github.com/assets/2844949/453749/0db6e7c0-b324-11e2-9faf-29630756c5dd.png)

### Architectural Overview
The Architectural Overview shows a high level overview of the system's architecture showing the main elements and relations to each other.  

The documention of HornetQ describes the core architecture of the system. It states that HornetQ implements a client-server architecture. Each server consists of a persistent journal, dealing with the messages, persistence and other services. The clients, that can be on different physical machines, interact with each others via the server. to communicate with the server, two APIs can be used, the Core client api deveoloped by the developers of HornetQ themselves and the JMS client api that uses the Java Messaging Service. As the server is not able to work with JMS messages, an JMS facade is put between user and the server to allow for sending messages that adheres to JMS standard. The figure below shows an overview of this structure.


Having this structure available is helpful as it gives a quick high-level overview of the architecture of the system. Especially since we are all new to HornetQ, this overview gives a starting point in studying the system in more detail.

![Architecture Design Sketch](https://f.cloud.github.com/assets/2643634/437473/db032e5c-b0a2-11e2-98c6-9283a320e901.png)

### Context View
In order to describe the context, we first have to understand what is the problem that HornetQ solves, as it provides an illustration of
the domain that it covers. The problem can be formulated in the following way: How can we integrate applications in a loosely coupled way (without 
a common interface) and offer the applications the ability to interact with eachother?

Therefore, the environment of the project is composed of the applications that have to be integrated in a loosely coupled way. HornetQ's position in the 
environment is a middleware services provider that receives messages and routes them to the destinations. Therefore the decoupled communication 
is established by the message passing infrastructure provided by HornetQ through point-to-point or publish/subscribe models.

![context_1](https://f.cloud.github.com/assets/2643634/454786/e075fb90-b33f-11e2-915b-725ab1f599b0.png)
![context_2](https://f.cloud.github.com/assets/2643634/454788/e9ae6b5c-b33f-11e2-843f-faa9e99c27ae.png)

### Functional View
The below diagram represents the functional view of the system. The nodes are the functional objects along with the interactions (dotted lines). The points of connection of the lines with the nodes represent interfaces. Along the interaction lines, various kind of data flows which is not represented in this graphic.

The functional view diagram was created after inspecting what are the functional components that the server uses in order to provide the features. 

The Functional View may apply to Application Design among the design types.
![functional_view](https://f.cloud.github.com/assets/2643634/455700/5c7ce240-b358-11e2-9719-8bfbb6f691b1.png)

### Sequence Diagrams
The first diagram presents the workflow (method invocations and types on which the methods are invoked) in the point-to-point messaging scenario.  In the graphic, we consider that the queues are already created, so we can directly use them.  
The second diagram presents the workflow in the publish/subscribe model based on topics (as per the JMS model).  We assume the topics and server startup are already done.  
We have created these sketches after we understood by the help of the examples contained in the project how to work with the system.  
The two sketches resemble to the Interaction Design concept because they present the steps that are required from the user in order to use the system for messaging purposes. 
![Interaction Design Sketch](https://f.cloud.github.com/assets/2643634/458244/7a65f87e-b3cf-11e2-9673-b057e08afa62.png)
![Interaction Design Sketch](https://f.cloud.github.com/assets/2643634/458476/165e9170-b3d8-11e2-8d98-c6b9a68b141d.png)





### Development View
The two diagrams below show the development view of HornetQ. The development view is important because the system is fairly complex and a sketch from this viewpoint provides a good starting point in understanding the internal implementation structure of the system. 

A high-level implementation overview contains the following packages: 
* Core-client
* JMS-client
* HornetQ server
* HornetQ JMS server
* Native interfaces
* Commons
* Resource Adapter (RA)
* REST client and services
* HornetQ journal
* Service Archive

The Core-client is used by other applications to communicate with the HornetQ Server, through the means of 
API and SPI (service provider interface) for other applications. The JMS client provides a facade for users applications that want to use the messaging system using JMS. Native Interfaces are used by HornetQ Journal component to handle persistency by writing to disk. Hornetq Commons has the utility functions for various purposes (e.g.: buffering). REST client and services provides the means to other applications to invoke operations through the REST interface. The delivery of messages to a [Message Driven-bean] (http://docs.jboss.org/hornetq/2.2.5.Final/user-manual/en/html/appserver-integration.html) using HornetQ is configured on the JCA Adapter provided by Resource Adapter. The second figure below shows a closer look at the Core-client component.

To create this sketch, we analyzed the source code with inFusion, explored the issue tracker, investigated the change logs as well as the HornetQ user manual.  

We can relate these diagrams to the Implementation Design from the four design types presented by Andre van der Hoek.


![implementation_diagram](https://f.cloud.github.com/assets/950121/459372/0a656736-b400-11e2-8639-28f8b238c7ad.jpg)

![core_client](https://f.cloud.github.com/assets/950121/459085/f75b9920-b3f6-11e2-9612-c4b384a2f23f.jpg)




