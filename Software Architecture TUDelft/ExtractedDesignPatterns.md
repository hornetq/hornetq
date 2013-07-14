#Automated Design Pattern Extraction 

##Introduction

This effort was fueled by our desire to find a way to somehow extract _design decisions_ from the codebase.  One way to do this would be to mine all the instances of the [GoF Design Patterns](http://c2.com/cgi/wiki?GangOfFour). By recognizing a design pattern in code, we can immediately look it up in the book and see what its intent is. For this purpose we have used [pinot](http://www.cs.ucdavis.edu/~shini/research/pinot/). The project under observation was the 2.2.14 release of HornetQ, the first one before the last release which is 2.3.0. We haven't used the last one due to it's directory structure which makes it easier to be given as input to the tool.  

At first sight, the tool has missed the class _SequentialFileFactory_, which even by reading it's name we can understand that it's an instance of the Factory Method design pattern. On the other hand, the numerous Strategy patterns present in the codebase it was able to find, and this is clearly the most used design pattern in HornetQ, and it provided a positive confirmation to our intuition.  


The results of running the tool are presented below, first a categorization of the mined design patterns and after that the details of the tool's output, in order to pinpoint where exactly does every design pattern appear.


##Aggregate

Pattern Instance Statistics:

* Creational Patterns:

| Pattern        | Count           |
| ------------- |:-------------:| 
| Abstract Factory     | 0 |
| Factory Method      | 0     |
| Singleton |0     | 


* Structural Patterns

| Pattern        | Count           |
| ------------- |:-------------:| 
| Adapter     | 4 |
| Bridge      | 2     |
| Composite |0     | 
| Decorator | 1     | 
| Facade | 4    |
| Flyweight | 1   |
| Proxy | 1   |


* Behavioral Patterns

| Pattern        | Count           |
| ------------- |:-------------:| 
| Chain of Responsibility     | 1 |
| Mediator      | 0     |
| Observer |2    | 
| State |0     | 
| Strategy |  16   |
| Template Method    | 0  |
| Visitor | 2  |




## Details (output of the tool)




--------- Original GoF Patterns ----------

Chain of Responsibility Pattern  
TransactionCallback is a Chain of Responsibility Handler class  
onError is a handle operation  
delegateCompletion of type IOAsyncTask propogates the request  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/journal/impl/TransactionCallback.java  



Decorator Pattern  
HornetQRAXAResource is a Decorator class  
setTransactionTimeout is a decorate operation  
xaResource of type XAResource is the Decoratee class  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/ra/HornetQRAXAResource.java  

 IOAsyncTask  
 XAResource  
Bridge Pattern.  
EmbeddedHornetQ is abstract.  
HornetQComponent is an interface.  
EmbeddedHornetQ delegates HornetQComponent.  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/server/embedded/EmbeddedHornetQ.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/server/HornetQComponent.java  

Bridge Pattern.  
EmbeddedJMS is abstract.  
BindingRegistry is an interface.  
EmbeddedJMS delegates BindingRegistry.  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/jms/server/embedded/EmbeddedJMS.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/spi/core/naming/BindingRegistry.java  

Strategy Pattern.  
ClientRequestor is the Context class.  
ClientSession is the Strategy interface.  
Concrete Strategy classes:  ClientSessionInternal  
Delegation through queueSession of type ClientSession  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/api/core/client/ClientRequestor.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/api/core/client/ClientSession.java  

Strategy Pattern.  
ClientRequestor is the Context class.  
ClientProducer is the Strategy interface.  
Concrete Strategy classes:  ClientProducerInternal ClientProducerImpl   
Delegation through requestProducer of type ClientProducer  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/api/core/client/ClientRequestor.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/api/core/client/ClientProducer.java  

Strategy Pattern.  
ClientRequestor is the Context class.  
ClientConsumer is the Strategy interface.  
Concrete Strategy classes:  ClientConsumerInternal  
Delegation through replyConsumer of type ClientConsumer  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/api/core/client/ClientRequestor.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/api/core/client/ClientConsumer.java  

Strategy Pattern.  
ClientProducerCreditsImpl is the Context class.  
ClientSessionInternal is the Strategy interface.  
Concrete Strategy classes:  ClientSessionImpl DelegatingSession  
Delegation through session of type ClientSessionInternal  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/client/impl/ClientProducerCreditsImpl.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/client/impl/ClientSessionInternal.java   

Strategy Pattern.  
ClientProducerImpl is the Context class.  
ClientSessionInternal is the Strategy interface.  
Concrete Strategy classes:  ClientSessionImpl DelegatingSession  
Delegation through session of type ClientSessionInternal  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/client/impl/ClientProducerImpl.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/client/impl/ClientSessionInternal.java  

Strategy Pattern.  
ClientProducerImpl is the Context class.  
Channel is the Strategy interface.  
Concrete Strategy classes:  ChannelImpl  
Delegation through channel of type Channel  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/client/impl/ClientProducerImpl.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/core/Channel.java  

Strategy Pattern.  
ClientProducerImpl is the Context class.  
TokenBucketLimiter is the Strategy interface.  
Concrete Strategy classes:  TokenBucketLimiterImpl  
Delegation through rateLimiter of type TokenBucketLimiter  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/client/impl/ClientProducerImpl.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/utils/TokenBucketLimiter.java  

Strategy Pattern.  
ClientProducerImpl is the Context class.  
ClientProducerCredits is the Strategy interface.  
Concrete Strategy classes:  ClientProducerCreditsNoFlowControl ClientProducerCreditsImpl  
Delegation through credits of type ClientProducerCredits  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/client/impl/ClientProducerImpl.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/client/impl/ClientProducerCredits.java  

Strategy Pattern.  
ConnectionEntry is the Context class.  
RemotingConnection is the Strategy interface.  
Concrete Strategy classes:  CoreRemotingConnection StompConnection  
Delegation through connection of type RemotingConnection  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/spi/core/protocol/ConnectionEntry.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/spi/core/protocol/RemotingConnection.java  

Strategy Pattern.  
CoreSessionCallback is the Context class.  
Channel is the Strategy interface.   
Concrete Strategy classes:  ChannelImpl  
Delegation through channel of type Channel  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/core/impl/CoreSessionCallback.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/core/Channel.java  

Strategy Pattern.  
CoreSessionCallback is the Context class.  
ProtocolManager is the Strategy interface.  
Concrete Strategy classes:  StompProtocolManager CoreProtocolManager  
Delegation through protocolManager of type ProtocolManager  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/core/impl/CoreSessionCallback.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/spi/core/protocol/ProtocolManager.java  

Strategy Pattern.  
DeliveryImpl is the Context class.  
MessageReference is the Strategy interface.  
Concrete Strategy classes:  PagedReference MessageReferenceImpl HolderReference  
Delegation through reference of type MessageReference  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/server/impl/DeliveryImpl.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/server/MessageReference.java  

Strategy Pattern.  
HornetQJMSStarterService is the Context class.  
HornetQStarterServiceMBean is the Strategy interface.  
Concrete Strategy classes:  HornetQStarterService  
Delegation through service of type HornetQStarterServiceMBean  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/service/HornetQJMSStarterService.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/service/HornetQStarterServiceMBean.java  

Strategy Pattern.  
HornetQStarterService is the Context class.  
JBossASSecurityManagerServiceMBean is the Strategy interface.  
Concrete Strategy classes:  JBossASSecurityManagerService  
Delegation through securityManagerService of type JBossASSecurityManagerServiceMBean  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/service/HornetQStarterService.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/service/JBossASSecurityManagerServiceMBean.java  

Strategy Pattern.  
HornetQStarterService is the Context class.  
HornetQFileConfigurationServiceMBean is the Strategy interface.  
Concrete Strategy classes:  HornetQFileConfigurationService  
Delegation through configurationService of type HornetQFileConfigurationServiceMBean  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/service/HornetQStarterService.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/service/HornetQFileConfigurationServiceMBean.java  

Strategy Pattern.  
HornetQStarterService is the Context class.  
HornetQServer is the Strategy interface.   
Concrete Strategy classes:  HornetQServerImpl  
Delegation through server of type HornetQServer  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/service/HornetQStarterService.java,  
               /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/server/HornetQServer.java  

Flyweight Pattern.  
FilterConstants is a flyweight factory.  
HORNETQ_USERID is a flyweight object (declared public-static-final).  
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/api/core/FilterConstants.java  

Flyweight Pattern.  
ResourceNames is a flyweight factory.   
CORE_SERVER is a flyweight object (declared public-static-final).  
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/api/core/management/ResourceNames.java  

Flyweight Pattern.  
HornetQJMSConstants is a flyweight factory.  
JMS_HORNETQ_INPUT_STREAM is a flyweight object (declared public-static-final).  
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/api/jms/HornetQJMSConstants.java  

Flyweight Pattern.  
Stomp is a flyweight factory.  
NULL is a flyweight object (declared public-static-final).  
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java  

Flyweight Pattern.  
Commands is a flyweight factory.  
CONNECT is a flyweight object (declared public-static-final).  
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java  

Flyweight Pattern.  
Responses is a flyweight factory.  
CONNECTED is a flyweight object (declared public-static-final).  
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java  

Flyweight Pattern.  
Headers is a flyweight factory.  
SEPARATOR is a flyweight object (declared public-static-final).
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java  

Flyweight Pattern.  
Response is a flyweight factory.
RECEIPT_ID is a flyweight object (declared public-static-final).
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java  

Flyweight Pattern.  
Send is a flyweight factory.
DESTINATION is a flyweight object (declared public-static-final).  
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java  

Flyweight Pattern.  
Message is a flyweight factory.
MESSAGE_ID is a flyweight object (declared public-static-final).
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java  
 
Flyweight Pattern.  
Subscribe is a flyweight factory.
DESTINATION is a flyweight object (declared public-static-final).
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java  

Flyweight Pattern.  
AckModeValues is a flyweight factory.
AUTO is a flyweight object (declared public-static-final).
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java

Flyweight Pattern.
Unsubscribe is a flyweight factory.
DESTINATION is a flyweight object (declared public-static-final).
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java

Flyweight Pattern.
Connect is a flyweight factory.
LOGIN is a flyweight object (declared public-static-final).
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java

Flyweight Pattern.
Error is a flyweight factory.
MESSAGE is a flyweight object (declared public-static-final).
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java

Flyweight Pattern.
Connected is a flyweight factory.
SESSION is a flyweight object (declared public-static-final).
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java  

Flyweight Pattern.  
Ack is a flyweight factory.  
MESSAGE_ID is a flyweight object (declared public-static-final).  
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/protocol/stomp/Stomp.java  

Flyweight Pattern.  
XmlDataConstants is a flyweight factory.  
XML_VERSION is a flyweight object (declared public-static-final).
File location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/persistence/impl/journal/XmlDataConstants.java  

Visitor pattern found.  
ClientMessage is an abstract Visitor class.  
ClientRequestor is a Vistee class.  
request is the accept method.  
putStringProperty is the visit method.  
REPLYTO_HEADER_NAME is exposed to visitor ClientMessage  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/api/core/client/ClientRequestor.java  

Visitor pattern found.  
HornetQBuffer is an abstract Visitor class.  
ByteArrayEncoding is a Vistee class.  
EncodingSupport is an abstract Visitee class.  
 ServerMessage PersistedRoles PersistedAddressSetting ByteArrayEncoding PageTransactionInfo PagedMessage AddressSettings JournalInternalRecord NullEncoding NullEncoding MyRecord IDCounterEncoding XidEncoding HeuristicCompletionEncoding GroupingEncoding PersistentQueueBindingEncoding LargeMessageEncoding PendingLargeMessageEncoding DeliveryCountUpdateEncoding QueueEncoding DeleteEncoding PageUpdateTXEncoding DuplicateIDEncoding PageCountRecord PageCountRecordInc CursorAckRecordEncoding PersistedJNDI PersistedDestination ConnectionFactoryConfiguration PersistedConnectionFactory LargeServerMessage  
encode is the accept method.  
writeBytes is the visit method.  
data is exposed to visitor HornetQBuffer  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/journal/impl/dataformat/ByteArrayEncoding.java  

Observer Pattern.  
ClientProducerImpl is an observer iterator.   
Channel is the generic type for the listeners.  
largeMessageSendStreamed is the notify method.  
sendBlocking is the update method.  
Subject class(es): ClientProducerImpl  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/client/impl/ClientProducerImpl.java  

Observer Pattern.  
ClientProducerImpl is an observer iterator.  
Channel is the generic type for the listeners.  
largeMessageSendStreamed is the notify method.  
send is the update method.  
Subject class(es): ClientProducerImpl  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/client/impl/ClientProducerImpl.java  

Proxy Pattern.  
PersistedConnectionFactory is a proxy.  
EncodingSupport is a proxy interface.  
The real object(s): ConnectionFactoryConfiguration  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/jms/persistence/config/PersistedConnectionFactory.java  

Adapter Pattern.  
Adapting classes:  EmbeddedHornetQ Object  
EmbeddedJMS is an adapter class.  
JMSServerManagerImpl is the adaptee class.  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/jms/server/embedded/EmbeddedJMS.java  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/jms/server/impl/JMSServerManagerImpl.java  
 
Adapter Pattern.  
Adapting classes:  EmbeddedHornetQ Object  
EmbeddedJMS is an adapter class.  
BindingRegistry is the adaptee class.  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/jms/server/embedded/EmbeddedJMS.java  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/spi/core/naming/BindingRegistry.java  

Adapter Pattern.  
Adapting classes:  Object HornetQFileConfigurationServiceMBean  
HornetQFileConfigurationService is an adapter class.  
FileConfiguration is the adaptee class.  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/service/HornetQFileConfigurationService.java  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/config/impl/FileConfiguration.java  

Adapter Pattern.  
Adapting classes:  Object HornetQJMSStarterServiceMBean  
HornetQJMSStarterService is an adapter class.  
JMSServerManagerImpl is the adaptee class.  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/service/HornetQJMSStarterService.java  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/jms/server/impl/JMSServerManagerImpl.java   
 
Facade Pattern.  
ClientRequestor is a facade class.  
Hidden types: Message ClientProducer ClientConsumer ClientSession  
Facade access types: ClientRequestor  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/api/core/client/ClientRequestor.java  

Facade Pattern.  
ClientProducerImpl is a facade class.  
Hidden types: SimpleString TokenBucketLimiter ClientSessionInternal MessageInternal Message HornetQBuffer Channel ClientProducerCredits BodyEncoder HornetQBuffers SessionSendContinuationMessage PacketImpl DeflaterReader UUIDGenerator
Facade access types: ClientProducerImpl   
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/client/impl/ClientProducerImpl.java   

Facade Pattern.  
Reclaimer is a facade class.  
Hidden types: Logger JournalFile 
Facade access types: Reclaimer  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/journal/impl/Reclaimer.java  

Facade Pattern.  
EmbeddedHornetQ is a facade class.   
Hidden types: HornetQComponent FileConfiguration HornetQServerImpl  
Facade access types: EmbeddedJMS  
File Location: /host/hornetq_versions/backup/hornetq-2.2.14.Final-src/src/main/org/hornetq/core/server/embedded/EmbeddedHornetQ.java  


------------------------------------------

