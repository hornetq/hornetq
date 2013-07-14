#The Operational View

In their [book](http://www.viewpoints-and-perspectives.info/), Rozanski and Woods define the Operational View as "describes how the system will be operated, administered and supported when it is running in its production environment".

In order to apply the Operational View on HornetQ, we will focus on the Management modules that are used to control the server from the exterior

Based on the [documentation](http://docs.jboss.org/hornetq/2.2.2.Final/user-manual/en/html/management.html), We can manage HornetQ in two ways - Using Management APIs exposed by the HornetQ and Management using JMX

## Management APIs

HornetQ exposes it managed resources in two way:

### Core Management APIs 

HornetQ defines a core management API to manage core resources (located in package `org.hornetq.api.core.management`). Core resources are listed as follows.

#### Core Server Management  
 * _**Responsible for listing, creating, deploying and destroying queues**_ : Methods available are `getQueueNames()` , `createQueue()` , `deployQueue()` and `destroyQueue()` respectively, on `HornetQServerControl` object.
 
 * _**Pausing and Resuming Queues**_ : `QueueControl` object can be used to pause or resume a queue.
 
 * _**Listing and closing remote Queues**_ : `listRemoteAddresses()` can be used to list the remote client's address. For closing connections `closeConnectionsForAddress()`.
 
 * _**Server crash recovery and other post-failure transaction management**_ : In case of server crash and restart, some transactions in HornetQ requires manual interventions. HornetQ exposes various methods like _ listPreparedTransactions()_ to obtain the list of transactions. For rollback or commit, it provides `commitPreparedTransaction()` or `rollbackPreparedTransaction()`. For obtaining list of automatically committed list of transactions, it provides method `listHeuristicCommittedTransactions()` and `listHeuristicRolledBackTransactions()`.
 
 * _**Enabling and resetting Message Counters**_ : `enableMessageCounters()` and `disableMessageCounters()` can be used to enable or disable message counters. To reset `resetAllMessageCounters()` and `resetAllMessageCounterHistories()` methods can be used based on the use case.
 
 * _**Managing server configurations and attributes**_ : `HornetQServerControl` exposes server configurations and attributes.
  
 * _**Listing, creating and destroying Core bridges**_ : `HornetQServerControl` object exposes operations `getBridgeNames()`, `createBridge()` and `destroyBridge()` respectively.
 
 * _**Start/stop servers, enforce failovers**_ : We can enforce failover using object `HornetQServerControl` and methods `forceFailover()`.


#### Core Address Management

Core addresses can be managed using the `AddressControl` for modifying roles and permissions for an address using `addRole()`, `removeRole()` and `getRoles()`.

#### Core Queue Management

`QueueControl` defines core queue management operations. Following are the all the possible operation.

* _**Expiring, sending to [dead letter address]( http://msdn.microsoft.com/en-us/library/windows/desktop/ms706227.aspx ) and moving messages**_ : `expireMessages()`, `setExpiryAddress()` , `sendMessagesToDeadLetterAddress()`, `moveMessages()` and `setDeadLetterAddress()` are possible operations

* _**Listing, removing, counting messages and changing message priority**_ : Possible operations are `listMessages()`, `removeMessages()` , `countMessages()`, `changeMessagesPriority()` respectively.

#### Other Core Resource Management 

* **Acceptors** : `AcceptorControl` exposes `start()` and `stop()` operation. More detailed attributes are listed [here](http://docs.jboss.org/hornetq/2.2.2.Final/user-manual/en/html/configuring-transports.html#configuring-transports.acceptors). Possible attributes are shown below.

``` xml
<acceptors>                
    <acceptor name="netty">
        <factory-class>
org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory
        </factory-class>
        <param key="port" value="5446"/>
    </acceptor>
</acceptors> 

```

* **Diverts** : `DivertControl` exposes `start()` and `stop()` operation. More details about the attributes can be found [here](http://docs.jboss.org/hornetq/2.2.2.Final/user-manual/en/html/diverts.html). Possible attributes are shown here.

``` xml
<divert name="prices-divert">                  
    <address>jms.topic.priceUpdates</address>
    <forwarding-address>jms.queue.priceForwarding</forwarding-address>    
    <filter string="office='New York'"/>
    <transformer-class-name>
        org.hornetq.jms.example.AddForwardingTimeTransformer
    </transformer-class-name>     
    <exclusive>true</exclusive>
</divert> 

``` 

* **Bridges** : `BridgeControl` exposes `start()` and `stop()` operation. More details can be found [here](http://docs.jboss.org/hornetq/2.2.2.Final/user-manual/en/html/core-bridges.html). The attributes of the BridgeControl is shown below and also can be found in `hornetq-configuration.xml`.

```xml

<bridge name="my-bridge">
    <queue-name>jms.queue.sausage-factory</queue-name>
    <forwarding-address>jms.queue.mincing-machine</forwarding-address>
    <filter-string="name='aardvark'"/>
    <transformer-class-name>
        org.hornetq.jms.example.HatColourChangeTransformer
    </transformer-class-name>
    <retry-interval>1000</retry-interval>
    <ha>true</ha>
    <retry-interval-multiplier>1.0</retry-interval-multiplier>
    <reconnect-attempts>-1</reconnect-attempts>
    <failover-on-server-shutdown>false</failover-on-server-shutdown>
    <use-duplicate-detection>true</use-duplicate-detection>
    <confirmation-window-size>10000000</confirmation-window-size>
    <connector-ref connector-name="remote-connector" 
        backup-connector-name="backup-remote-connector"/>     
    <user>foouser</user>
    <password>foopassword</password>
</bridge>

```
* **Broadcast groups** : `BroadcastGroupControl` exposes `start()` and `stop()` operation. More details can be found [here](http://docs.jboss.org/hornetq/2.2.2.Final/user-manual/en/html/clusters.html). Belows shows all possible attributes for `BroadcastGroupControl`.

```xml
 <broadcast-groups>
         <broadcast-group name="bg-group1">
            <group-address>231.7.7.7</group-address>
            <group-port>9876</group-port>
            <broadcast-period>1000</broadcast-period>
            <connector-ref>netty-connector</connector-ref>
         </broadcast-group>
      </broadcast-groups>
```
* **Discovery Group** : `DiscoveryGroupControl` exposes `start()` and `stop()` operation. More details can be found [here](http://docs.jboss.org/hornetq/2.2.2.Final/user-manual/en/html/clusters.html). Below shows all possible attributes for `DiscoveryGroupControl`.

```xml
 <discovery-groups>
         <discovery-group name="dg-group1">
            <group-address>231.7.7.7</group-address>
            <group-port>9876</group-port>
            <refresh-timeout>60000</refresh-timeout>
            </discovery-group>
      </discovery-groups>

```
* **Cluster Connection** : `ClusterConnectionControl` exposes `start()` and `stop()` operation. More details can be found [here](http://docs.jboss.org/hornetq/2.2.2.Final/user-manual/en/html/clusters.html). Following are then attributes of `ClusterConnectionControl`.

```xml
<cluster-connections>
         <cluster-connection name="my-cluster">
            <address>jms</address>
            <connector-ref>netty-connector</connector-ref>
            <discovery-group-ref discovery-group-name="dg-group1"/>
         </cluster-connection>
      </cluster-connections>
```

![operationview](https://f.cloud.github.com/assets/950121/687643/9a21232c-da81-11e2-8c0c-96e5986b93ed.png)



### JMS Management APIs 

HornetQ provides JMS management APIs for managing JMS objects (located in package `org.hornetq.api.jms.management`)

#### JMS server management 

JMS resources can be created using `JMSServerControl`. Similar operations like core management like lisitng, creating and destroying connection, queues, topics, remote connections etc.
 
#### JMS ConnectionFactory Management 

JMS Connection Factories can be managed using the `ConnectionFactoryControl`. Exposes native JMS ConnectionFactory configuration operations eg. `getConsumerWindowSize()`.

#### JMS Queue Management 

JMS queues can be managed using the `JMSQueueControl`. The management operations on a JMS queue are very similar to the operations on a core queue. Eg. Expiring, sending to dead letter and moving messages etc.

#### JMS Topic Management 

JMS Topics can be managed using the `TopicControl`. Possible operations include - Listing subscriptions and messages ( `listAllSubscriptions()` , `listDurableSubscriptions()`, `listNonDurableSubscriptions()`), Dropping subscription (`dropDurableSubscription()`), Counting subscriptions (`countMessagesForSubscription()`) 

## Management via JMX

HornetQ can alternatively be managed using [JMX](http://www.oracle.com/technetwork/java/javase/tech/javamanagement-140525.html). The management API is exposed by HornetQ using MBeans interfaces.

Sample configuration for managing JMS Queue is exampleQueue is given below.

```java
  
  org.hornetq:module=JMS,type=Queue,name="exampleQueue"
  
``` 

More examples can be found [here](http://docs.jboss.org/hornetq/2.2.2.Final/user-manual/en/html/examples.html#examples.jmx)

