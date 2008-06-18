/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.jms.server;

import java.io.Serializable;
import java.util.List;


/**
 * The JMS Management interface.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface JMSServerManager extends Serializable
{
   public enum ListType
   {
      ALL, DURABLE, NON_DURABLE
   }

   /**
    * Has the Server been started.
    * @return true if the server us running
    */
   boolean isStarted();

   /**
    * Creates a JMS Queue.
    * @param queueName The name of the queue to create
    * @param jndiBinding the name of the binding for JNDI
    * @return true if the queue is created or if it existed and was added to JNDI
    * @throws Exception if problems were encountered creating the queue.
    */
   boolean createQueue(String queueName, String jndiBinding) throws Exception;

   /**
    * Creates a JMS Topic
    * @param topicName the name of the topic
    * @param jndiBinding the name of the binding for JNDI
    * @return true if the topic was created or if it existed and was added to JNDI
    * @throws Exception if a problem occurred creating the topic
    */
   boolean createTopic(String topicName, String jndiBinding) throws Exception;

   /**
    * destroys a queue and removes it from JNDI
    * @param name the name of the queue to destroy
    * @return true if destroyed
    * @throws Exception if a problem occurred destroying the queue
    */
   boolean destroyQueue(String name) throws Exception;

   /**
    * destroys a topic and removes it from JNDI
    * @param name the name of the topic to destroy
    * @return true if the topic was destroyed
    * @throws Exception if a problem occurred destroying the topic
    */
   boolean destroyTopic(String name) throws Exception;

//   /**
//    * returns a list of all the JMS queues
//    * @return all queues
//    */
//   Set<String> listAllQueues();
//
//   /**
//    * returns a list of all the JMS topics
//    * @return all topics
//    */
//   Set<String> listAllTopics();
//
//   /**
//    * returns all the temporary destinations
//    * @return all temporary destinations
//    */
//   Set<String> listTemporaryDestinations();

   /**
    * Creates a connection factory
    * @param name the name of this connection factory
    * @param clientID the client id
    * @param dupsOKBatchSize the bath size
    * @param consumerWindowSize The consumer window size
    * @param consumerMaxRate the Consumer max rate
    * @param producerWindowSize the producer window size
    * @param producerMaxRate the producer max rate
    * @param jndiBinding the binding name for JNDI
    * @return true if the connection factory was created
    * @throws Exception if a problem occurred creating the connection factory
    */
   boolean createConnectionFactory(String name, String clientID,
   		                          int dupsOKBatchSize,
   		                          int consumerWindowSize, int consumerMaxRate,
   		                          int producerWindowSize, int producerMaxRate,
   		                          boolean blockOnAcknowledge,
   		                          boolean defaultSendNonPersistentMessagesBlocking,
   		                          boolean defaultSendPersistentMessagesBlocking,
   		                          String jndiBinding) throws Exception;

   /**
    * Creates a connection factory
    * @param name the name of this connection factory
    * @param clientID the client id
    * @param dupsOKBatchSize the bath size
    * @param consumerWindowSize The consumer window size
    * @param consumerMaxRate the Consumer max rate
    * @param producerWindowSize the producer window size
    * @param producerMaxRate the producer max rate
    * @param jndiBindings the binding names for JNDI
    * @return true if the connection factory was created
    * @throws Exception if a problem occurred creating the connection factory
    */
   boolean createConnectionFactory(String name, String clientID, int dupsOKBatchSize,
   		                          int consumerWindowSize, int consumerMaxRate,
   		                          int producerWindowSize, int producerMaxRate,
   		                          boolean blockOnAcknowledge,
   		                          boolean defaultSendNonPersistentMessagesBlocking,
   		                          boolean defaultSendPersistentMessagesBlocking,
   		                          List<String> jndiBindings) throws Exception;

   /**
    * destroys a connection factory.
    * @param name the name of the connection factory to destroy
    * @return true if the connection factory was destroyed
    * @throws Exception if a problem occurred destroying the connection factory
    */
   boolean destroyConnectionFactory(String name) throws Exception;

//   /**
//    * list all messages for a specific queue
//    * @param queue the queue to inspect
//    * @return all messages
//    * @throws Exception if a problem occurred
//    */
//   public List<Message> listMessagesForQueue(String queue) throws Exception;
//
//   /**
//    * list the messages on a specific queue dependant on the ListType.
//    * ListType.ALL returns all messages
//    * ListType.DURABLE returns all durable messages
//    * ListType.NON_DURABLE returns all non durable messages
//    * @param queue the queue to inspect
//    * @param listType the list type.
//    * @return the messages
//    * @throws Exception if a problem occurred
//    */
//   public List<Message> listMessagesForQueue(String queue, ListType listType) throws Exception;
//
//   /**
//    * list all messages for a specific subscription
//    * @param subscription the subscription to inspect
//    * @return all messages
//    * @throws Exception if a problem occurred
//    */
//   public List<Message> listMessagesForSubscription(String subscription) throws Exception;
//
//   /**
//    * list the messages on a specific subscription dependant on the ListType.
//    * ListType.ALL returns all messages
//    * ListType.DURABLE returns all durable messages
//    * ListType.NON_DURABLE returns all non durable messages
//    * @param subscription the subscription to inspect
//    * @param listType the list type
//    * @return the messages
//    * @throws Exception if a problem occurred
//    */
//   public List<Message> listMessagesForSubscription(String subscription, ListType listType) throws Exception;
//
//   /**
//    * removes a particular message from a queue
//    * @param queue the name of the queue
//    * @param messageId the id of the message to remove
//    * @throws Exception if a problem occurred
//    */
// //  void removeMessageFromQueue(String queue, String messageId) throws Exception;
//
//   /**
//    * removes a particular message from a topic
//    * @param topic the name of the topic
//    * @param messageId the id of the message
//    * @throws Exception if a problem occurred
//    */
// //  void removeMessageFromTopic(String topic, String messageId) throws Exception;

   /**
    * removes all messages from a particular queue
    * @param queue the name of the queue
    * @throws Exception if a problem occurred
    */
   void removeAllMessagesForQueue(String queue) throws Exception;

   /**
    * removes all the messages from a topic
    * @param topic the name of the topic
    * @throws Exception if a problem occurred
    */
   void removeAllMessagesForTopic(String topic) throws Exception;
//
//   /**
//    * moves a message from one queue to another
//    * @param fromQueue the name of the queue to find the message
//    * @param toQueue the name of the queue to move the message to
//    * @param messageID the id of the message
//    * @throws Exception if a problem occurred
//    */
//   //void moveMessage(String fromQueue, String toQueue, String messageID) throws Exception;
//
//   /**
//    * expires a message
//    * @param queue the name of the queue
//    * @param messageId the message id
//    * @throws Exception if a problem occurred
//    */
//   void expireMessage(String queue, String messageId) throws Exception;
//
//   /**
//    * changes the priority of a message.
//    * @param queue the name of the queue
//    * @param messageId the id of the message
//    * @param priority the priority to change the message to
//    * @throws Exception if a problem occurred
//    */
// //  void changeMessagePriority(String queue, String messageId, int priority) throws Exception;
//
   /**
    * returns how many messages a queue is currently holding
    * @param queue the name of the queue
    * @return the number of messages
    * @throws Exception if a problem occurred
    */
   int getMessageCountForQueue(String queue) throws Exception;

   /**
    * lists all the subscriptions for a specific topic
    * @param topic the name of the topic
    * @return the subscriptions
    * @throws Exception if a problem occurred
    */
   List<SubscriptionInfo> listSubscriptions(String topic) throws Exception;
//
//   /**
//    * lists all the subscriptions for a specific topic for a specific ListType.
//    * ListType.ALL returns all subscriptions
//    * ListType.DURABLE returns all durable subscriptions
//    * ListType.NON_DURABLE returns all non durable subscriptions
//    *
//    * @param topicName the name of the topic
//    * @param listType the list type
//    * @return the subscriptions
//    * @throws Exception if a problem occurred
//    */
//   List<SubscriptionInfo> listSubscriptions(String topicName, ListType listType) throws Exception;
//
//   /**
//    * count the subscriptions a topic currently has
//    * @param topic the name of the topic
//    * @return the number of subscriptions
//    * @throws Exception if a problem occurred
//    */
//   int getSubscriptionsCountForTopic(String topic) throws Exception;
//
//  /**
//    * count the subscriptions a topic currently has of a specific type.
//    * ListType.ALL returns all subscriptions
//    * ListType.DURABLE returns all durable subscriptions
//    * ListType.NON_DURABLE returns all non durable subscriptions
//   *
//    * @param topic the name of the topic
//    * @param listType the list type
//    * @return the number of subscriptions
//    * @throws Exception if a problem occurred
//    */
//   int getSubscriptionsCountForTopic(String topic, ListType listType) throws Exception;
//
//   /**
//    * drops a particular subscription
//    *
//    * @param subscription the id of the subscription
//    * @throws Exception if a problem occurred
//    */
//   void dropSubscription(String subscription) throws Exception;
//
//   /**
//    * count the consumers for a specific queue
//    * @param queue the name of the queue
//    * @return the number of consumers
//    * @throws Exception if a problem occurred
//    */
//   int getConsumerCountForQueue(String queue) throws Exception;
//
//   /**
//    * returns info on all the current active connections
//    * @return the connections info
//    * @throws Exception if a problem occurred
//    */
//   List<ConnectionInfo> getConnections() throws Exception;
//
//   /**
//    * return the connections info for a particular user.
//    * @param user the user
//    * @return the connections info
//    * @throws Exception if a problem occurred
//    */
//   List<ConnectionInfo> getConnectionsForUser(String user) throws Exception;
//
//   /**
//    * drops the connection with the specified client id
//    * @param clientId the client id
//    * @throws Exception if a problem occurred
//    */
//   void dropConnection(long id) throws Exception;
//
//   /**
//    * drop all the connections for a specific user
//    * @param user the user
//    * @throws Exception if a problem occurred
//    */
//   void dropConnectionsForUser(String user) throws Exception;
//
//   /**
//    * list all the sessions info
//    * @return the session info
//    * @throws Exception if a problem occurred
//    */
//   //public List<SessionInfo> getSessions() throws Exception;
//
//   /**
//    * get the session info for a particular connection with the specified client id
//    * @param clientid the client id
//    * @return the session info
//    * @throws Exception if a problem occurred
//    */
//  // public List<SessionInfo> getSessionsForConnection(long id) throws Exception;
//
//   /**
//    * get the session info for a particular user
//    * @param user the user
//    * @return the session info
//    * @throws Exception if a problem occurred
//    */
//  // public List<SessionInfo> getSessionsForUser(String user) throws Exception;
//
//   /**
//    * Start gathering delivery statistics for all queues
//    * @throws Exception if a problem occurred
//    */
//   void startGatheringStatistics() throws Exception;
//
//   /**
//    * Start gathering delivery statistics for a specified queue
//    * @param queue the name of the queue
//    * @throws Exception if a problem occurred
//    */
//   void startGatheringStatisticsForQueue(String queue) throws Exception;
//
//   /**
//    * stop gathering delivery statistics for all queues
//    * @return the delivery statistics at the time of stopping gathering
//    * @throws Exception if a problem occurred
//    */
//   List<MessageStatistics> stopGatheringStatistics() throws Exception;
//
//   /**
//    * stop gathering statistics for a specified queue
//    * @param queue the name of the queue
//    * @return the delivery statistics for that queue at the time of stopping gathering
//    * @throws Exception if a problem occurred
//    */
//   MessageStatistics stopGatheringStatisticsForQueue(String queue) throws Exception;
//
//   /**
//    * list all message delivery statistics. This will include statistics up to the point this method is called.
//    * The gathering of statistics will carry on.
//    * @return the delivery statistics
//    * @throws Exception if a problem occurred
//    */
//   List<MessageStatistics> getStatistics() throws Exception;
}
