package org.jboss.messaging.jms.server;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import javax.jms.Message;

/**
 * The JMS Management interface.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface JMSServerManager extends Serializable
{
   // management operations
   public enum ListType
   {
      ALL, DURABLE, NON_DURABLE
   }

   boolean isStarted();

   boolean createQueue(String queueName, String jndiBinding) throws Exception;

   boolean createTopic(String topicName, String jndiBinding) throws Exception;

   boolean destroyQueue(String name) throws Exception;

   boolean destroyTopic(String name) throws Exception;

   Set<String> listAllQueues();

   Set<String> listAllTopics();

   Set<String> listTemporaryDestinations();

   boolean createConnectionFactory(String name, String clientID,
   		                          int dupsOKBatchSize, boolean strictTck, int prefetchSize,
   		                          int producerWindowSize, int producerMaxRate,
   		                          String jndiBinding) throws Exception;

   boolean createConnectionFactory(String name, String clientID, int dupsOKBatchSize,
   		                          boolean strictTck, int prefetchSize,
   		                          int producerWindowSize, int producerMaxRate,
   		                          List<String> jndiBindings) throws Exception;

   boolean destroyConnectionFactory(String name) throws Exception;

   public List<Message> listMessagesForQueue(String queue) throws Exception;

   public List<Message> listMessagesForQueue(String queue, ListType listType) throws Exception;

   public List<Message> listMessagesForSubscription(String subscription) throws Exception;

   public List<Message> listMessagesForSubscription(String subscription, ListType listType) throws Exception;

   void removeMessageFromQueue(String queueName, String messageId) throws Exception;

   void removeMessageFromTopic(String topicName, String messageId) throws Exception;

   void removeAllMessagesForQueue(String queueName) throws Exception;

   void removeAllMessagesForTopic(String topicName) throws Exception;

   void moveMessage(String fromQueue, String toQueue, String messageID) throws Exception;

   void expireMessage(String queue, String messageId) throws Exception;

   void changeMessagePriority(String queue, String messageId, int priority) throws Exception;

   void changeMessageHeader(String queue,String messageId, String header, Object value) throws Exception;

   int getMessageCountForQueue(String queue) throws Exception;

   List<SubscriptionInfo> listSubscriptions(String topicName) throws Exception;

   List<SubscriptionInfo> listSubscriptions(String topicName, ListType listType) throws Exception;

   int getSubscriptionsCountForTopic(String topicName) throws Exception;

   int getSubscriptionsCountForTopic(String topicName, ListType listType) throws Exception;

   void dropSubscription(String subscription) throws Exception;

   int getConsumerCountForQueue(String queue) throws Exception;

   List<ConnectionInfo> getConnections() throws Exception;

   List<ConnectionInfo> getConnectionsForUser(String user) throws Exception;

   void dropConnection(String clientId) throws Exception;

   void dropConnectionForUser(String user) throws Exception;

   public List<SessionInfo> getSessions() throws Exception;

   public List<SessionInfo> getSessionsForConnection(String id) throws Exception;

   public List<SessionInfo> getSessionsForUser(String user) throws Exception;

   void startGatheringStatistics() throws Exception;

   void startGatheringStatisticsForQueue(String queue) throws Exception;

   List<MessageStatistics> stopGatheringStatistics() throws Exception;

   MessageStatistics stopGatheringStatisticsForQueue(String queue) throws Exception;

   List<MessageStatistics> getStatistics() throws Exception;
}
