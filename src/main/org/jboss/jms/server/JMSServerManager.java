package org.jboss.jms.server;

import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.messaging.core.impl.server.SubscriptionInfo;
import org.jboss.jms.server.MessageStatistics;

import javax.jms.Message;
import java.util.List;

/**
 * A JMS Management interface.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface JMSServerManager
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

   boolean createConnectionFactory(String name, String clientID, int dupsOKBatchSize, boolean strictTck, int prefetchSize, String jndiBinding) throws Exception;

   boolean createConnectionFactory(String name, String clientID, int dupsOKBatchSize, boolean strictTck, int prefetchSize, List<String> jndiBindings) throws Exception;

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

   int getMessageCountForQueue(String queue) throws Exception;

   List<SubscriptionInfo> listSubscriptions(String topicName) throws Exception;

   List<SubscriptionInfo> listSubscriptions(String topicName, ListType listType) throws Exception;

   int getSubscriptionsCountForTopic(String topicName) throws Exception;

   int getSubscriptionsCountForTopic(String topicName, ListType listType) throws Exception;

   int getConsumerCountForQueue(String queue) throws Exception;

   List<ClientInfo> getClients() throws Exception;

   void startGatheringStatistics();

   void startGatheringStatisticsForQueue(String queue);

   void startGatheringStatisticsForTopic(String topic);

   void stopGatheringStatistics();

   void stopGatheringStatisticsForQueue(String queue);

   void stopGatheringStatisticsForTopic(String topic);

   List<MessageStatistics> getStatistics() throws Exception;
}
