package org.jboss.jms.server;

import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.messaging.core.impl.server.SubscriptionInfo;
import org.jboss.messaging.core.impl.messagecounter.MessageStatistics;

import javax.jms.Message;
import java.util.List;

/**
 * A JMS Management interface.
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface JMSServerManager
{
   // management operations
   enum ListType
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

   public List<Message> listMessagesForQueue(String queue);

   public List<Message> listMessagesForQueue(String queue, ListType listType);

   public List<Message> listMessages(JBossQueue queue);

   public List<Message> listMessagesForTopic(String topic);

   public List<Message> listMessagesForTopic(String topic, ListType listType);

   public List<Message> listMessages(JBossTopic topic);

   public List<Message> listMessages(JBossQueue queue, ListType listType);

   public List<Message> listMessages(JBossTopic topic, ListType listType);

   void removeAllMessagesForQueue(String queueName) throws Exception;

   void removeAllMessagesForTopic(String topicName) throws Exception;

   void removeAllMessages(JBossQueue queueName) throws Exception;

   void removeAllMessages(JBossTopic topicName) throws Exception;

   int getMessageCountForQueue(String queue) throws Exception;

   int getMessageCount(JBossQueue queue) throws Exception;

   int getMessageCountForTopic(String topic) throws Exception;

   int getMessageCount(JBossTopic topic) throws Exception;

   List<SubscriptionInfo> listSubscriptions(String topicName) throws Exception;

   List<SubscriptionInfo> listSubscriptions(JBossTopic topic) throws Exception;

   List<SubscriptionInfo> listSubscriptions(String topicName, ListType listType) throws Exception;

   List<SubscriptionInfo> listSubscriptions(JBossTopic topic, ListType listType) throws Exception;

   int getSubscriptionsCountForTopic(String topicName) throws Exception;

   int getSubscriptionsCount(JBossTopic topic) throws Exception;

   int getSubscriptionsCountForTopic(String topicName, ListType listType) throws Exception;

   int getSubscriptionsCount(JBossTopic topic,ListType listType) throws Exception;

   int getConsumerCountForQueue(String queue) throws Exception;

   int getConsumerCountForQueue(JBossQueue queue) throws Exception;

   List getClients() throws Exception;

   void startGatheringStatistics();

   void startGatheringStatisticsForQueue(String queue);

   void startGatheringStatistics(JBossQueue queue);

   void startGatheringStatisticsForTopic(String topic);

   void startGatheringStatistics(JBossTopic topic);

   void stopGatheringStatistics();

   void stopGatheringStatisticsForQueue(String queue);

   void stopGatheringStatistics(JBossQueue queue);

   void stopGatheringStatisticsForTopic(String topic);

   void stopGatheringStatistics(JBossTopic topic);

   List<MessageStatistics> getStatistics() throws Exception;
}
