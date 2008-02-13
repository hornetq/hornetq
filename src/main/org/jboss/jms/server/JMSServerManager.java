package org.jboss.jms.server;

import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.messaging.core.impl.server.SubscriptionInfo;

import java.util.List;

/**
 * A JMS Management interface.
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface JMSServerManager
{
   // management operations
   boolean isStarted();

   boolean createQueue(String queueName, String jndiBinding) throws Exception;

   boolean createTopic(String topicName, String jndiBinding) throws Exception;

   boolean destroyQueue(String name) throws Exception;

   boolean destroyTopic(String name) throws Exception;

   boolean createConnectionFactory(String name, String clientID, int dupsOKBatchSize, boolean strictTck, int prefetchSize, String jndiBinding) throws Exception;

   boolean createConnectionFactory(String name, String clientID, int dupsOKBatchSize, boolean strictTck, int prefetchSize, List<String> jndiBindings) throws Exception;

   boolean destroyConnectionFactory(String name) throws Exception;

   public void removeAllMessagesForQueue(String queueName) throws Exception;

   public void removeAllMessagesForTopic(String topicName) throws Exception;

   public void removeAllMessagesForQueue(JBossQueue queueName) throws Exception;

   public void removeAllMessagesForTopic(JBossTopic topicName) throws Exception;

   int getMessageCountForQueue(String queue) throws Exception;

   int getMessageCountForQueue(JBossQueue queue) throws Exception;

   List<SubscriptionInfo> listAllSubscriptionsForTopic(String topicName) throws Exception;

   List<SubscriptionInfo> listAllSubscriptionsForTopic(JBossTopic topicName) throws Exception;

   List<SubscriptionInfo> listDurableSubscriptionsForTopic(String topicName) throws Exception;

   List<SubscriptionInfo> listDurableSubscriptionsForTopic(JBossTopic topicName) throws Exception;

   List<SubscriptionInfo> listNonDurableSubscriptionsForTopic(String topicName) throws Exception;

   List<SubscriptionInfo> listNonDurableSubscriptionsForTopic(JBossTopic topicName) throws Exception;
}
