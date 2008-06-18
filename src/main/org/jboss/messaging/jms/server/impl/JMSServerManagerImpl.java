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

package org.jboss.messaging.jms.server.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.SubscriptionInfo;
import org.jboss.messaging.util.JNDIUtil;
import org.jboss.messaging.util.Pair;

/**
 * A Deployer used to create and add to JNDI queues, topics and connection factories. Typically this would only be used
 * in an app server env.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JMSServerManagerImpl implements JMSServerManager
{
   private static final Logger log = Logger.getLogger(JMSServerManagerImpl.class);

   /**
    * the initial context to bind to
    */
   private InitialContext initialContext;

   private final Map<String, List<String>> destinations = new HashMap<String, List<String>>();

   private final Map<String, JBossConnectionFactory> connectionFactories = new HashMap<String, JBossConnectionFactory>();

   private final Map<String, List<String>> connectionFactoryBindings = new HashMap<String, List<String>>();

   private MessagingServerManagement messagingServerManagement;

   public void setMessagingServerManagement(final MessagingServerManagement messagingServerManagement)
   {
      this.messagingServerManagement = messagingServerManagement;
   }

   /**
    * lifecycle method
    *
    * @throws Exception ex
    */
   public void start() throws Exception
   {
      try
      {
         initialContext = new InitialContext();          
      }
      catch (NamingException e)
      {
         log.error("Unable to create Initial Context", e);
      }
   }


   private boolean bindToJndi(final String jndiName, final Object objectToBind) throws NamingException
   {
      String parentContext;
      String jndiNameInContext;
      int sepIndex = jndiName.lastIndexOf('/');
      if (sepIndex == -1)
      {
         parentContext = "";
      }
      else
      {
         parentContext = jndiName.substring(0, sepIndex);
      }
      jndiNameInContext = jndiName.substring(sepIndex + 1);
      try
      {
         initialContext.lookup(jndiName);

         log.warn("Binding for " + jndiName + " already exists");
         return false;
      }
      catch (Throwable e)
      {
         // OK
      }

      Context c = JNDIUtil.createContext(initialContext, parentContext);

      c.rebind(jndiNameInContext, objectToBind);
      return true;
   }

   // management operations

   public boolean isStarted()
   {
      return messagingServerManagement.isStarted();
   }

   public boolean createQueue(String queueName, String jndiBinding) throws Exception
   {
      JBossQueue jBossQueue = new JBossQueue(queueName);
      messagingServerManagement.addDestination(jBossQueue.getSimpleAddress());
      messagingServerManagement.createQueue(jBossQueue.getSimpleAddress(), jBossQueue.getSimpleAddress());
      boolean added = bindToJndi(jndiBinding, jBossQueue);
      if (added)
      {
         addToDestinationBindings(queueName, jndiBinding);
      }
      return added;
   }

   public boolean createTopic(String topicName, String jndiBinding) throws Exception
   {
      JBossTopic jBossTopic = new JBossTopic(topicName);
      messagingServerManagement.addDestination(jBossTopic.getSimpleAddress());
      boolean added = bindToJndi(jndiBinding, jBossTopic);
      if (added)
      {
         addToDestinationBindings(topicName, jndiBinding);
      }
      return added;
   }

   public boolean destroyQueue(String name) throws Exception
   {
 //  	JBossQueue jBossQueue = new JBossQueue(name);
//      messagingServerManagement.destroyQueue(new SimpleString(name));
//      messagingServerManagement.removeDestination(jBossQueue.getSimpleAddress());
      List<String> jndiBindings = destinations.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      for (String jndiBinding : jndiBindings)
      {
         initialContext.unbind(jndiBinding);
      }
      destinations.remove(name);
      return true;
   }

   public boolean destroyTopic(String name) throws Exception
   {
   //   JBossTopic jBossTopic = new JBossTopic(name);
    //  messagingServerManagement.removeDestination(jBossTopic.getSimpleAddress());
      List<String> jndiBindings = destinations.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      for (String jndiBinding : jndiBindings)
      {
         initialContext.unbind(jndiBinding);
      }
      destinations.remove(name);
      return true;
   }

//   public Set<String> listAllQueues()
//   {
//      Set<String> availableAddresses = messagingServerManagement.listAvailableAddresses();
//      Set<String> availableQueues = new HashSet<String>();
//      for (String address : availableAddresses)
//      {
//         if (address.startsWith(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX))
//         {
//            availableQueues.add(address.replace(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX, ""));
//         }
//      }
//      return availableQueues;
//   }
//
//   public Set<String> listAllTopics()
//   {
//      Set<String> availableAddresses = messagingServerManagement.listAvailableAddresses();
//      Set<String> availableTopics = new HashSet<String>();
//      for (String address : availableAddresses)
//      {
//         if (address.startsWith(JBossTopic.JMS_TOPIC_ADDRESS_PREFIX))
//         {
//            availableTopics.add(address.replace(JBossTopic.JMS_TOPIC_ADDRESS_PREFIX, ""));
//         }
//      }
//      return availableTopics;
//   }
//
//   public Set<String> listTemporaryDestinations()
//   {
//      Set<String> availableAddresses = messagingServerManagement.listAvailableAddresses();
//      Set<String> tempDests = new HashSet<String>();
//      for (String address : availableAddresses)
//      {
//         if (address.startsWith(JBossTemporaryTopic.JMS_TOPIC_ADDRESS_PREFIX) || address.startsWith(JBossTemporaryQueue.JMS_QUEUE_ADDRESS_PREFIX))
//         {
//            tempDests.add(address.replace(JBossTopic.JMS_TOPIC_ADDRESS_PREFIX, ""));
//         }
//      }
//      return tempDests;
//   }

   public boolean createConnectionFactory(String name, String clientID,
   		int dupsOKBatchSize, int consumerWindowSize, int consumerMaxRate,
   		int producerWindowSize, int producerMaxRate, boolean blockOnAcknowledge,
   		boolean defaultSendNonPersistentMessagesBlocking, boolean defaultSendPersistentMessagesBlocking,
   		String jndiBinding) throws Exception
   {
      JBossConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         cf = new JBossConnectionFactory( clientID, dupsOKBatchSize,
                 messagingServerManagement.getConfiguration().getLocation(),
                 messagingServerManagement.getConfiguration().getConnectionParams(),                 
                 consumerWindowSize, consumerMaxRate, producerWindowSize, producerMaxRate,
                 blockOnAcknowledge,
                 defaultSendNonPersistentMessagesBlocking,
                 defaultSendPersistentMessagesBlocking
                 );
      }
      if (!bindToJndi(jndiBinding, cf))
      {
         return false;
      }
      if (connectionFactoryBindings.get(name) == null)
      {
         connectionFactoryBindings.put(name, new ArrayList<String>());
      }
      connectionFactoryBindings.get(name).add(jndiBinding);
      return true;
   }


   public boolean createConnectionFactory(String name, String clientID, int dupsOKBatchSize,
   		                                 int consumerWindowSize, int consumerMaxRate,
   		                                 int producerWindowSize, int producerMaxRate, boolean blockOnAcknowledge,
   		                                 boolean defaultSendNonPersistentMessagesBlocking,
   		                                 boolean defaultSendPersistentMessagesBlocking,
   		                                 List<String> jndiBindings) throws Exception
   {
      JBossConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         cf = new JBossConnectionFactory( clientID, dupsOKBatchSize,
                 messagingServerManagement.getConfiguration().getLocation(),
                 messagingServerManagement.getConfiguration().getConnectionParams(),
                 consumerWindowSize, consumerMaxRate, producerWindowSize, producerMaxRate,
                 blockOnAcknowledge, defaultSendNonPersistentMessagesBlocking,
                 defaultSendPersistentMessagesBlocking
                 );
      }
      for (String jndiBinding : jndiBindings)
      {
         bindToJndi(jndiBinding, cf);
         if (connectionFactoryBindings.get(name) == null)
         {
            connectionFactoryBindings.put(name, new ArrayList<String>());
         }
         connectionFactoryBindings.get(name).add(jndiBinding);
      }
      return true;
   }

   public boolean destroyConnectionFactory(String name) throws Exception
   {
      List<String> jndiBindings = connectionFactoryBindings.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      for (String jndiBinding : jndiBindings)
      {
         initialContext.unbind(jndiBinding);
      }
      connectionFactoryBindings.remove(name);
      connectionFactories.remove(name);
      return true;
   }


//   public List<Message> listMessagesForQueue(String queue) throws Exception
//   {
//      return listMessagesForQueue(queue, ListType.ALL);
//   }
//
//   public List<Message> listMessagesForQueue(String queue, ListType listType) throws Exception
//   {
//      return listMessages(new JBossQueue(queue).getAddress(), listType);
//   }
//
//   public List<Message> listMessagesForSubscription(String subscription) throws Exception
//   {
//      return listMessagesForSubscription(subscription, ListType.ALL);
//   }
//
//   public List<Message> listMessagesForSubscription(String subscription, ListType listType) throws Exception
//   {
//      return listMessages(subscription, listType);
//   }
//
////   public void removeMessageFromQueue(String queueName, String messageId) throws Exception
////   {
////      messagingServerManagement.removeMessageForBinding(new JBossQueue(queueName).getAddress(), new FilterImpl("JMSMessageID='" + messageId + "'"));
////   }
////
////   public void removeMessageFromTopic(String topicName, String messageId) throws Exception
////   {
////      messagingServerManagement.removeMessageForAddress(new JBossTopic(topicName).getAddress(), new FilterImpl("JMSMessageID='" + messageId + "'"));
////   }

   public void removeAllMessagesForQueue(String queueName) throws Exception
   {
      JBossQueue jBossQueue = new JBossQueue(queueName);
      removeAllMessages(jBossQueue);
   }

   public void removeAllMessagesForTopic(String topicName) throws Exception
   {
      JBossTopic jBossTopic = new JBossTopic(topicName);
      removeAllMessages(jBossTopic);
   }
//
////   public void moveMessage(String fromQueue, String toQueue, String messageId) throws Exception
////   {
////      messagingServerManagement.moveMessages(new JBossQueue(fromQueue).getAddress(), new JBossQueue(toQueue).getAddress(),
////              "JMSMessageID='" + messageId + "'");
////   }
//
//   public void expireMessage(String queue, String messageId) throws Exception
//   {
//      messagingServerManagement.expireMessages(new JBossQueue(queue).getAddress(),
//              "JMSMessageID='" + messageId + "'");
//   }
//
////   public void changeMessagePriority(String queue, String messageId, int priority) throws Exception
////   {
////      messagingServerManagement.changeMessagePriority(new JBossQueue(queue).getAddress(),
////              "JMSMessageID='" + messageId + "'", priority);
////   }
//
   public int getMessageCountForQueue(String queue) throws Exception
   {
      return getMessageCount(new JBossQueue(queue));
   }

   public List<SubscriptionInfo> listSubscriptions(String topicName) throws Exception
   {
      return listSubscriptions(new JBossTopic(topicName));
   }
//
//   public List<SubscriptionInfo> listSubscriptions(String topic, ListType type) throws Exception
//   {
//      return listSubscriptions(new JBossTopic(topic), type);
//   }
//
//   public int getSubscriptionsCountForTopic(String topicName) throws Exception
//   {
//      return getSubscriptionsCount(new JBossTopic(topicName));
//   }
//
//   public int getSubscriptionsCountForTopic(String topicName, ListType listType) throws Exception
//   {
//      return getSubscriptionsCount(new JBossTopic(topicName), listType);
//   }
//
//   public void dropSubscription(String subscription) throws Exception
//   {
//      messagingServerManagement.destroyQueue(subscription);
//   }
//
//   public int getConsumerCountForQueue(String queue) throws Exception
//   {
//      return getConsumerCount(new JBossQueue(queue));
//   }
//
//   public List<ConnectionInfo> getConnections() throws Exception
//   {
//      return getConnectionsForUser(null);
//   }
//
//   public List<ConnectionInfo> getConnectionsForUser(String user) throws Exception
//   {
//      List<ConnectionInfo> connectionInfos = new ArrayList<ConnectionInfo>();
//      List<ServerConnection> endpoints = messagingServerManagement.getActiveConnections();
//      for (ServerConnection endpoint : endpoints)
//      {
//         if (user == null || user.equals(endpoint.getUsername()))
//         {
//            connectionInfos.add(new ConnectionInfo(endpoint.getID(),
//                    endpoint.getUsername(),
//                    endpoint.getClientAddress(),
//                    endpoint.isStarted(),
//                    endpoint.getCreated()));
//         }
//      }
//      return connectionInfos;
//   }
//
//   public void dropConnection(long clientId) throws Exception
//   {
//      List<ServerConnection> endpoints = messagingServerManagement.getActiveConnections();
//      for (ServerConnection endpoint : endpoints)
//      {
//         if (endpoint.getID() == clientId)
//         {
//            endpoint.close();
//            break;
//         }
//      }
//   }
//
//   public void dropConnectionsForUser(String user) throws Exception
//   {
//      List<ServerConnection> endpoints = messagingServerManagement.getActiveConnections();
//      List<ConnectionInfo> connectionInfos = getConnectionsForUser(user);
//      for (ConnectionInfo connectionInfo : connectionInfos)
//      {
//         for (ServerConnection endpoint : endpoints)
//         {
//            if (endpoint.getID() == connectionInfo.getId())
//            {
//               endpoint.close();
//               break;
//            }
//         }
//      }
//   }
//
////   public List<SessionInfo> getSessions() throws Exception
////   {
////      return getSessionsForConnection(null);
////   }
////
////   public List<SessionInfo> getSessionsForConnection(long id) throws Exception
////   {
////      List<SessionInfo> sessionInfos = new ArrayList<SessionInfo>();
////      List<ServerConnection> endpoints = messagingServerManagement.getActiveConnections();
////      for (ServerConnection endpoint : endpoints)
////      {
////         if (id == endpoint.getID())
////         {
////            Collection<ServerSession> serverSessionEndpoints = endpoint.getSessions();
////            for (ServerSession serverSessionEndpoint : serverSessionEndpoints)
////            {
////               sessionInfos.add(new SessionInfo(serverSessionEndpoint.getID(),
////                       endpoint.getID()));
////            }
////         }
////      }
////      return sessionInfos;
////   }
//
////   public List<SessionInfo> getSessionsForUser(String user) throws Exception
////   {
////      List<SessionInfo> sessionInfos = new ArrayList<SessionInfo>();
////      List<ServerConnection> endpoints = messagingServerManagement.getActiveConnections();
////      for (ServerConnection endpoint : endpoints)
////      {
////         if (user == null || user.equals(endpoint.getUsername()))
////         {
////            sessionInfos.addAll(getSessionsForConnection(endpoint.getID()));
////         }
////      }
////      return sessionInfos;
////   }
//
//   public void startGatheringStatistics() throws Exception
//   {
//      Set<String> availableAddresses = messagingServerManagement.listAvailableAddresses();
//      for (String address : availableAddresses)
//      {
//         if (address.startsWith(JBossQueue.JMS_QUEUE_ADDRESS_PREFIX))
//         {
//            messagingServerManagement.startMessageCounter(address, 0);
//         }
//      }
//   }
//
//   public void startGatheringStatisticsForQueue(String queue) throws Exception
//   {
//      JBossQueue jBossQueue = new JBossQueue(queue);
//      messagingServerManagement.startMessageCounter(jBossQueue.getAddress(), 0);
//   }
//
//   public List<MessageStatistics> stopGatheringStatistics() throws Exception
//   {
//      List<MessageStatistics> messageStatisticses = createStats(messagingServerManagement.stopAllMessageCounters());
//      messagingServerManagement.unregisterAllMessageCounters();
//      return messageStatisticses;
//   }
//
//   public MessageStatistics stopGatheringStatisticsForQueue(String queue) throws Exception
//   {
//      JBossQueue queue1 = new JBossQueue(queue);
//      MessageCounter counter = messagingServerManagement.stopMessageCounter(queue1.getAddress());
//
//      MessageStatistics stats = new MessageStatistics();
//      stats.setName(counter.getDestinationName());
//      stats.setDurable(counter.getDestinationDurable());
//      stats.setCount(counter.getMessageCount());
//      stats.setTotalMessageCount(counter.getTotalMessages());
//      stats.setCurrentMessageCount(counter.getCurrentMessageCount());
//      stats.setTimeLastUpdate(counter.getLastUpdate());
//      messagingServerManagement.unregisterMessageCounter(queue1.getAddress());
//      return stats;
//   }
//
//   public List<MessageStatistics> getStatistics() throws Exception
//   {
//      Collection<MessageCounter> counters = messagingServerManagement.getMessageCounters();
//      return createStats(counters);
//   }
//
//   private List<MessageStatistics> createStats(Collection<MessageCounter> counters)
//   {
//      List<MessageStatistics> list = new ArrayList<MessageStatistics>(counters.size());
//      for (Object counter1 : counters)
//      {
//         MessageCounter counter = (MessageCounter) counter1;
//
//         MessageStatistics stats = new MessageStatistics();
//         stats.setName(counter.getDestinationName());
//         stats.setDurable(counter.getDestinationDurable());
//         stats.setCount(counter.getMessageCount());
//         stats.setTotalMessageCount(counter.getTotalMessages());
//         stats.setCurrentMessageCount(counter.getCurrentMessageCount());
//         stats.setTimeLastUpdate(counter.getLastUpdate());
//
//         list.add(stats);
//      }
//      return list;
//   }
//   //private
//
   private void addToDestinationBindings(String destination, String jndiBinding)
   {
      if (destinations.get(destination) == null)
      {
         destinations.put(destination, new ArrayList<String>());
      }
      destinations.get(destination).add(jndiBinding);
   }
//
//
//   private List<Message> listMessages(String queue, ListType listType) throws Exception
//   {
//      List<Message> messages = new ArrayList<Message>();
//      Filter filter = null;
//      switch (listType)
//      {
//         case DURABLE:
//            filter = new FilterImpl("JBMDurable='DURABLE'");
//            break;
//         case NON_DURABLE:
//            filter = new FilterImpl("JBMDurable='NON_DURABLE'");
//            break;
//      }
//      List<org.jboss.messaging.core.message.Message> messageList = messagingServerManagement.listMessages(queue, filter);
//      for (org.jboss.messaging.core.message.Message message : messageList)
//      {
//         messages.add(JBossMessage.createMessage(message, null));
//      }
//      return messages;
//   }
//
//
   private void removeAllMessages(JBossQueue queue) throws Exception
   {
      messagingServerManagement.removeAllMessagesForAddress(queue.getSimpleAddress());
   }

   private void removeAllMessages(JBossTopic topic) throws Exception
   {
      messagingServerManagement.removeAllMessagesForAddress(topic.getSimpleAddress());
   }

   private int getMessageCount(JBossQueue queue) throws Exception
   {
      return messagingServerManagement.getMessageCountForQueue(queue.getSimpleAddress());
   }
//
//   private int getMessageCount(JBossTopic topic) throws Exception
//   {
//      return 0;  //To change body of implemented methods use File | Settings | File Templates.
//   }
//
   private List<SubscriptionInfo> listSubscriptions(JBossTopic topic) throws Exception
   {
      return listSubscriptions(topic, ListType.ALL);
   }

   private List<SubscriptionInfo> listSubscriptions(JBossTopic topic, ListType type) throws Exception
   {
      List<SubscriptionInfo> subs = new ArrayList<SubscriptionInfo>();

      List<Queue> queues = messagingServerManagement.getQueuesForAddress(topic.getSimpleAddress());

      for (Queue queue : queues)
      {
         if (type == ListType.ALL || (type == ListType.DURABLE && queue.isDurable()) || (type == ListType.NON_DURABLE && !queue.isDurable()))
         {
            String subName = null;
            String clientID = null;

            if (queue.isDurable())
            {
               Pair<String, String> pair = JBossTopic.decomposeQueueNameForDurableSubscription(queue.getName().toString());
               clientID = pair.a;
               subName = pair.b;
            }

            SubscriptionInfo info = new SubscriptionInfo(queue.getName().toString(), queue.isDurable(), subName, clientID,
                    queue.getFilter() == null ? null : queue.getFilter().getFilterString().toString(), queue.getMessageCount(), queue.getMaxSizeBytes());

            subs.add(info);
         }
      }

      return subs;
   }
//
//   private int getSubscriptionsCount(JBossTopic topic) throws Exception
//   {
//      return getSubscriptionsCount(topic, ListType.ALL);
//   }
//
//   private int getSubscriptionsCount(JBossTopic topic, ListType listType) throws Exception
//   {
//      return listSubscriptions(topic, listType).size();
//   }
//
//   private int getConsumerCount(JBossQueue queue) throws Exception
//   {
//      return messagingServerManagement.getConsumerCountForQueue(queue.getAddress());
//   }

   public void setInitialContext(InitialContext initialContext)
   {
      this.initialContext = initialContext;
   }
}
