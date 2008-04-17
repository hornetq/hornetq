/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.management.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.server.MessagingComponent;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConnection;

/**
 * This interface describes the properties and operations that comprise the management interface of the
 * Messaging Server.
 * <p/>
 * It includes operations to create and destroy queues and provides various statistics measures
 * such as message count for queues and topics.
 *
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 */
//@JMX(name = "jboss.messaging:service=MessagingServerManagement", exposedInterface = MessagingServerManagement.class)
public class MessagingServerManagementImpl implements MessagingServerManagement, MessagingComponent
{
   private MessagingServer messagingServer;

   private HashMap<String, MessageCounter> currentCounters = new HashMap<String, MessageCounter>();

   private HashMap<String, ScheduledFuture> currentRunningCounters = new HashMap<String, ScheduledFuture>();

   private ScheduledExecutorService scheduler;

   private int maxMessageCounters = 20;

   public void setMessagingServer(MessagingServer messagingServer)
   {
      this.messagingServer = messagingServer;
   }

   public boolean isStarted()
   {
      return messagingServer.isStarted();
   }

   public void createQueue(String address, String name) throws Exception
   {
      if (messagingServer.getPostOffice().getBinding(name) == null)
      {
         messagingServer.getPostOffice().addBinding(address, name, null, true, false);
      }
   }

   public void destroyQueue(String name) throws Exception
   {
      Binding binding = messagingServer.getPostOffice().getBinding(name);

      if (binding != null)
      {
         Queue queue = binding.getQueue();

         queue.deleteAllReferences(messagingServer.getStorageManager());
         
         messagingServer.getPostOffice().removeBinding(queue.getName());
      }
   }

   public boolean addDestination(String address) throws Exception
   {
      return messagingServer.getPostOffice().addDestination(address, false);
   }

   public boolean removeDestination(String address) throws Exception
   {
      return messagingServer.getPostOffice().removeDestination(address, false);
   }

   public ClientConnectionFactory createClientConnectionFactory(boolean strictTck,
   		                                                       int consumerWindowSize, int consumerMaxRate,
   		                                                       int producerWindowSize, int producerMaxRate)
   {
      return new ClientConnectionFactoryImpl(messagingServer.getConfiguration().getMessagingServerID(),
              messagingServer.getConfiguration(),
              messagingServer.getConfiguration().isStrictTck() || strictTck,
              consumerWindowSize, consumerMaxRate,
              producerWindowSize, producerMaxRate);
   }

   public void removeAllMessagesForAddress(String address) throws Exception
   {
      List<Binding> bindings = messagingServer.getPostOffice().getBindingsForAddress(address);

      for (Binding binding : bindings)
      {
         Queue queue = binding.getQueue();

         queue.deleteAllReferences(messagingServer.getStorageManager());
      }
   }

   public void removeAllMessagesForBinding(String name) throws Exception
   {
      Binding binding = messagingServer.getPostOffice().getBinding(name);
      if (binding != null)
      {
         Queue queue = binding.getQueue();

         queue.deleteAllReferences(messagingServer.getStorageManager());
      }
   }

   public List<Message> listMessages(String queueName, Filter filter) throws Exception
   {
      List<Message> msgs = new ArrayList<Message>();
      Queue queue = getQueue(queueName);
      if (queue != null)
      {
         List<MessageReference> allRefs = queue.list(filter);
         for (MessageReference allRef : allRefs)
         {
            msgs.add(allRef.getMessage());
         }
      }
      return msgs;
   }

//   public void removeMessageForBinding(String name, Filter filter) throws Exception
//   {
//      Binding binding = messagingServer.getPostOffice().getBinding(name);
//      if (binding != null)
//      {
//         Queue queue = binding.getQueue();
//         List<MessageReference> allRefs = queue.list(filter);
//         for (MessageReference messageReference : allRefs)
//         {
//            messagingServer.getPersistenceManager().deleteReference(messageReference);
//            queue.removeReference(messageReference);
//         }
//      }
//   }

//   public void removeMessageForAddress(String binding, Filter filter) throws Exception
//   {
//      List<Binding> bindings = messagingServer.getPostOffice().getBindingsForAddress(binding);
//      for (Binding binding1 : bindings)
//      {
//         removeMessageForBinding(binding1.getQueue().getName(), filter);
//      }
//   }

   public List<Queue> getQueuesForAddress(String address) throws Exception
   {
      List<Queue> queues = new ArrayList<Queue>();
      List<Binding> bindings = messagingServer.getPostOffice().getBindingsForAddress(address);

      for (Binding binding : bindings)
      {
         Queue queue = binding.getQueue();
         queues.add(queue);
      }
      return queues;
   }

   public int getMessageCountForQueue(String queue) throws Exception
   {
      return getQueue(queue).getMessageCount();
   }

   public int getMaxMessageCounters()
   {
      return maxMessageCounters;
   }

   public void setMaxMessageCounters(int maxMessageCounters)
   {
      this.maxMessageCounters = maxMessageCounters;
   }

   public void registerMessageCounter(final String queueName) throws Exception
   {
      if (currentCounters.get(queueName) != null)
      {
         throw new IllegalStateException("Message Counter Already Registered");
      }
      Binding binding = messagingServer.getPostOffice().getBinding(queueName);
      if (binding == null)
      {
         throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
      }
      Queue queue = binding.getQueue();
      currentCounters.put(queueName, new MessageCounter(queue.getName(), queue, queue.isDurable(),
      		messagingServer.getQueueSettingsRepository().getMatch(queue.getName()).getMessageCounterHistoryDayLimit()));
   }

   public void unregisterMessageCounter(final String queueName) throws Exception
   {
      if (currentCounters.get(queueName) == null)
      {
         throw new MessagingException(MessagingException.ILLEGAL_STATE, "Counter is not registered");
      }
      currentCounters.remove(queueName);
      if (currentRunningCounters.get(queueName) != null)
      {
         currentRunningCounters.get(queueName).cancel(true);
         currentRunningCounters.remove(queueName);
      }
   }

   public void startMessageCounter(final String queueName, long duration) throws Exception
   {
      MessageCounter messageCounter = currentCounters.get(queueName);
      if (messageCounter == null)
      {
         Binding binding = messagingServer.getPostOffice().getBinding(queueName);
         if (binding == null)
         {
            throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
         }
         Queue queue = binding.getQueue();
         messageCounter = new MessageCounter(queue.getName(), queue, queue.isDurable(),
         		messagingServer.getQueueSettingsRepository().getMatch(queue.getName()).getMessageCounterHistoryDayLimit());
      }
      currentCounters.put(queueName, messageCounter);
      messageCounter.resetCounter();
      if (duration > 0)
      {

         ScheduledFuture future = scheduler.schedule(new Runnable()
         {
            public void run()
            {
               currentCounters.get(queueName).sample();
            }
         }, duration, TimeUnit.SECONDS);
         currentRunningCounters.put(queueName, future);
      }
   }

   public MessageCounter stopMessageCounter(String queueName) throws Exception
   {
      MessageCounter messageCounter = currentCounters.get(queueName);
      if (messageCounter == null)
      {
         throw new IllegalArgumentException(queueName + "counter not registered");
      }
      if (currentRunningCounters.get(queueName) != null)
      {
         currentRunningCounters.get(queueName).cancel(true);
         currentRunningCounters.remove(queueName);
      }
      messageCounter.sample();
      return messageCounter;
   }

   public MessageCounter getMessageCounter(String queueName)
   {
      MessageCounter messageCounter = currentCounters.get(queueName);
      if (messageCounter != null && currentRunningCounters.get(queueName) == null)
      {
         messageCounter.sample();
      }
      return messageCounter;
   }


   public Collection<MessageCounter> getMessageCounters()
   {
      for (String s : currentCounters.keySet())
      {
         currentCounters.get(s).sample();
      }
      return currentCounters.values();
   }

   public void resetMessageCounter(String queue)
   {
      MessageCounter messageCounter = currentCounters.get(queue);
      if (messageCounter != null)
      {
         messageCounter.resetCounter();
      }
   }

   public void resetMessageCounters()
   {
      Set<String> counterNames = currentCounters.keySet();
      for (String counterName : counterNames)
      {
         resetMessageCounter(counterName);
      }
   }

   public void resetMessageCounterHistory(String queue)
   {
      MessageCounter messageCounter = currentCounters.get(queue);
      if (messageCounter != null)
      {
         messageCounter.resetHistory();
      }
   }

   public void resetMessageCounterHistories()
   {
      Set<String> counterNames = currentCounters.keySet();
      for (String counterName : counterNames)
      {
         resetMessageCounterHistory(counterName);
      }
   }

   public List<MessageCounter> stopAllMessageCounters() throws Exception
   {
      Set<String> counterNames = currentCounters.keySet();
      List<MessageCounter> messageCounters = new ArrayList<MessageCounter>();
      for (String counterName : counterNames)
      {
         messageCounters.add(stopMessageCounter(counterName));
      }
      return messageCounters;
   }

   public void unregisterAllMessageCounters() throws Exception
   {
      Set<String> counterNames = currentCounters.keySet();
      for (String counterName : counterNames)
      {
         unregisterMessageCounter(counterName);
      }
   }

   public int getConsumerCountForQueue(String queue) throws Exception
   {
      return getQueue(queue).getConsumerCount();
   }

   public List<ServerConnection> getActiveConnections()
   {
      return messagingServer.getConnectionManager().getActiveConnections();
   }

//   public void moveMessages(String fromQueue, String toQueue, String filter) throws Exception
//   {
//      Filter actFilter = new FilterImpl(filter);
//      Queue from = getQueue(fromQueue);
//      Queue to = getQueue(toQueue);
//      List<MessageReference> messageReferences = from.list(actFilter);
//      for (MessageReference messageReference : messageReferences)
//      {
//         from.move(messageReference, to, messagingServer.getPersistenceManager());
//      }
//
//   }

   public void expireMessages(String queue, String filter) throws Exception
   {
      Filter actFilter = new FilterImpl(filter);
      List<MessageReference> allRefs = getQueue(queue).list(actFilter);
      for (MessageReference messageReference : allRefs)
      {
         messageReference.getMessage().setExpiration(System.currentTimeMillis());
      }
   }

//   public void changeMessagePriority(String queue, String filter, int priority) throws Exception
//   {
//      Filter actFilter = new FilterImpl(filter);
//      List<MessageReference> allRefs = getQueue(queue).list(actFilter);
//      for (MessageReference messageReference : allRefs)
//      {
//         List<MessageReference> allRefsForMessage = messageReference.getMessage().getReferences();
//         for (MessageReference reference : allRefsForMessage)
//         {
//            reference.getQueue().changePriority(reference, priority);
//         }
//         messageReference.getMessage().setPriority((byte) priority);
//      }
//
//   }

   public Set<String> listAvailableAddresses()
   {
      return messagingServer.getPostOffice().listAllDestinations();
   }

   // Private ---------------------------------------------------------------------------


   private Queue getQueue(String queueName) throws Exception
   {
      Binding binding = messagingServer.getPostOffice().getBinding(queueName);
      if (binding == null)
      {
         throw new IllegalArgumentException("No queue with name " + queueName);
      }

      return binding.getQueue();
   }



   public void start() throws Exception
   {
      scheduler = Executors.newScheduledThreadPool(maxMessageCounters);
   }

   public void stop() throws Exception
   {
      if (scheduler != null)
      {
         scheduler.shutdown();
      }
   }

   protected void finalize() throws Throwable
   {
      super.finalize();

   }
}
