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

import org.jboss.messaging.core.config.Configuration;
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
import org.jboss.messaging.util.SimpleString;

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

//   private HashMap<String, MessageCounter> currentCounters = new HashMap<String, MessageCounter>();
//
//   private HashMap<String, ScheduledFuture> currentRunningCounters = new HashMap<String, ScheduledFuture>();
//
//   private ScheduledExecutorService scheduler;
//
//   private int maxMessageCounters = 20;

   public void setMessagingServer(MessagingServer messagingServer)
   {
      this.messagingServer = messagingServer;
   }

   public boolean isStarted()
   {
      return messagingServer.isStarted();
   }

   public void createQueue(SimpleString address, SimpleString name) throws Exception
   {
      if (messagingServer.getPostOffice().getBinding(name) == null)
      {
         messagingServer.getPostOffice().addBinding(address, name, null, true, false);
      }
   }

   public void destroyQueue(SimpleString name) throws Exception
   {
      Binding binding = messagingServer.getPostOffice().getBinding(name);

      if (binding != null)
      {
         Queue queue = binding.getQueue();

         queue.deleteAllReferences(messagingServer.getStorageManager());
         
         messagingServer.getPostOffice().removeBinding(queue.getName());
      }
   }

   public boolean addDestination(SimpleString address) throws Exception
   {
      return messagingServer.getPostOffice().addDestination(address, false);
   }

   public boolean removeDestination(SimpleString address) throws Exception
   {
      return messagingServer.getPostOffice().removeDestination(address, false);
   }

   public void removeAllMessagesForAddress(SimpleString address) throws Exception
   {
      List<Binding> bindings = messagingServer.getPostOffice().getBindingsForAddress(address);

      for (Binding binding : bindings)
      {
         Queue queue = binding.getQueue();

         queue.deleteAllReferences(messagingServer.getStorageManager());
      }
   }
//
//   public void removeAllMessagesForBinding(SimpleString name) throws Exception
//   {
//      Binding binding = messagingServer.getPostOffice().getBinding(name);
//      if (binding != null)
//      {
//         Queue queue = binding.getQueue();
//
//         queue.deleteAllReferences(messagingServer.getStorageManager());
//      }
//   }
//
//   public List<Message> listMessages(SimpleString queueName, Filter filter) throws Exception
//   {
//      List<Message> msgs = new ArrayList<Message>();
//      Queue queue = getQueue(queueName);
//      if (queue != null)
//      {
//         List<MessageReference> allRefs = queue.list(filter);
//         for (MessageReference allRef : allRefs)
//         {
//            msgs.add(allRef.getMessage());
//         }
//      }
//      return msgs;
//   }

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

   public List<Queue> getQueuesForAddress(SimpleString address) throws Exception
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

   public int getMessageCountForQueue(SimpleString queue) throws Exception
   {
      return getQueue(queue).getMessageCount();
   }
//
//   public int getMaxMessageCounters()
//   {
//      return maxMessageCounters;
//   }
//
//   public void setMaxMessageCounters(int maxMessageCounters)
//   {
//      this.maxMessageCounters = maxMessageCounters;
//   }
//
//   public void registerMessageCounter(final SimpleString queueName) throws Exception
//   {
//      if (currentCounters.get(queueName) != null)
//      {
//         throw new IllegalStateException("Message Counter Already Registered");
//      }
//      Binding binding = messagingServer.getPostOffice().getBinding(queueName);
//      if (binding == null)
//      {
//         throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
//      }
//      Queue queue = binding.getQueue();
//      currentCounters.put(queueName, new MessageCounter(queue.getName(), queue, queue.isDurable(),
//      		messagingServer.getQueueSettingsRepository().getMatch(queue.getName()).getMessageCounterHistoryDayLimit()));
//   }
//
//   public void unregisterMessageCounter(final SimpleString queueName) throws Exception
//   {
//      if (currentCounters.get(queueName) == null)
//      {
//         throw new MessagingException(MessagingException.ILLEGAL_STATE, "Counter is not registered");
//      }
//      currentCounters.remove(queueName);
//      if (currentRunningCounters.get(queueName) != null)
//      {
//         currentRunningCounters.get(queueName).cancel(true);
//         currentRunningCounters.remove(queueName);
//      }
//   }
//
//   public void startMessageCounter(final String SimpleString, long duration) throws Exception
//   {
//      MessageCounter messageCounter = currentCounters.get(queueName);
//      if (messageCounter == null)
//      {
//         Binding binding = messagingServer.getPostOffice().getBinding(queueName);
//         if (binding == null)
//         {
//            throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
//         }
//         Queue queue = binding.getQueue();
//         messageCounter = new MessageCounter(queue.getName(), queue, queue.isDurable(),
//         		messagingServer.getQueueSettingsRepository().getMatch(queue.getName()).getMessageCounterHistoryDayLimit());
//      }
//      currentCounters.put(queueName, messageCounter);
//      messageCounter.resetCounter();
//      if (duration > 0)
//      {
//
//         ScheduledFuture future = scheduler.schedule(new Runnable()
//         {
//            public void run()
//            {
//               currentCounters.get(queueName).sample();
//            }
//         }, duration, TimeUnit.SECONDS);
//         currentRunningCounters.put(queueName, future);
//      }
//   }
//
//   public MessageCounter stopMessageCounter(SimpleString queueName) throws Exception
//   {
//      MessageCounter messageCounter = currentCounters.get(queueName);
//      if (messageCounter == null)
//      {
//         throw new IllegalArgumentException(queueName + "counter not registered");
//      }
//      if (currentRunningCounters.get(queueName) != null)
//      {
//         currentRunningCounters.get(queueName).cancel(true);
//         currentRunningCounters.remove(queueName);
//      }
//      messageCounter.sample();
//      return messageCounter;
//   }
//
//   public MessageCounter getMessageCounter(SimpleString queueName)
//   {
//      MessageCounter messageCounter = currentCounters.get(queueName);
//      if (messageCounter != null && currentRunningCounters.get(queueName) == null)
//      {
//         messageCounter.sample();
//      }
//      return messageCounter;
//   }
//
//
//   public Collection<MessageCounter> getMessageCounters()
//   {
//      for (String s : currentCounters.keySet())
//      {
//         currentCounters.get(s).sample();
//      }
//      return currentCounters.values();
//   }
//
//   public void resetMessageCounter(SimpleString queue)
//   {
//      MessageCounter messageCounter = currentCounters.get(queue);
//      if (messageCounter != null)
//      {
//         messageCounter.resetCounter();
//      }
//   }
//
//   public void resetMessageCounters()
//   {
//      Set<String> counterNames = currentCounters.keySet();
//      for (String counterName : counterNames)
//      {
//         resetMessageCounter(counterName);
//      }
//   }
//
//   public void resetMessageCounterHistory(SimpleString queue)
//   {
//      MessageCounter messageCounter = currentCounters.get(queue);
//      if (messageCounter != null)
//      {
//         messageCounter.resetHistory();
//      }
//   }
//
//   public void resetMessageCounterHistories()
//   {
//      Set<String> counterNames = currentCounters.keySet();
//      for (String counterName : counterNames)
//      {
//         resetMessageCounterHistory(counterName);
//      }
//   }
//
//   public List<MessageCounter> stopAllMessageCounters() throws Exception
//   {
//      Set<String> counterNames = currentCounters.keySet();
//      List<MessageCounter> messageCounters = new ArrayList<MessageCounter>();
//      for (String counterName : counterNames)
//      {
//         messageCounters.add(stopMessageCounter(counterName));
//      }
//      return messageCounters;
//   }
//
//   public void unregisterAllMessageCounters() throws Exception
//   {
//      Set<String> counterNames = currentCounters.keySet();
//      for (String counterName : counterNames)
//      {
//         unregisterMessageCounter(counterName);
//      }
//   }
//
//   public int getConsumerCountForQueue(SimpleString queue) throws Exception
//   {
//      return getQueue(queue).getConsumerCount();
//   }
//
//   public List<ServerConnection> getActiveConnections()
//   {
//      return messagingServer.getConnectionManager().getActiveConnections();
//   }

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
//
//   public void expireMessages(SimpleString queue, SimpleString filter) throws Exception
//   {
//      Filter actFilter = new FilterImpl(filter);
//      List<MessageReference> allRefs = getQueue(queue).list(actFilter);
//      for (MessageReference messageReference : allRefs)
//      {
//         messageReference.getMessage().setExpiration(System.currentTimeMillis());
//      }
//   }

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
//
//   public Set<SimpleString> listAvailableAddresses()
//   {
//      return messagingServer.getPostOffice().listAllDestinations();
//   }

   public Configuration getConfiguration()
   {
      return messagingServer.getConfiguration();
   }

   // Private ---------------------------------------------------------------------------


   private Queue getQueue(SimpleString queueName) throws Exception
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
      //scheduler = Executors.newScheduledThreadPool(maxMessageCounters);
   }

   public void stop() throws Exception
   {
//      if (scheduler != null)
//      {
//         scheduler.shutdown();
//      }
   }

//   protected void finalize() throws Throwable
//   {
//      super.finalize();
//
//   }
}
