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

package org.jboss.messaging.core.management.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;

/**
 * This interface describes the properties and operations that comprise the
 * management interface of the Messaging Server. <p/> It includes operations to
 * create and destroy queues and provides various statistics measures such as
 * message count for queues and topics.
 * 
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class MessagingServerManagementImpl implements MessagingServerManagement
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final PostOffice postOffice;

   private final StorageManager storageManager;

   private final Configuration configuration;

   private final MessagingServer server;

   private HierarchicalRepository<Set<Role>> securityRepository;

   private HierarchicalRepository<QueueSettings> queueSettingsRepository;   
   
   public MessagingServerManagementImpl(final PostOffice postOffice, final StorageManager storageManager,
                                        final Configuration configuration,                                                                             
                                        final HierarchicalRepository<Set<Role>> securityRepository,
                                        final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                                        final MessagingServer server)
   {
      this.postOffice = postOffice;

      this.storageManager = storageManager;

      this.configuration = configuration;

      this.server = server;

      this.securityRepository = securityRepository;

      this.queueSettingsRepository = queueSettingsRepository;
   }

   // MessagingServerManagement implementation ----------------------

   public boolean isStarted()
   {
      return server.isStarted();
   }

   public String getVersion()
   {
      return server.getVersion().getFullVersion();
   }

   public void createQueue(final SimpleString address, final SimpleString name)
         throws Exception
   {
      if (postOffice.getBinding(name) == null)
      {
         postOffice.addBinding(address, name, null, true, false);
      }
   }

   public void createQueue(final SimpleString address, final SimpleString name,
         final SimpleString filterStr, final boolean durable,
         final boolean temporary) throws Exception
   {
      if (postOffice.getBinding(name) == null)
      {
         Filter filter = null;
         if (filterStr != null)
         {
            filter = new FilterImpl(filterStr);
         }
         postOffice.addBinding(address, name, filter, durable, temporary);
      }
   }

   public int getConnectionCount()
   {
      return server.getConnectionCount();
   }

   public void destroyQueue(final SimpleString name) throws Exception
   {
      Binding binding = postOffice.getBinding(name);

      if (binding != null)
      {
         Queue queue = binding.getQueue();

         queue.deleteAllReferences(storageManager);

         postOffice.removeBinding(queue.getName());
      }
   }

   public boolean addDestination(final SimpleString address) throws Exception
   {
      return postOffice.addDestination(address, false);
   }

   public boolean removeDestination(final SimpleString address)
         throws Exception
   {
      return postOffice.removeDestination(address, false);
   }

   public void removeAllMessagesForAddress(final SimpleString address)
         throws Exception
   {
      List<Binding> bindings = postOffice.getBindingsForAddress(address);

      for (Binding binding : bindings)
      {
         Queue queue = binding.getQueue();

         queue.deleteAllReferences(storageManager);
      }
   }

   public boolean removeMessageFromAddress(final long messageID,
         SimpleString address) throws Exception
   {
      Binding binding = postOffice.getBinding(address);
      if (binding != null)
      {
         Queue queue = binding.getQueue();
         return queue.deleteReference(messageID, storageManager);
      }
      return false;
   }

   public boolean expireMessage(final long messageID, final SimpleString address)
         throws Exception
   {
      Binding binding = postOffice.getBinding(address);
      if (binding != null)
      {
         Queue queue = binding.getQueue();
         return queue.expireMessage(messageID, storageManager, postOffice,
               queueSettingsRepository);
      }
      return false;
   }

   public int expireMessages(final Filter filter,
         final SimpleString address) throws Exception
   {
      Binding binding = postOffice.getBinding(address);
      if (binding != null)
      {
         Queue queue = binding.getQueue();
         List<MessageReference> refs = queue.list(filter);
         for (MessageReference ref : refs)
         {
            queue.expireMessage(ref.getMessage().getMessageID(),
                  storageManager, postOffice, queueSettingsRepository);
         }
         return refs.size();
      }
      return 0;
   }

   public int sendMessagesToDLQ(final Filter filter,
         final SimpleString address) throws Exception
   {
      Binding binding = postOffice.getBinding(address);
      if (binding != null)
      {
         Queue queue = binding.getQueue();
         List<MessageReference> refs = queue.list(filter);
         for (MessageReference ref : refs)
         {
            queue.sendMessageToDLQ(ref.getMessage().getMessageID(),
                  storageManager, postOffice, queueSettingsRepository);
         }
         return refs.size();
      }
      return 0;
   }

   public int changeMessagesPriority(final Filter filter,
         final byte newPriority, final SimpleString address) throws Exception
   {
      Binding binding = postOffice.getBinding(address);
      if (binding != null)
      {
         Queue queue = binding.getQueue();
         List<MessageReference> refs = queue.list(filter);
         for (MessageReference ref : refs)
         {
            queue.changeMessagePriority(ref.getMessage().getMessageID(),
                  newPriority, storageManager, postOffice,
                  queueSettingsRepository);
         }
         return refs.size();
      }
      return 0;
   }

   public QueueSettings getQueueSettings(final SimpleString simpleAddress)
   {
      return queueSettingsRepository.getMatch(simpleAddress.toString());
   }

   public List<Queue> getQueuesForAddress(final SimpleString address)
         throws Exception
   {
      List<Queue> queues = new ArrayList<Queue>();
      List<Binding> bindings = postOffice.getBindingsForAddress(address);

      for (Binding binding : bindings)
      {
         Queue queue = binding.getQueue();
         queues.add(queue);
      }
      return queues;
   }

   public void setSecurityForAddress(final SimpleString address, final Set<Role> roles)
         throws Exception
   {
      this.securityRepository.addMatch(address.toString(), roles);
   }

   public void removeSecurityForAddress(final SimpleString address) throws Exception
   {
      this.securityRepository.removeMatch(address.toString());
   }

   public Set<Role> getSecurityForAddress(final SimpleString address)
         throws Exception
   {
      return this.securityRepository.getMatch(address.toString());
   }

   public void setQueueAttributes(final SimpleString queueName,
         final QueueSettings settings) throws Exception
   {
      this.queueSettingsRepository.addMatch(queueName.toString(), settings);
   }

   public Configuration getConfiguration()
   {
      return configuration;
   }

   public Queue getQueue(final SimpleString queueName) throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);
      if (binding == null)
      {
         throw new IllegalArgumentException("No queue with name " + queueName);
      }

      return binding.getQueue();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
