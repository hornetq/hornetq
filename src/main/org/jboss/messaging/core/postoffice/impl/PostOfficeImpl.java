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

package org.jboss.messaging.core.postoffice.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.AddressManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.FlowController;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A PostOfficeImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class PostOfficeImpl implements PostOffice
{
   private static final Logger log = Logger.getLogger(PostOfficeImpl.class);

   private final AddressManager addressManager;

   private final ConcurrentMap<SimpleString, FlowController> flowControllers = new ConcurrentHashMap<SimpleString, FlowController>();

   private final QueueFactory queueFactory;

   private final boolean checkAllowable;

   private final StorageManager storageManager;

   private final PagingManager pagingManager;

   private volatile boolean started;

   private volatile boolean backup;

   private final ManagementService managementService;

   private final ResourceManager resourceManager;

   public PostOfficeImpl(final StorageManager storageManager,
                         final PagingManager pagingManager,
                         final QueueFactory queueFactory,
                         final ManagementService managementService,
                         final boolean checkAllowable,
                         final ResourceManager resourceManager,
                         final boolean enableWildCardRouting)
   {
      this.storageManager = storageManager;

      this.queueFactory = queueFactory;

      this.managementService = managementService;

      this.checkAllowable = checkAllowable;

      this.pagingManager = pagingManager;

      this.resourceManager = resourceManager;

      if (enableWildCardRouting)
      {
         addressManager = new WildcardAddressManager();
      }
      else
      {
         addressManager = new SimpleAddressManager();
      }
   }

   // MessagingComponent implementation ---------------------------------------

   public void start() throws Exception
   {
      if (pagingManager != null)
      {
         pagingManager.setPostOffice(this);

         pagingManager.start();
      }

      // Injecting the postoffice (itself) on queueFactory for paging-control
      queueFactory.setPostOffice(this);

      loadBindings();

      started = true;
   }

   public void stop() throws Exception
   {
      pagingManager.stop();

      addressManager.clear();

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   // PostOffice implementation -----------------------------------------------

   public boolean addDestination(final SimpleString address, final boolean durable) throws Exception
   {
      boolean added = addressManager.addDestination(address);// destinations.addIfAbsent(address);

      if (added)
      {
         if (durable)
         {
            storageManager.addDestination(address);
         }

         flowControllers.put(address, new FlowControllerImpl(address, this));
         managementService.registerAddress(address);
      }

      return added;
   }

   public boolean removeDestination(final SimpleString address, final boolean durable) throws Exception
   {
      boolean removed = addressManager.removeDestination(address);

      if (removed)
      {
         flowControllers.remove(address);

         if (durable)
         {
            storageManager.deleteDestination(address);
         }
         managementService.unregisterAddress(address);
      }

      return removed;
   }

   public boolean containsDestination(final SimpleString address)
   {
      return addressManager.containsDestination(address);
   }

   public Set<SimpleString> listAllDestinations()
   {
      return addressManager.getDestinations();
   }

   public Binding addBinding(final SimpleString address,
                             final SimpleString queueName,
                             final Filter filter,
                             final boolean durable, boolean temporary) throws Exception
   {
      Binding binding = createBinding(address, queueName, filter, durable, temporary);

      addBindingInMemory(binding);

      if (durable)
      {
         storageManager.addBinding(binding);
      }

      return binding;
   }

   public Binding removeBinding(final SimpleString queueName) throws Exception
   {
      Binding binding = removeQueueInMemory(queueName);

      if (binding.getQueue().isDurable())
      {
         storageManager.deleteBinding(binding);
      }

      managementService.unregisterQueue(queueName, binding.getAddress());

      return binding;
   }

   public List<Binding> getBindingsForAddress(final SimpleString address)
   {
      List<Binding> bindings = addressManager.getBindings(address);

      if (bindings != null)
      {
         return bindings;
      }
      else
      {
         return Collections.emptyList();
      }
   }

   public Binding getBinding(final SimpleString queueName)
   {
      return addressManager.getBinding(queueName);
   }

   public List<MessageReference> route(final ServerMessage message) throws Exception
   {
      long size = pagingManager.addSize(message);

      if (size < 0)
      {
         return new ArrayList<MessageReference>();
      }
      else
      {
         SimpleString address = message.getDestination();

         if (checkAllowable)
         {
            if (!addressManager.containsDestination(address))
            {
               throw new MessagingException(MessagingException.ADDRESS_DOES_NOT_EXIST,
                                            "Cannot route to address " + address);
            }
         }

         List<Binding> bindings = addressManager.getBindings(address);

         List<MessageReference> refs = new ArrayList<MessageReference>();

         if (bindings != null)
         {
            for (Binding binding : bindings)
            {
               Queue queue = binding.getQueue();

               Filter filter = queue.getFilter();

               if (filter == null || filter.match(message))
               {
                  MessageReference reference = message.createReference(queue);

                  refs.add(reference);
               }
            }
         }

         return refs;
      }

   }

   // public void routeFromCluster(final String address, final Message message)
   // throws Exception
   // {
   // List<Binding> bindings = mappings.get(address);
   //      
   // for (Binding binding: bindings)
   // {
   // Queue queue = binding.getQueue();
   //         
   // if (binding.getNodeID() == nodeID)
   // {
   // if (queue.getFilter() == null || queue.getFilter().match(message))
   // {
   // MessageReference ref = message.createReference(queue);
   //
   // //We never route durably from other nodes - so no need to persist
   //
   // queue.addLast(ref);
   // }
   // }
   // }
   // }

   public PagingManager getPagingManager()
   {
      return pagingManager;
   }

   public Map<SimpleString, List<Binding>> getMappings()
   {
      return addressManager.getMappings();
   }

   public FlowController getFlowController(final SimpleString address)
   {
      return flowControllers.get(address);
   }

   public void setBackup(final boolean backup)
   {
      if (this.backup != backup)
      {
         this.backup = backup;

         Map<SimpleString, Binding> nameMap = addressManager.getBindings();

         for (Binding binding : nameMap.values())
         {
            binding.getQueue().setBackup(backup);
         }
      }
   }

   // Private -----------------------------------------------------------------

   private Binding createBinding(final SimpleString address,
                                 final SimpleString name,
                                 final Filter filter,
                                 final boolean durable, final boolean temporary) throws Exception
   {
      Queue queue = queueFactory.createQueue(-1, name, filter, durable, false);

      queue.setBackup(backup);

      Binding binding = new BindingImpl(address, queue);

      return binding;
   }

   private void addBindingInMemory(final Binding binding) throws Exception
   {
      boolean exists = addressManager.addMapping(binding.getAddress(), binding);
      if (!exists)
      {
         managementService.registerAddress(binding.getAddress());
      }

      managementService.registerQueue(binding.getQueue(), binding.getAddress(), storageManager);

      addressManager.addBinding(binding);

      FlowController flowController = flowControllers.get(binding.getAddress());

      binding.getQueue().setFlowController(flowController);
   }

   private Binding removeQueueInMemory(final SimpleString queueName) throws Exception
   {
      Binding binding = addressManager.removeBinding(queueName);

      if (addressManager.removeMapping(binding.getAddress(), queueName))
      {
         managementService.unregisterAddress(binding.getAddress());

         binding.getQueue().setFlowController(null);
      }

      return binding;
   }

   private void loadBindings() throws Exception
   {
      List<Binding> bindings = new ArrayList<Binding>();

      List<SimpleString> dests = new ArrayList<SimpleString>();

      storageManager.loadBindings(queueFactory, bindings, dests);

      // Destinations must be added first to ensure flow controllers exist
      // before queues are created
      for (SimpleString destination : dests)
      {
         addDestination(destination, true);
      }

      Map<Long, Queue> queues = new HashMap<Long, Queue>();

      for (Binding binding : bindings)
      {
         addBindingInMemory(binding);

         queues.put(binding.getQueue().getPersistenceID(), binding.getQueue());
      }

      storageManager.loadMessages(this, queues, resourceManager);

      for (SimpleString destination : dests)
      {
         if (!pagingManager.isGlobalPageMode())
         {
            PagingStore store = pagingManager.getPageStore(destination);
            store.startDepaging();
         }
      }
   }

}
