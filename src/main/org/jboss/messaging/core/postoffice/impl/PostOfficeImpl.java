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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.FlowController;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.ConcurrentSet;
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
   
   private final ConcurrentMap<SimpleString, List<Binding>> mappings = new ConcurrentHashMap<SimpleString, List<Binding>>();
   
   private final ConcurrentSet<SimpleString> destinations = new ConcurrentHashSet<SimpleString>();
   
   private final ConcurrentMap<SimpleString, Binding> nameMap = new ConcurrentHashMap<SimpleString, Binding>();
   
   private final ConcurrentMap<SimpleString, FlowController> flowControllers = new ConcurrentHashMap<SimpleString, FlowController>();
   
   private final QueueFactory queueFactory;
   
   private final boolean checkAllowable;
   
   private final StorageManager storageManager;
   
   private volatile boolean started;

   private volatile boolean backup;

   private final ManagementService managementService;
  
   public PostOfficeImpl(final StorageManager storageManager,
   		                final QueueFactory queueFactory, final ManagementService managementService, final boolean checkAllowable)
   {
      this.storageManager = storageManager;
      
      this.queueFactory = queueFactory;
      
      this.managementService = managementService;
      
      this.checkAllowable = checkAllowable;
   }
      
   // MessagingComponent implementation ---------------------------------------
   
   public void start() throws Exception
   {
      loadBindings();
      
      started = true;
   }

   public void stop() throws Exception
   {
      mappings.clear();
      
      destinations.clear();

      nameMap.clear();
      
      started = false;
   }
   
   public boolean isStarted()
   {
      return started;
   }
   
   // PostOffice implementation -----------------------------------------------

   public boolean addDestination(final SimpleString address, final boolean durable) throws Exception
   {      
   	boolean added = destinations.addIfAbsent(address);
   	
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
      boolean removed = destinations.remove(address);
      
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
      return destinations.contains(address);
   }

   public Set<SimpleString> listAllDestinations()
   {
      return destinations;
   }

   public Binding addBinding(final SimpleString address, final SimpleString queueName, final Filter filter, 
                             final boolean durable) throws Exception
   {
      Binding binding = createBinding(address, queueName, filter, durable);

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
      List<Binding> bindings = mappings.get(address);
      
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
      return nameMap.get(queueName);
   }
         
   public List<MessageReference> route(final ServerMessage message) throws Exception
   {
      SimpleString address = message.getDestination();
          
      if (checkAllowable)
      {
         if (!destinations.contains(address))
         {
            throw new MessagingException(MessagingException.ADDRESS_DOES_NOT_EXIST,
                                         "Cannot route to address " + address);
         }
      }
           
      List<Binding> bindings = mappings.get(address);
      
      List<MessageReference> refs = new ArrayList<MessageReference>();
      
      if (bindings != null)
      {
         for (Binding binding: bindings)
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
   
//   public void routeFromCluster(final String address, final Message message) throws Exception
//   {     
//      List<Binding> bindings = mappings.get(address);
//      
//      for (Binding binding: bindings)
//      {
//         Queue queue = binding.getQueue();
//         
//         if (binding.getNodeID() == nodeID)
//         {         
//            if (queue.getFilter() == null || queue.getFilter().match(message))
//            {         
//               MessageReference ref = message.createReference(queue);
//
//               //We never route durably from other nodes - so no need to persist
//
//               queue.addLast(ref);             
//            }
//         }
//      }
//   }

   public Map<SimpleString, List<Binding>> getMappings()
   {
      return mappings;
   }

   public FlowController getFlowController(SimpleString address)
   {   	
   	return flowControllers.get(address);
   }
   
   public void setBackup(final boolean backup)
   {
      this.backup = backup;
      
      for (Binding binding: nameMap.values())
      {
         binding.getQueue().setBackup(backup);
      }
   }

   // Private -----------------------------------------------------------------
   
   private Binding createBinding(final SimpleString address, final SimpleString name, final Filter filter,
                                 final boolean durable) throws Exception
   {
      Queue queue = queueFactory.createQueue(-1, name, filter, durable);
      
      queue.setBackup(backup);
      
      Binding binding = new BindingImpl(address, queue);
      
      managementService.registerQueue(queue, address, storageManager);

      return binding;
   }
   
   private void addBindingInMemory(final Binding binding) throws Exception
   {              
      List<Binding> bindings = new CopyOnWriteArrayList<Binding>();
      
      List<Binding> prevBindings = mappings.putIfAbsent(binding.getAddress(), bindings);
      
      if (prevBindings != null)
      {
         bindings = prevBindings;
      } else
      {
         managementService.registerAddress(binding.getAddress());
      }
                     
      managementService.registerQueue(binding.getQueue(), binding.getAddress(), storageManager);

      bindings.add(binding);  

      if (nameMap.putIfAbsent(binding.getQueue().getName(), binding) != null)
      {
         throw new IllegalStateException("Binding already exists " + binding);
      }     
      
      FlowController flowController = flowControllers.get(binding.getAddress());
                    
      binding.getQueue().setFlowController(flowController);
   }
   
   private Binding removeQueueInMemory(final SimpleString queueName) throws Exception
   {
      Binding binding = nameMap.remove(queueName);
      
      if (binding == null)
      {
         throw new IllegalStateException("Queue is not bound " + queueName);
      }
                  
      List<Binding> bindings = mappings.get(binding.getAddress());
                  
      for (Iterator<Binding> iter = bindings.iterator(); iter.hasNext();)
      {
         Binding b = iter.next();
         
         if (b.getQueue().getName().equals(queueName))
         {
            binding = b;
                                          
            break;
         }
      }
      
      if (binding == null)
      {
         throw new IllegalStateException("Cannot find binding " + queueName);
      }
      
      bindings.remove(binding);      
      
      if (bindings.isEmpty())
      {
         mappings.remove(binding.getAddress());
                           
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
                       
      //Destinations must be added first to ensure flow controllers exist before queues are created
      for (SimpleString destination: dests)
      {
         addDestination(destination, true);
      }
                	
      Map<Long, Queue> queues = new HashMap<Long, Queue>();
      
      for (Binding binding: bindings)
      {
         addBindingInMemory(binding);             
         
         queues.put(binding.getQueue().getPersistenceID(), binding.getQueue());
      }
                 
      storageManager.loadMessages(this, queues);
   }

}
