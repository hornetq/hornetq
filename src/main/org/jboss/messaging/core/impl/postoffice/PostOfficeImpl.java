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
package org.jboss.messaging.core.impl.postoffice;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.TextMessage;

import org.jboss.jms.message.JBossMessage;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Binding;
import org.jboss.messaging.core.Condition;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.PostOffice;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.QueueFactory;
import org.jboss.messaging.core.TransactionSynchronization;
import org.jboss.messaging.core.impl.BindingImpl;
import org.jboss.messaging.util.ConcurrentHashSet;

/**
 * 
 * A PostOfficeImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class PostOfficeImpl implements PostOffice
{  
   private static final Logger log = Logger.getLogger(PostOfficeImpl.class);
   
   private int nodeID;
   
  // private Map<Integer, Map<String, Queue>> queues = new HashMap<Integer, Map<String, Queue>>();
   
   private Map<Condition, List<Binding>> mappings = new ConcurrentHashMap<Condition, List<Binding>>();
   
   private Set<Condition> conditions = new ConcurrentHashSet<Condition>();
   
   private PersistenceManager persistenceManager;
   
   private QueueFactory queueFactory;
    
   public PostOfficeImpl(int nodeID, PersistenceManager persistenceManager, QueueFactory queueFactory)
   {
      this.nodeID = nodeID;
      
      this.persistenceManager = persistenceManager;
      
      this.queueFactory = queueFactory;
   }
      
   // MessagingComponent implementation ---------------------------------------
   
   public void start() throws Exception
   {
      loadBindings();
   }

   public void stop() throws Exception
   {
      mappings.clear();
      
      conditions.clear();
   }
   
   // PostOffice implementation -----------------------------------------------

   public Queue addQueue(Condition condition, String name, Filter filter, 
                         boolean durable, boolean temporary, boolean allNodes) throws Exception
   {
      Binding binding = createBinding(condition, name, filter, durable, temporary, allNodes);
      
      addBindingInMemory(binding);
       
      if (durable)
      {
         persistenceManager.addBinding(binding);
      }
      
      return binding.getQueue();
   }
         
   public boolean removeQueue(Condition condition, String name, boolean allNodes) throws Exception
   {
      Binding binding = removeQueueInMemory(condition, name);
      
      if (binding != null)
      {
         if (binding.getQueue().isDurable())
         {
            persistenceManager.deleteBinding(binding);
         }
         
         return true;
      }
      else
      {
         return false;
      }            
   }
   
   public void addCondition(Condition condition)
   {      
      conditions.add(condition);
   }
   
   public boolean removeCondition(Condition condition)
   {      
      return conditions.remove(condition);
   }
   
   public boolean containsCondition(Condition condition)
   {
      return conditions.contains(condition);
   }
    
   public void route(Condition condition, Message message) throws Exception
   {
     // boolean routeRemote = false;
           
      List<Binding> bindings = mappings.get(condition);
      
      if (bindings != null)
      {
         for (Binding binding: bindings)
         {
            Queue queue = binding.getQueue();
            
            if (queue.getFilter() == null || queue.getFilter().match(message))
            {         
               if (binding.getNodeID() == nodeID)
               {
                  //Local queue
                                 
                  message.createReference(queue);              
               }
               else
               {
//                  if (!queue.isDurable())
//                  {
//                     //Remote queue - we never route to remote durable queues since we will lose atomicity in event
//                     //of crash - for moving between durable queues we use message redistribution
//                     
//                     routeRemote = true;                  
//                  }               
               }
            }
         }
      }

      
//      if (routeRemote)
//      {
//         tx.addSynchronization(new CastMessageCallback(new MessageRequest(condition, message)));
//      }
   }
   
   public void routeFromCluster(Condition condition, Message message) throws Exception
   {     
      List<Binding> bindings = mappings.get(condition);
      
      for (Binding binding: bindings)
      {
         Queue queue = binding.getQueue();
         
         if (binding.getNodeID() == nodeID)
         {         
            if (queue.getFilter() == null || queue.getFilter().match(message))
            {         
               MessageReference ref = message.createReference(queue);

               //We never route durably from other nodes - so no need to persist

               queue.addLast(ref);             
            }
         }
      }
   }

   public Map<Condition, List<Binding>> getMappings()
   {
      return mappings;
   }
   
   public List<Binding> getBindingsForQueueName(String queueName)
   {
      List<Binding> list = new ArrayList<Binding>();
      
      for (List<Binding> bindings: mappings.values())
      {
         for (Binding binding: bindings)
         {
            if (binding.getQueue().getName().equals(queueName) && binding.getNodeID() == nodeID)
            {
               list.add(binding);
            }
         }
      }
      return list;
   }
   
   public List<Binding> getBindingsForCondition(Condition condition)
   {
      List<Binding> list = new ArrayList<Binding>();
      
      List<Binding> bindings = mappings.get(condition);
      
      if (bindings != null)
      {
         for (Binding binding: bindings)
         {
            if (binding.getNodeID() == nodeID)
            {
               list.add(binding);
            }
         }
      }         
         
      return list;
   }
   
   // Private -----------------------------------------------------------------
   
   private Binding createBinding(Condition condition, String name, Filter filter,
                                 boolean durable, boolean temporary, boolean allNodes)
   {
      Queue queue = queueFactory.createQueue(-1, name, filter, durable, temporary);
      
      Binding binding = new BindingImpl(this.nodeID, condition, queue, allNodes);
      
      return binding;
   }
   
   private void addBindingInMemory(Binding binding) throws Exception
   {      
      List<Binding> bindings = mappings.get(binding.getCondition());
      
      if (bindings == null)
      {
         bindings = new CopyOnWriteArrayList<Binding>();
         
         mappings.put(binding.getCondition(), bindings);
      }
      
      bindings.add(binding);
      
   //      Map<String, Queue> nameMap = queues.get(nodeID);
   //      
   //      if (nameMap == null)
   //      {
   //         nameMap = new HashMap<String, Queue>();
   //         
   //         queues.put(nodeID, nameMap);
   //      }
   //      
   //      nameMap.put(name, queue);
   }
   
   private Binding removeQueueInMemory(Condition condition, String name) throws Exception
   {
      Binding binding = null;
      
   //      Map<String, Queue> nameMap = queues.get(nodeID);
   //      
   //      nameMap.remove(name);
   //      
   //      if (nameMap.isEmpty())
   //      {
   //         queues.remove(nodeID);
   //      }
             
      List<Binding> bindings = mappings.get(condition);
                  
      for (Iterator<Binding> iter = bindings.iterator(); iter.hasNext();)
      {
         Binding b = iter.next();
         
         if (b.getQueue().getName().equals(name))
         {
            binding = b;
                                          
            break;
         }
      }
      
      if (binding != null)
      {
         bindings.remove(binding);
      }
      
      if (bindings.isEmpty())
      {
         mappings.remove(condition);
      }
         
      return binding;
   }
   
   private void loadBindings() throws Exception
   {
      List<Binding> bindings = persistenceManager.loadBindings(queueFactory);
      
      for (Binding binding: bindings)
      {
         addBindingInMemory(binding);                    
      }
   }
   
   private void deleteMappingsForNode(int theNodeID)
   {
      
   }
   
   private class CastMessageCallback implements TransactionSynchronization
   {
      private ClusterRequest request;
      
      CastMessageCallback(ClusterRequest request)
      {
         this.request = request;
      }
      
      public void afterCommit() throws Exception
      {
         //TODO - cast request
      }

      public void afterRollback() throws Exception
      { 
      }

      public void beforeCommit() throws Exception
      {
      }

      public void beforeRollback() throws Exception
      {
      } 
   }
}
