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
package org.jboss.messaging.core.plugin.postoffice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.plugin.JDBCSupport;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;

import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.WriterPreferenceReadWriteLock;

/**
 * 
 * A PostOfficeImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class PostOfficeImpl extends JDBCSupport implements PostOffice
{
   private static final Logger log = Logger.getLogger(PostOfficeImpl.class);
          
   private String officeName;
   
   protected ReadWriteLock lock;
   
   protected MessageStore ms;     
   
   protected TransactionRepository tr;
   
   protected String nodeId;
   
   //Map <node id, Map < queue name, binding > >
   protected Map nameMaps;
   
   //Map <condition, List <binding> >
   protected Map conditionMap;
     
   public PostOfficeImpl()
   {      
   }
   
   public PostOfficeImpl(DataSource ds, TransactionManager tm, Properties sqlProperties,
                         boolean createTablesOnStartup,
                         String nodeId, String officeName, MessageStore ms,
                         TransactionRepository tr)
   {            
      super (ds, tm, sqlProperties, createTablesOnStartup);
      
      lock = new WriterPreferenceReadWriteLock();
      
      nameMaps = new LinkedHashMap();
       
      conditionMap = new LinkedHashMap(); 
      
      this.nodeId = nodeId;
      
      this.officeName = officeName;
      
      this.ms = ms;
      
      this.tr = tr;
   }
   
   // MessagingComponent implementation --------------------------------
   
   public void start() throws Exception
   {
      super.start();
      
      loadBindings();
   }
   
   public void stop() throws Exception
   {
      super.stop();
   }
     
   // PostOffice implementation ---------------------------------------        
         
   public Binding bindQueue(String queueName, String condition, Queue queue) throws Exception
   {
      if (queueName == null)
      {
         throw new IllegalArgumentException("Queue name is null");
      }
      
      if (condition == null)
      {
         throw new IllegalArgumentException("Condition is null");
      }
      
      lock.writeLock().acquire();

      try
      {         
         //We currently only allow one binding per name per node
         Map nameMap = (Map)nameMaps.get(this.nodeId);
         
         Binding binding = null;
         
         if (nameMap != null)
         {
            binding = (Binding)nameMap.get(queueName);
         }
         
         if (binding != null)
         {
            throw new IllegalArgumentException("Binding already exists for name " + queueName);
         }
         
         boolean durable = queue.isRecoverable();
         
         String filter = queue.getFilter() == null ? null : queue.getFilter().getFilterString();
                    
         binding = createBinding(nodeId, queueName, condition, filter,
                                   queue.getChannelID(), durable);         
         
         binding.setQueue(queue);
         
         binding.activate();
         
         addBinding(binding);
               
         if (durable)
         {
            //Need to write the binding to the db            
            insertBinding(binding);       
         }
                           
         return binding;   
      }
      finally
      {
         lock.writeLock().release();
      }
   }   
            
   public Binding unbindQueue(String queueName) throws Throwable
   {
      if (queueName == null)
      {
         throw new IllegalArgumentException("Queue name is null");
      }
      
      lock.writeLock().acquire();
      
      try
      {         
         Binding binding = removeBinding(this.nodeId, queueName);
      
         if (binding.isDurable())
         {
            //Need to remove from db too
            
            deleteBinding(binding.getQueueName());                        
         }
         
         if (binding.getQueue() != null)
         {
            binding.getQueue().removeAllReferences();
         }
         
         return binding;     
      }
      finally
      {
         lock.writeLock().release();
      }
   }   
   
   public List listBindingsForCondition(String condition) throws Exception
   {
      if (condition == null)
      {
         throw new IllegalArgumentException("Condition is null");
      }
      
      lock.readLock().acquire();
      
      try
      {
         ConditionBindings cb = (ConditionBindings)conditionMap.get(condition);
                  
         if (cb == null)
         {
            return Collections.EMPTY_LIST;
         }
         else
         {
            List list = cb.getAllBindings();                        
            
            return list;
         }
      }
      finally
      {
         lock.readLock().release();
      }
   }
   
   public Binding getBindingForQueueName(String queueName) throws Exception
   {    
      if (queueName == null)
      {
         throw new IllegalArgumentException("Queue name is null");
      }
      
      lock.readLock().acquire();
      
      try
      {
         Map nameMap = (Map)nameMaps.get(this.nodeId);
         
         Binding binding = null;
         
         if (nameMap != null)
         {
            binding = (Binding)nameMap.get(queueName);
         }
         
         return binding;
      }
      finally
      {
         lock.readLock().release();
      }
   }
   
   public void recover() throws Exception
   {
      //NOOP
   }
   
   public boolean route(MessageReference ref, String condition, Transaction tx) throws Exception
   {
      if (ref == null)
      {
         throw new IllegalArgumentException("Message reference is null");
      }
      
      if (condition == null)
      {
         throw new IllegalArgumentException("Condition key is null");
      }
      
      boolean routed = false;
      
      lock.readLock().acquire();
                
      try
      {                 
         ConditionBindings cb = (ConditionBindings)conditionMap.get(condition);
                             
         if (cb != null)
         {            
            boolean startInternalTx = false;
            
            if (tx == null && ref.isReliable())
            {
               if (cb.getDurableCount() > 1)
               {
                  // When routing a persistent message without a transaction then we may need to start an 
                  // internal transaction in order to route it.
                  // This is so we can guarantee the message is delivered to all or none of the subscriptions.
                  // We need to do this if there is more than one durable sub
                  startInternalTx = true;
               }
            }
            
            if (startInternalTx)
            {
               tx = tr.createTransaction();
            }
                        
            List bindings = cb.getAllBindings();
            
            Iterator iter = bindings.iterator();
            
            while (iter.hasNext())
            {
               Binding binding = (Binding)iter.next();
                 
               if (binding.isActive() && binding.getNodeId().equals(this.nodeId))
               {
                  //It's a local binding so we pass the message on to the subscription
                  Queue subscription = binding.getQueue();
               
                  Delivery del = subscription.handle(null, ref, tx);
                  
                  if (del != null && del.isSelectorAccepted())
                  {
                     routed = true;
                  }                  
               }               
            } 
            
            if (startInternalTx)
            {
               //TODO - do we need to rollback if an exception is thrown??
               tx.commit();
            }
         }
                 
         return routed;
      }
      finally
      {                  
         lock.readLock().release();
      }
   } 
     
   // Protected -----------------------------------------------------
   
   protected Binding createBinding(String nodeId, String queueName, String condition, String filter,
                                   long channelId, boolean durable)
   {
      return new BindingImpl(nodeId, queueName, condition, filter,
                             channelId, durable);   
   }
   
   protected void loadBindings() throws Exception
   {
      lock.writeLock().acquire();
      
      try
      {
         List list = getBindings();
         
         Iterator iter = list.iterator();
         
         while (iter.hasNext())
         {
            Binding binding = (Binding)iter.next();
            
            addBinding(binding);              
         }
      }
      finally
      {
         lock.writeLock().release();
      }
   }
         
   protected void insertBinding(Binding binding) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps  = null;     
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(getSQLStatement("INSERT_BINDING"));
                  
         ps.setString(1, this.officeName);
         ps.setString(2, this.nodeId);
         ps.setString(3, binding.getQueueName());
         ps.setString(4, binding.getCondition());         
         ps.setString(5, binding.getSelector());
         ps.setLong(6, binding.getChannelId());

         ps.executeUpdate();;
      }
      finally
      {
         if (ps != null)
         {
            ps.close();
         }
         if (conn != null)
         {
            conn.close();
         }
         wrap.end();
      }     
   }
   
   protected boolean deleteBinding(String queueName) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps  = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();

         ps = conn.prepareStatement(getSQLStatement("DELETE_BINDING"));
         
         ps.setString(1, this.officeName);
         ps.setString(2, this.nodeId);
         ps.setString(3, queueName);

         int rows = ps.executeUpdate();
         
         return rows == 1;
      }
      finally
      {
         if (ps != null)
         {
            ps.close();
         }
         if (conn != null)
         {
            conn.close();
         }
         wrap.end();
      }     
   }

   /*
    * Note that we get all bindings irrespective of which node this represents.
    * This is because persistent messages are always persisted in durable subscriptions on
    * 
    */
   protected List getBindings() throws Exception
   {
      Connection conn = null;
      PreparedStatement ps  = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      List list = new ArrayList();
      
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(getSQLStatement("LOAD_BINDINGS"));
                 
         ps.setString(1, this.officeName);

         rs = ps.executeQuery();
              
         while (rs.next())
         {
            String nodeId = rs.getString(1);
            
            String queueName = rs.getString(2);
            
            String condition = rs.getString(3);
            
            String selector = rs.getString(4);
            
            long channelId = rs.getLong(5);
              
            //We don't load the actual queue - this is because we don't know the paging params until
            //activation time
                    
            Binding binding = createBinding(nodeId, queueName, condition, selector, channelId, true);
            
            list.add(binding);
         }
         
         return list;
      }
      finally
      {
         if (rs != null)
         {
            rs.close();
         }
         if (ps != null)
         {
            ps.close();
         }
         if (conn != null)
         {
            conn.close();
         }
         wrap.end();
      }     
   }
   
   protected void addBinding(Binding binding)
   {
      Map nameMap = (Map)nameMaps.get(binding.getNodeId());
      
      if (nameMap == null)
      {
         nameMap = new LinkedHashMap();
         
         nameMaps.put(binding.getNodeId(), nameMap);
      }
      
      nameMap.put(binding.getQueueName(), binding);
      
      String condition = binding.getCondition();
            
      ConditionBindings bindings = (ConditionBindings)conditionMap.get(condition);
      
      if (bindings == null)
      {
         bindings = new ConditionBindings(this.nodeId);
         
         conditionMap.put(condition, bindings);
      }
      
      bindings.addBinding(binding);
   }   
   
   protected Binding removeBinding(String nodeId, String queueName)
   {
      if (queueName == null)
      {
         throw new IllegalArgumentException("Queue name is null");
      }
             
      Map nameMap = (Map)nameMaps.get(nodeId);
      
      if (nameMap == null)
      {
         throw new IllegalArgumentException("Cannot find any bindings for node Id: " + nodeId);
      }
      
      Binding binding = null;            
      
      if (nameMap != null)
      {
         binding = (Binding)nameMap.remove(queueName);
      }
      
      if (binding == null)
      {
         throw new IllegalArgumentException("Name map does not contain binding for " + queueName);
      }
              
      if (nameMap.isEmpty())
      {
         nameMaps.remove(nodeId);
      }
                  
      ConditionBindings bindings = (ConditionBindings)conditionMap.get(binding.getCondition());
      
      if (bindings == null)
      {
         throw new IllegalStateException("Cannot find condition bindings for " + binding.getCondition());
      }
      
      boolean removed = bindings.removeBinding(binding);
      
      if (!removed)
      {
         throw new IllegalStateException("Cannot find binding in condition binding list");
      }           
      
      if (bindings.isEmpty())
      {
         conditionMap.remove(binding.getCondition());
      }        
      
      return binding;
   }
   
   protected Map getDefaultDMLStatements()
   {                
      Map map = new HashMap();
      map.put("INSERT_BINDING",
               "INSERT INTO JMS_POSTOFFICE (POSTOFFICE_NAME, NODE_ID, QUEUE_NAME, CONDITION, SELECTOR, CHANNEL_ID) " +
               "VALUES (?, ?, ?, ?, ?, ?)");
      map.put("DELETE_BINDING",
              "DELETE FROM JMS_POSTOFFICE WHERE POSTOFFICE_NAME=? AND NODE_ID=? AND QUEUE_NAME=?");
      map.put("LOAD_BINDINGS",
              "SELECT NODE_ID, QUEUE_NAME, CONDITION, SELECTOR, CHANNEL_ID FROM JMS_POSTOFFICE " +
              "WHERE POSTOFFICE_NAME  = ?");
      return map;
   }
   
   protected Map getDefaultDDLStatements()
   {
      Map map = new HashMap();
      map.put("CREATE_POSTOFFICE_TABLE",
              "CREATE TABLE JMS_POSTOFFICE (POSTOFFICE_NAME VARCHAR(256), NODE_ID VARCHAR(256)," +
              "QUEUE_NAME VARCHAR(1024), CONDITION VARCHAR(1024), " +
              "SELECTOR VARCHAR(1024), CHANNEL_ID BIGINT)");
      return map;
   }
   
   // Private -------------------------------------------------------             
                 
   // Inner classes -------------------------------------------------            
}
