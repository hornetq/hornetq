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
package org.jboss.messaging.core.plugin.exchange;

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

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jms.selector.Selector;
import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.local.MessageQueue;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.JDBCServiceSupport;
import org.jboss.messaging.core.plugin.contract.Exchange;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;
import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.WriterPreferenceReadWriteLock;

/**
 * 
 * A ExchangeSupport

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public abstract class ExchangeSupport extends JDBCServiceSupport implements Exchange
{
   private static final Logger log = Logger.getLogger(ExchangeSupport.class);
           
   private String exchangeName;
   
   private QueuedExecutorPool queuedExecutorPool;
   
   private IdManager idManager;
   
   protected ReadWriteLock lock;
   
   protected MessageStore ms;     
   
   protected String nodeId;
   
   //Map <node id, Map < queue name, binding > >
   protected Map nameMaps;
   
   //Map <condition, List <binding> >
   protected Map conditionMap;
   
   public ExchangeSupport() throws Exception
   { 
      init();
   }     
   
   /*
    * This constructor should only be used for testing
    */
   protected ExchangeSupport(DataSource ds, TransactionManager tm)
   {
      super(ds, tm);  
      
      init();
   }
   
   protected void injectAttributes(String exchangeName, String nodeID,
                                   MessageStore ms, IdManager im, QueuedExecutorPool pool)
      throws Exception
   {
            
      this.exchangeName = exchangeName;
      
      this.nodeId = nodeID;
      
      this.ms = ms;
      
      this.idManager = im;
      
      this.queuedExecutorPool = pool;            
   }
   
   // ServiceMBeanSupport overrides ---------------------------------
   
   protected void startService() throws Exception
   {
      super.startService();
       
      loadBindings();
   }
   
   // Exchange implementation ---------------------------------------        
      
   public Binding bindQueue(String queueName, String condition, Filter filter, boolean noLocal, boolean durable,
                            MessageStore ms, PersistenceManager pm,
                            int fullSize, int pageSize, int downCacheSize) throws Exception
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
         
         long id = idManager.getId();         
         
         //Create the message queue
         MessageQueue queue = new MessageQueue(id, ms, pm, 
                                               true, durable,
                                               fullSize, pageSize, downCacheSize,
                                               (QueuedExecutor)queuedExecutorPool.get(),
                                               filter);
            
         binding = new SimpleBinding(nodeId, queueName, condition, filter == null ? null : filter.getFilterString(),
                                     noLocal, queue.getChannelID(), durable);         
         
         binding.setQueue(queue);
         
         binding.activate();
         
         if (durable)
         {
            queue.load();
         }
         
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
         
         binding.getQueue().removeAllReferences();
         
         return binding;     
      }
      finally
      {
         lock.writeLock().release();
      }
   }
   
   
   public void reloadQueues(String wildcard,
                            MessageStore ms, PersistenceManager pm,
                            int fullSize, int pageSize, int downCacheSize) throws Exception
   {
      if (wildcard == null)
      {
         throw new IllegalArgumentException("Wildcard is null");
      }
        
      lock.writeLock().acquire();
      
      try
      {
         //We deploy the queues of any bindings whose condition matches the wildcard
         //And we activate the bindings
         
         List matches = listMatchingBindings(wildcard);
         
         Iterator iter = matches.iterator();
         
         while (iter.hasNext())
         {           
            Binding binding = (Binding)iter.next();
            
            if (binding.isActive())
            {
               //Already active - do nothing
            }
            else
            {
               long id = binding.getChannelId();
                              
               QueuedExecutor executor = (QueuedExecutor)queuedExecutorPool.get();
               
               Filter filter = createFilter(binding.getSelector());
               
               //Now create the queue
               MessageQueue queue = new MessageQueue(id, ms, pm, true, binding.isDurable(),
                                                     fullSize, pageSize, downCacheSize, executor, filter);
               
               if (binding.isDurable())
               {
                  queue.load();
               }
               
               binding.setQueue(queue);    
               
               //Activate it
               binding.activate();               
            }            
         }      
      }
      finally
      {
         lock.writeLock().release();
      }      
   }
        
   public void unloadQueues(String wildcard) throws Exception
   {
      if (wildcard == null)
      {
         throw new IllegalArgumentException("Wildcard is null");
      }
      
      lock.writeLock().acquire();
      
      try
      {
         List matches = listMatchingBindings(wildcard);
         
         //We undeploy the queues of any bindings whose condition matches the wildcard    
         //And also deactivate the binding
         Iterator iter = matches.iterator();
         
         while (iter.hasNext())
         {
            Binding binding = (Binding)iter.next();
               
            if (binding.isActive())
            {
               //deactivate it
               binding.deactivate();
               
               //unload the queue from memory
               binding.setQueue(null);              
            }
            else
            {
               //Already deactivated - do nothing
            }            
         }      
      }
      finally
      {
         lock.writeLock().release();
      }      
   }
   
   
   public List listBindingsForWildcard(String wildcard) throws Exception
   {
      if (wildcard == null)
      {
         throw new IllegalArgumentException("Wildcard is null");
      }
      
      lock.writeLock().acquire();
      
      try
      {
         return listMatchingBindings(wildcard);
      }
      finally
      {
         lock.writeLock().release();
      }
   }
   
   public Binding getBindingForName(String queueName) throws Exception
   {    
      if (queueName == null)
      {
         throw new IllegalArgumentException("Queue name is null");
      }
      
      lock.writeLock().acquire();
      
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
         lock.writeLock().release();
      }
   }
     
   // Protected -----------------------------------------------------
   
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
          
         ps.setString(1, this.exchangeName);
         ps.setString(2, this.nodeId);
         ps.setString(3, binding.getQueueName());
         ps.setString(4, binding.getCondition());         
         ps.setString(5, binding.getSelector());
         ps.setString(6, binding.isNoLocal() ? "Y" : "N");
         ps.setLong(7, binding.getChannelId());

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
         
         ps.setString(1, this.exchangeName);
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
         
         ps = conn.prepareStatement(this.getSQLStatement("LOAD_BINDINGS"));
         
         ps.setString(1, this.exchangeName);

         rs = ps.executeQuery();
              
         while (rs.next())
         {
            String nodeId = rs.getString(1);
            
            String queueName = rs.getString(2);
            
            String condition = rs.getString(3);
            
            String selector = rs.getString(4);
            
            boolean noLocal = rs.getString(5).equals("Y");
            
            long channelId = rs.getLong(6);
              
            //We don't load the actual queue - this is because we don't know the paging params until
            //activation time
                    
            Binding binding = new SimpleBinding(nodeId, queueName, condition, selector, noLocal, channelId, true);
            
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
            
      List bindings = (List)conditionMap.get(condition);
      
      if (bindings == null)
      {
         bindings = new ArrayList();
         
         conditionMap.put(condition, bindings);
      }
      
      bindings.add(binding);
   }   
   
   protected Binding removeBinding(String nodeId, String queueName)
   {
      if (queueName == null)
      {
         throw new IllegalArgumentException("Queue name is null");
      }
             
      Map nameMap = (Map)nameMaps.get(nodeId);
      
      Binding binding = null;
      
      if (nameMap != null)
      {
         binding = (Binding)nameMap.remove(queueName);
      }
      
      if (binding == null)
      {
         throw new IllegalStateException("Name map does not contain binding for " + queueName);
      }
      
      if (nameMap.isEmpty())
      {
         nameMaps.remove(nodeId);
      }
      
      if (binding.getQueue() == null && nodeId.equals(this.nodeId))
      {
         //Otherwise the data won't get deleted from the database
         throw new IllegalStateException("Need to reload the queue before unbinding");
      }
      
      List bindings = (List)conditionMap.get(binding.getCondition());
      
      if (bindings == null)
      {
         throw new IllegalStateException("Cannot find condition bindings for " + binding.getCondition());
      }
      
      boolean removed = bindings.remove(binding);
      
      if (!removed)
      {
         throw new IllegalStateException("Cannot find binding in condition binding list");
      }
      
      if (binding == null)
      {
         throw new IllegalStateException("Channel id map does not contain binding for " + binding.getChannelId());
      }
      
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
   
   // PersistentServiceSupport overrides ----------------------------
   
   protected Map getDefaultDMLStatements()
   {                
      Map map = new HashMap();
      map.put("INSERT_BINDING",
               "INSERT INTO JMS_EXCHANGE_BINDING (EXCHANGE_NAME, NODE_ID, QUEUE_NAME, CONDITION, SELECTOR, NOLOCAL, CHANNEL_ID) " +
               "VALUES (?, ?, ?, ?, ?, ?, ?)");
      map.put("DELETE_BINDING",
              "DELETE FROM JMS_EXCHANGE_BINDING WHERE EXCHANGE_NAME=? AND NODE_ID=? AND QUEUE_NAME=?");
      map.put("LOAD_BINDINGS",
              "SELECT NODE_ID, QUEUE_NAME, CONDITION, SELECTOR, NOLOCAL, CHANNEL_ID FROM JMS_EXCHANGE_BINDING " +
              "WHERE EXCHANGE_NAME  = ?");
      return map;
   }
   
   protected Map getDefaultDDLStatements()
   {
      Map map = new HashMap();
      map.put("CREATE_BINDING_TABLE",
              "CREATE TABLE JMS_EXCHANGE_BINDING (EXCHANGE_NAME VARCHAR(256), NODE_ID VARCHAR(256)," +
              "QUEUE_NAME VARCHAR(1024), CONDITION VARCHAR(1024), " +
              "SELECTOR VARCHAR(1024), NOLOCAL CHAR(1), CHANNEL_ID BIGINT)");
      return map;
   }
          
   // Private -------------------------------------------------------
              
   private Filter createFilter(String selector) throws Exception
   {
      //We should abstract this somehow so we don't have this dependency on the jms selector
      
      if (selector == null)
      {
         return null;
      }
      else
      {
         return new Selector(selector);
      }
   }
   
   /*
    * List all bindings whose condition matches the wildcard
    * Initially we just do an exact match - when we support topic hierarchies this
    * will change
    */
   private List listMatchingBindings(String wildcard)
   {      
      List list = (List)conditionMap.get(wildcard);
      
      if (list == null)
      {
         return Collections.EMPTY_LIST;
      }
      else
      {
         return list;
      }
   }
   
   private void init()
   {
      lock = new WriterPreferenceReadWriteLock();
      
      nameMaps = new LinkedHashMap();
       
      conditionMap = new LinkedHashMap();   
   }
                  
   // Inner classes -------------------------------------------------            
}
