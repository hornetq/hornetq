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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.message.SimpleMessageReference;
import org.jboss.messaging.core.plugin.JDBCSupport;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.contract.Condition;
import org.jboss.messaging.core.plugin.contract.ConditionFactory;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;

import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.ReentrantWriterPreferenceReadWriteLock;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class DefaultPostOffice extends JDBCSupport implements PostOffice
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(DefaultPostOffice.class);

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   protected MessageStore ms;
   protected PersistenceManager pm;
   protected TransactionRepository tr;
   protected FilterFactory filterFactory;
   protected ConditionFactory conditionFactory;
   protected int currentNodeId;

   // Map <NodeID, Map<queueName, Binding>>
   protected Map nameMaps;

   // Map <Condition, Bindings>
   protected Map conditionMap;

   // this lock protects the condition and name maps
   protected ReadWriteLock lock;

   private String officeName;

   // Constructors ---------------------------------------------------------------------------------

   public DefaultPostOffice()
   {
   }

   public DefaultPostOffice(DataSource ds, TransactionManager tm, Properties sqlProperties,
                         boolean createTablesOnStartup,
                         int nodeId, String officeName, MessageStore ms,
                         PersistenceManager pm,
                         TransactionRepository tr, FilterFactory filterFactory,
                         ConditionFactory conditionFactory)
   {
      super (ds, tm, sqlProperties, createTablesOnStartup);

      lock = new ReentrantWriterPreferenceReadWriteLock();

      nameMaps = new LinkedHashMap();

      conditionMap = new LinkedHashMap();

      this.currentNodeId = nodeId;
      this.ms = ms;
      this.pm = pm;
      this.tr = tr;
      this.filterFactory = filterFactory;
      this.conditionFactory = conditionFactory;
      this.officeName = officeName;

   }

   // MessagingComponent implementation ------------------------------------------------------------

   public void start() throws Exception
   {
      if (trace) { log.trace(this + " starting"); }

      super.start();

      loadBindings();

      log.debug(this + " started");
   }

   public void stop() throws Exception
   {
      stop(true);
   }

   public void stop(boolean sendNotification) throws Exception
   {
      if (trace) { log.trace(this + " stopping"); }

      super.stop();

      log.debug(this + " stopped");
   }

   // PostOffice implementation --------------------------------------------------------------------

   public String getOfficeName()
   {
      return officeName;
   }

   public Binding bindQueue(Condition condition, Queue queue) throws Exception
   {
      if (trace) { log.trace(this + " binding queue " + queue.getName() + " with condition " + condition); }

      if (queue.getName() == null)
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
         // We currently only allow one binding per name per node
         Map nameMap = (Map)nameMaps.get(new Integer(currentNodeId));

         Binding binding = null;

         if (nameMap != null)
         {
            binding = (Binding)nameMap.get(queue.getName());
         }

         if (binding != null)
         {
            throw new IllegalArgumentException("Binding already exists for name " + queue.getName());
         }

         binding = new DefaultBinding(currentNodeId, condition, queue, false);

         addBinding(binding);

         if (queue.isRecoverable())
         {
            // Need to write the binding to the database
            insertBinding(binding);
         }

         return binding;
      }
      finally
      {
         lock.writeLock().release();
      }
   }

   public Binding unbindQueue( String queueName) throws Throwable
   {
      if (trace) { log.trace(this + " unbinding queue " + queueName); }

      if (queueName == null)
      {
         throw new IllegalArgumentException("Queue name is null");
      }

      lock.writeLock().acquire();

      try
      {
         Binding binding = removeBinding(currentNodeId,queueName);

         if (binding.getQueue().isRecoverable())
         {
            //Need to remove from db too

            deleteBinding(currentNodeId, binding.getQueue().getName());
         }

         binding.getQueue().removeAllReferences();

         return binding;
      }
      finally
      {
         lock.writeLock().release();
      }
   }

   public Collection getBindingsForCondition(Condition condition) throws Exception
   {
      return listBindingsForConditionInternal(condition, true);
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
         return internalGetBindingForQueueName(queueName);
      }
      finally
      {
         lock.readLock().release();
      }
   }

   public boolean route(MessageReference ref, Condition condition, Transaction tx) throws Exception
   {
      if (trace) { log.trace(this + " routing " + ref + " with condition '" + condition + "' " + (tx == null ? "non-transactionally" : " in " + tx)); }

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
         Bindings bd = (Bindings)conditionMap.get(condition);

         if (bd != null)
         {
            boolean startInternalTx = false;

            if (tx == null && ref.getMessage().isReliable())
            {
               if (bd.getDurableCount() > 1)
               {
                  // When routing a persistent message without a transaction then we may need to
                  // start an internal transaction in order to route it. This is so we can guarantee
                  // the message is delivered to all or none of the subscriptions. We need to do
                  // this if there is more than one durable subscription.
                  startInternalTx = true;
               }
            }

            if (startInternalTx)
            {
               tx = tr.createTransaction();
            }

            Collection bindings = bd.getAllBindings();

            Iterator iter = bindings.iterator();

            while (iter.hasNext())
            {
               Binding binding = (Binding)iter.next();

               //Sanity check
               if (binding.getNodeID() != this.currentNodeId)
               {
                  throw new IllegalStateException("Local post office has foreign bindings!");
               }

               Queue queue = binding.getQueue();

               Delivery del = queue.handle(null, ref, tx);

               if (del != null && del.isSelectorAccepted())
               {
                  routed = true;
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

   public boolean isLocal()
   {
      return true;
   }

   public Binding getBindingforChannelId(long channelId) throws Exception
   {
      lock.readLock().acquire();

      try
      {
         Map nameMap = (Map)nameMaps.get(new Integer(currentNodeId));

         if (nameMap == null)
         {
            throw new IllegalStateException("Cannot find name map for current node " + currentNodeId);
         }

         Binding binding = null;

         for (Iterator iterbindings = nameMap.values().iterator(); iterbindings.hasNext();)
         {
            Binding itemBinding = (Binding)iterbindings.next();

            if (itemBinding.getQueue().getChannelID() == channelId)
            {
               binding = itemBinding;
               break;
            }
         }

         return binding;
      }
      finally
      {
         lock.readLock().release();
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public String printBindingInformation()
   {
       StringWriter buffer = new StringWriter();
       PrintWriter out = new PrintWriter(buffer);

       out.println("Ocurrencies of nameMaps:");
       out.println("<table border=1>");
       for (Iterator mapIterator = nameMaps.entrySet().iterator();mapIterator.hasNext();)
       {
           Map.Entry entry = (Map.Entry)mapIterator.next();
           out.println("<tr><td colspan=3><b>Map on node " + entry.getKey() + "</b></td></tr>");
           Map valuesOnNode = (Map)entry.getValue();

           out.println("<tr><td>Key</td><td>Value</td><td>Class of Value</td></tr>");
           for (Iterator valuesIterator=valuesOnNode.entrySet().iterator();valuesIterator.hasNext();)
           {
               Map.Entry entry2 = (Map.Entry)valuesIterator.next();

               out.println("<tr>");
               out.println("<td>" + entry2.getKey() + "</td><td>" + entry2.getValue()+
                  "</td><td>" + entry2.getValue().getClass().getName() + "</td>");
               out.println("</tr>");

               if (entry2.getValue() instanceof Binding &&
                  ((Binding)entry2.getValue()).getQueue() instanceof PagingFilteredQueue)
               {
                   PagingFilteredQueue queue =
                      (PagingFilteredQueue)((Binding)entry2.getValue()).getQueue();
                   List undelivered = queue.undelivered(null);
                   if (!undelivered.isEmpty())
                   {
                       out.println("<tr><td>List of undelivered messages on Paging</td>");

                       out.println("<td colspan=2><table border=1>");
                       out.println("<tr><td>Reference#</td><td>Message</td></tr>");
                       for (Iterator i = undelivered.iterator();i.hasNext();)
                       {
                           SimpleMessageReference reference = (SimpleMessageReference)i.next();
                           out.println("<tr><td>" + reference.getInMemoryChannelCount() +
                              "</td><td>" + reference.getMessage() +"</td></tr>");
                       }
                       out.println("</table></td>");
                       out.println("</tr>");
                   }
               }
           }
       }

       out.println("</table>");
       out.println("<br>Ocurrencies of conditionMap:");
       out.println("<table border=1>");
       out.println("<tr><td>EntryName</td><td>Value</td>");

       for (Iterator iterConditions = conditionMap.entrySet().iterator();iterConditions.hasNext();)
       {
           Map.Entry entry = (Map.Entry)iterConditions.next();
           out.println("<tr><td>" + entry.getKey() + "</td><td>" + entry.getValue() + "</td></tr>");

           if (entry.getValue() instanceof Bindings)
           {
               out.println("<tr><td>Binding Information:</td><td>");
               out.println("<table border=1>");
               out.println("<tr><td>Binding</td><td>Queue</td></tr>");
               Bindings bindings = (Bindings)entry.getValue();
               for (Iterator i = bindings.getAllBindings().iterator();i.hasNext();)
               {

                   Binding binding = (Binding)i.next();
                   out.println("<tr><td>" + binding + "</td><td>" + binding.getQueue() +
                      "</td></tr>");
               }
               out.println("</table></td></tr>");
           }
       }
       out.println("</table>");

       return buffer.toString();
   }

   public String toString()
   {
      return "DefaultPostOffice[" + Integer.toHexString(hashCode()) + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   /**
    * Internal methods (e.g. failOver) will already hold a lock and will need to call
    * getBindingForQueueNames() without a lock. (Also... I dind't move this method to the protected
    * section of the code as this is related to getBindingForQueueNames).
    */
   protected Binding internalGetBindingForQueueName(String queueName)
   {
      Map nameMap = (Map)nameMaps.get(new Integer(currentNodeId));

      Binding binding = null;

      if (nameMap != null)
      {
         binding = (Binding)nameMap.get(queueName);
      }

      return binding;
   }

   protected Collection listBindingsForConditionInternal(Condition condition, boolean localOnly)
      throws Exception
   {
      if (condition == null)
      {
         throw new IllegalArgumentException("Condition is null");
      }

      lock.readLock().acquire();

      try
      {
         //We should only list the bindings for the local node

         Bindings cb = (Bindings)conditionMap.get(condition);

         if (cb == null)
         {
            return Collections.EMPTY_LIST;
         }
         else
         {
            List list = new ArrayList();

            Collection bindings = cb.getAllBindings();

            Iterator iter = bindings.iterator();

            while (iter.hasNext())
            {
               Binding binding = (Binding)iter.next();

               if (!localOnly || (binding.getNodeID() == this.currentNodeId))
               {
                  list.add(binding);
               }
            }

            return list;
         }
      }
      finally
      {
         lock.readLock().release();
      }
   }

   // FIXME - this is not quite right
   // We should not load any bindings at startup - since then we do not have to create any queues
   // internally. Creating queues is problematic since there are params we do not know until
   // destination deploy time e.g. paging params, maxsize etc. This means we have to load the queues
   // disabled and then set the params and re-activate them which is not clean.
   protected void loadBindings() throws Exception
   {
      lock.writeLock().acquire();

      Connection conn = null;
      PreparedStatement ps  = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();

         ps = conn.prepareStatement(getSQLStatement("LOAD_BINDINGS"));

         ps.setString(1, officeName);

         rs = ps.executeQuery();

         while (rs.next())
         {
            int nodeID = rs.getInt(1);
            String queueName = rs.getString(2);
            String conditionText = rs.getString(3);
            String selector = rs.getString(4);

            if (rs.wasNull())
            {
               selector = null;
            }

            long channelID = rs.getLong(5);
            boolean failed = rs.getString(6).equals("Y");

            Integer failedNodeID = null;
            int i = rs.getInt(7);

            if(!rs.wasNull())
            {
               failedNodeID = new Integer(i);
            }

            Condition condition = conditionFactory.createCondition(conditionText);

            //Temp hack
            //For non clustered po don't want to load other nodes bindings!
            if (!(this instanceof ClusteredPostOffice) && (nodeID != this.currentNodeId))
            {
               //Don't load other nodes binding
            }
            else
            {
               Binding binding = createBinding(nodeID, condition, queueName, channelID,
                                               selector, true, failed, failedNodeID);

               log.debug(this + " loaded from database " + binding);

               binding.getQueue().deactivate();
               addBinding(binding);
            }
         }
      }
      finally
      {
         lock.writeLock().release();

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

   /**
    * @param failedNodeID - ignored for non-clustered bindings.
    */
   protected Binding createBinding(int nodeID, Condition condition, String queueName,
                                   long channelID, String filterString, boolean durable,
                                   boolean failed, Integer failedNodeID) throws Exception
   {
      Filter filter = filterFactory.createFilter(filterString);
      return createBinding(nodeID, condition, queueName, channelID,
                           filter, durable, failed, failedNodeID);
   }

   /**
    * @param failedNodeID - ignored for non-clustered bindings.
    */
   protected Binding createBinding(int nodeID, Condition condition, String queueName,
                                   long channelID, Filter filter, boolean durable,
                                   boolean failed, Integer failedNodeID)
   {
      Queue queue;

      if (nodeID == currentNodeId)
      {         
         queue =
            new PagingFilteredQueue(queueName, channelID, ms, pm, true, true, -1, filter);
      }
      else
      {
         throw new IllegalStateException("This is a non clustered post office - should not " +
            "have bindings from different nodes!");
      }

      return new DefaultBinding(nodeID, condition, queue, failed);
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

         String filterString =
            binding.getQueue().getFilter() == null ?
               null : binding.getQueue().getFilter().getFilterString();

         ps.setString(1, officeName);
         ps.setInt(2, currentNodeId);
         ps.setString(3, binding.getQueue().getName());
         ps.setString(4, binding.getCondition().toText());
         if (filterString != null)
         {
            ps.setString(5, filterString);
         }
         else
         {
            ps.setNull(5, Types.VARCHAR);
         }
         ps.setLong(6, binding.getQueue().getChannelID());
         ps.setString(7,binding.isFailed() ? "Y":"N");

         Integer failedNodeID = binding.getFailedNodeID();
         if (failedNodeID == null)
         {
            ps.setNull(8, Types.INTEGER);
         }
         else
         {
            ps.setInt(8, failedNodeID.intValue());
         }

         ps.executeUpdate();
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

   protected boolean deleteBinding(int parameterNodeId, String queueName) throws Exception
   {
      if (parameterNodeId<0) parameterNodeId=this.currentNodeId;
      Connection conn = null;
      PreparedStatement ps  = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();

         ps = conn.prepareStatement(getSQLStatement("DELETE_BINDING"));

         ps.setString(1, this.officeName);
         ps.setInt(2, parameterNodeId);
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

   protected void addBinding(Binding binding)
   {
      addToNameMap(binding);
      addToConditionMap(binding);
   }

   protected Binding removeBinding(int nodeId, String queueName)
   {
      Binding binding = removeFromNameMap(nodeId, queueName);

      removeFromConditionMap(binding);

      return binding;
   }

   protected void addToNameMap(Binding binding)
   {
      Integer nodeID = new Integer(binding.getNodeID());
      Map nameMap = (Map)nameMaps.get(nodeID);

      if (nameMap == null)
      {
         nameMap = new LinkedHashMap();
         nameMaps.put(nodeID, nameMap);
      }

      nameMap.put(binding.getQueue().getName(), binding);

      if (trace) { log.trace(this + " added " + binding + " to name map"); }
   }

   protected void addToConditionMap(Binding binding)
   {
      Condition condition = binding.getCondition();

      Bindings bindings = (Bindings)conditionMap.get(condition);

      if (bindings == null)
      {
         bindings = new DefaultBindings();
         conditionMap.put(condition, bindings);
      }

      bindings.addBinding(binding);

      if (trace) { log.trace(this + " added " + binding + " to condition map"); }
   }

   protected Binding removeFromNameMap(int nodeId, String queueName)
   {
      if (queueName == null)
      {
         throw new IllegalArgumentException("Queue name is null");
      }

      Map nameMap = (Map)nameMaps.get(new Integer(nodeId));

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
         nameMaps.remove(new Integer(nodeId));
      }

      return binding;
   }

   protected void removeFromConditionMap(Binding binding)
   {
      Bindings bindings = (Bindings)conditionMap.get(binding.getCondition());

      if (bindings == null)
      {
         throw new IllegalStateException("Cannot find condition bindings for " +
            binding.getCondition());
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
   }

   protected Map getDefaultDMLStatements()
   {
      Map map = new LinkedHashMap();

      map.put("INSERT_BINDING",
              "INSERT INTO JBM_POSTOFFICE (" +
                 "POSTOFFICE_NAME, " +
                 "NODE_ID, " +
                 "QUEUE_NAME, " +
                 "CONDITION, " +
                 "SELECTOR, " +
                 "CHANNEL_ID, " +
                 "IS_FAILED_OVER, " +
                 "FAILED_NODE_ID) " +
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

      map.put("DELETE_BINDING",
              "DELETE FROM JBM_POSTOFFICE WHERE POSTOFFICE_NAME=? AND NODE_ID=? AND QUEUE_NAME=?");

      map.put("LOAD_BINDINGS",
              "SELECT " +
                 "NODE_ID, " +
                 "QUEUE_NAME, " +
                 "CONDITION, " +
                 "SELECTOR, " +
                 "CHANNEL_ID, " +
                 "IS_FAILED_OVER, " +
                 "FAILED_NODE_ID " +
                 "FROM JBM_POSTOFFICE WHERE POSTOFFICE_NAME  = ?");

      return map;
   }

   protected Map getDefaultDDLStatements()
   {
      Map map = new LinkedHashMap();
      map.put("CREATE_POSTOFFICE_TABLE",
              "CREATE TABLE JBM_POSTOFFICE (POSTOFFICE_NAME VARCHAR(255), NODE_ID INTEGER," +
              "QUEUE_NAME VARCHAR(1023), CONDITION VARCHAR(1023), " +
              "SELECTOR VARCHAR(1023), CHANNEL_ID BIGINT, IS_FAILED_OVER CHAR(1), " +
              "FAILED_NODE_ID INTEGER)");
      return map;
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   // Private -------------------------------------------------------
                 
   // Inner classes ------------------------------------------------- 
      
}
