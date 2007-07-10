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
package org.jboss.messaging.core.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;
import javax.transaction.xa.Xid;

import org.jboss.jms.tx.MessagingXid;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.impl.message.MessageFactory;
import org.jboss.messaging.core.impl.message.MessageSupport;
import org.jboss.messaging.core.impl.tx.PreparedTxInfo;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.messaging.core.impl.tx.TxCallback;
import org.jboss.messaging.util.JDBCUtil;
import org.jboss.messaging.util.LockMap;
import org.jboss.messaging.util.StreamUtils;
import org.jboss.messaging.util.Util;

/**
 * JDBC implementation of PersistenceManager.
 *  
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 *
 * @version <tt>1.1</tt>
 *
 * JDBCPersistenceManager.java,v 1.1 2006/02/22 17:33:41 timfox Exp
 */
public class JDBCPersistenceManager extends JDBCSupport implements PersistenceManager
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(JDBCPersistenceManager.class); 

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
      
   private boolean usingBatchUpdates = false;
   
   private boolean usingBinaryStream = true;
   
   private boolean usingTrailingByte = false;
   
   private int maxParams;
   
   private short orderCount;
   
   private int nodeID;
   
   private boolean nodeIDSet;
      
   // Constructors --------------------------------------------------
    
   public JDBCPersistenceManager(DataSource ds, TransactionManager tm, Properties sqlProperties,
                                 boolean createTablesOnStartup, boolean usingBatchUpdates,
                                 boolean usingBinaryStream, boolean usingTrailingByte, int maxParams)
   {
      super(ds, tm, sqlProperties, createTablesOnStartup);
      
      this.usingBatchUpdates = usingBatchUpdates;
      
      this.usingBinaryStream = usingBinaryStream;
      
      this.usingTrailingByte = usingTrailingByte;
      
      this.maxParams = maxParams;      
   }
   
   
   // MessagingComponent overrides ---------------------------------
   
   public void start() throws Exception
   {
      super.start();

      Connection conn = null;

      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();      
         //JBossMessaging requires transaction isolation of READ_COMMITTED
         //Any looser isolation level and we cannot maintain consistency for paging (HSQL)
         if (conn.getTransactionIsolation() != Connection.TRANSACTION_READ_COMMITTED)
         {
            int level = conn.getTransactionIsolation();

            String warn =
               "\n\n" +
               "JBoss Messaging Warning: DataSource connection transaction isolation should be READ_COMMITTED, but it is currently " + Util.transactionIsolationToString(level) + ".\n" +
               "                         Using an isolation level less strict than READ_COMMITTED may lead to data consistency problems.\n" +
               "                         Using an isolation level more strict than READ_COMMITTED may lead to deadlock.\n";
            log.warn(warn);
         }
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         wrap.end();
      }
             
      log.debug(this + " started");
   }
   
   public void stop() throws Exception
   {
      super.stop();
   }
   
   // Injection -------------------------------------------------
   
   // This is only known by server peer so we inject it after startup
   
   public void injectNodeID(int nodeID)
   {
   	this.nodeID = nodeID;
   	
   	this.nodeIDSet = true;
   }
   
   // PersistenceManager implementation -------------------------
   
   // Related to XA Recovery
   // ======================
   
   public List getMessageChannelPairRefsForTx(long transactionId) throws Exception
   {
      String sql = this.getSQLStatement("SELECT_MESSAGE_ID_FOR_REF");
      return getMessageChannelPair(sql, transactionId);
   }
   
   public List getMessageChannelPairAcksForTx(long transactionId) throws Exception
   {
      String sql = this.getSQLStatement("SELECT_MESSAGE_ID_FOR_ACK");
      return getMessageChannelPair(sql, transactionId);
   }
   
   public List retrievePreparedTransactions() throws Exception
   {
      if (!this.nodeIDSet)
      {
      	//Sanity
      	throw new IllegalStateException("Node id has not been set");
      }
   	
      /* Note the API change for 1.0.2 XA Recovery -- List now contains instances of PreparedTxInfo<TxId, Xid>
       * instead of direct Xids [JPL] */
      
      Connection conn = null;
      PreparedStatement st = null;
      ResultSet rs = null;
      PreparedTxInfo txInfo = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         List transactions = new ArrayList();
         
         conn = ds.getConnection();
         
         st = conn.prepareStatement(getSQLStatement("SELECT_PREPARED_TRANSACTIONS"));
         
         st.setInt(1, nodeID);
         
         rs = st.executeQuery();
         
         while (rs.next())
         {
            //get the existing tx id --MK START
            long txId = rs.getLong(1);
            
            byte[] branchQual = getVarBinaryColumn(rs, 2);
            
            int formatId = rs.getInt(3);
            
            byte[] globalTxId = getVarBinaryColumn(rs, 4);
            
            Xid xid = new MessagingXid(branchQual, formatId, globalTxId);
            
            // create a tx info object with the result set detailsdetails
            txInfo = new PreparedTxInfo(txId, xid);
            
            transactions.add(txInfo);
         }
         
         return transactions;
         
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeResultSet(rs);
      	closeStatement(st);
      	closeConnection(conn);
         wrap.end();
      }
   }
         
               
   // Related to counters
   // ===================
   
   public long reserveIDBlock(String counterName, int size) throws Exception
   {
      if (trace) { log.trace("Getting ID block for counter " + counterName + ", size " + size); }
      
      if (size <= 0)
      {
         throw new IllegalArgumentException("block size must be > 0");
      }
      
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();

         //For the clustered case - this MUST use SELECT .. FOR UPDATE or a similar
         //construct the locks the row
         String selectCounterSQL = getSQLStatement("SELECT_COUNTER");
         
         ps = conn.prepareStatement(selectCounterSQL);
         
         ps.setString(1, counterName);
         
         rs = ps.executeQuery();
         
         if (trace) { log.trace(JDBCUtil.statementToString(selectCounterSQL, counterName)); }         
         
         if (!rs.next())
         {
            rs.close();
            rs = null;
            
            ps.close();
            
            //There is a very small possibility that two threads will attempt to insert the same counter
            //at the same time, if so, then the second one will fail eventually after a few retries by throwing
            //a primary key violation.
            
            String insertCounterSQL = getSQLStatement("INSERT_COUNTER");
            
            ps = conn.prepareStatement(insertCounterSQL);
            
            ps.setString(1, counterName);
            ps.setLong(2, size);
            
            int rows = updateWithRetry(ps);
            if (trace) { log.trace(JDBCUtil.statementToString(insertCounterSQL, counterName, new Integer(size)) + " inserted " + rows + " rows"); }
            
            ps.close();            
            ps = null;
            return 0;
         }
         
         long nextId = rs.getLong(1);
         
         rs.close();
         rs = null;
         
         ps.close();

         String updateCounterSQL = getSQLStatement("UPDATE_COUNTER");

         ps = conn.prepareStatement(updateCounterSQL);
         
         ps.setLong(1, nextId + size);
         ps.setString(2, counterName);
         
         int rows = updateWithRetry(ps);
         if (trace) { log.trace(JDBCUtil.statementToString(updateCounterSQL, new Long(nextId + size), counterName) + " updated " + rows + " rows"); }
         
         return nextId;
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeStatement(ps);
      	closeConnection(conn);
         wrap.end();
      }     
   }
         
   /*
    * Retrieve a List of messages corresponding to the specified List of message ids.
    * The implementation here for HSQLDB does this by using a PreparedStatment with an IN clause
    * with a maximum of 100 elements.
    * If there are more than maxParams message to retrieve this is repeated a number of times.
    * For "Enterprise" databases (Oracle, DB2, Sybase etc) a more sophisticated technique should be used
    * e.g. Oracle ARRAY types in Oracle which can be submitted as a param to an Oracle prepared statement
    * Although this would all be DB specific.
    */
   public List getMessages(List messageIds) throws Exception
   {
      if (trace) { log.trace("Getting batch of messages for " + messageIds); }
      
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         Iterator iter = messageIds.iterator();
         
         int size = messageIds.size();
         
         int count = 0;
         
         List msgs = new ArrayList();
         
         while (iter.hasNext())
         {
            if (ps == null)
            {
               //PreparedStatements are cached in the JCA layer so we will never actually have more than
               //100 distinct ones            
               int numParams;
               if (count < (size / maxParams) * maxParams)
               {
                  numParams = maxParams;
               }
               else
               {
                  numParams = size % maxParams;
               }
               StringBuffer buff = new StringBuffer(getSQLStatement("LOAD_MESSAGES"));
               buff.append(" WHERE ").append(getSQLStatement("MESSAGE_ID_COLUMN")).append(" IN (");
               for (int i = 0; i < numParams; i++)
               {
                  buff.append("?");
                  if (i < numParams - 1)
                  {
                     buff.append(",");
                  }
               }
               buff.append(")");
               ps = conn.prepareStatement(buff.toString());
               
               if (trace)
               {
                  log.trace(buff.toString());
               }
            }
            
            long msgId = ((Long)iter.next()).longValue();
            
            ps.setLong((count % maxParams) + 1, msgId);
            
            count++;
            
            if (!iter.hasNext() || count % maxParams == 0)
            {
               rs = ps.executeQuery();
               
               while (rs.next())
               {       
                  long messageId = rs.getLong(1);
                  
                  boolean reliable = rs.getString(2).equals("Y");
                  
                  long expiration = rs.getLong(3);
                  
                  long timestamp = rs.getLong(4);
                  
                  byte priority = rs.getByte(5);        
                  
                  byte[] bytes = getBytes(rs, 6);
                  
                  HashMap headers = bytesToMap(bytes);
                  
                  byte[] payload = getBytes(rs, 7);
                  
                  byte type = rs.getByte(8);
                  
                  Message m = MessageFactory.createMessage(messageId, reliable, expiration, timestamp, priority,
                                                           headers, payload, type);
                  msgs.add(m);
               }
               
               rs.close();
               rs = null;
               
               ps.close();
               ps = null;
            }
         }
         
         if (trace) { log.trace("Loaded " + msgs.size() + " messages in total"); }

         return msgs;
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeResultSet(rs);
      	closeStatement(ps);
      	closeConnection(conn);
         wrap.end();
      }
   }  
   
       
   // Related to paging functionality
   // ===============================                 
   
   public void pageReferences(long channelID, List references, boolean paged) throws Exception
   {
      Connection conn = null;
      PreparedStatement psInsertReference = null;  
      PreparedStatement psInsertMessage = null;
      PreparedStatement psUpdateMessage = null;
      PreparedStatement psMessageExists = null;
      ResultSet rsMessageExists = null;
      TransactionWrapper wrap = new TransactionWrapper();
            
      //First we order the references in message order
      orderReferences(references);
                         
      try
      {
         //Now we get a lock on all the messages. Since we have ordered the refs we should avoid deadlock
         getLocks(references);
         
         conn = ds.getConnection();
         
         Iterator iter = references.iterator();
         
         boolean messageInsertsInBatch = false;
         boolean messageUpdatesInBatch = false;
         
         psInsertReference = conn.prepareStatement(getSQLStatement("INSERT_MESSAGE_REF"));
         psInsertMessage = conn.prepareStatement(getSQLStatement("INSERT_MESSAGE"));
         psUpdateMessage = conn.prepareStatement(getSQLStatement("INC_CHANNEL_COUNT"));

         while (iter.hasNext())
         {
            //We may need to persist the message itself 
            MessageReference ref = (MessageReference) iter.next();
                                            
            //For non reliable refs we insert the ref (and maybe the message) itself
                           
            //Now store the reference
            addReference(channelID, ref, psInsertReference, paged);
                        
            if (usingBatchUpdates)
            {
               psInsertReference.addBatch();
            }
            else
            {
               int rows = updateWithRetry(psInsertReference);
               
               if (trace)
               {
                  log.trace("Inserted " + rows + " rows");
               }
            }
            
            //Maybe we need to persist the message itself
            Message m = ref.getMessage();
            
            //In a paging situation, we cannot use the persisted flag on the message to determine whether
            //to insert the message or not.
            //This is because a channel (possibly on another node) may be paging too and referencing
            //the same message, and might have removed the message independently, the other
            //channel will not know about this.
            //Therefore we have to check if the message is already in the database and insert it if it isn't
            
            //TODO This is a bit of a hassle -
            //A cleaner and better solution here is to completely separate out the paging functionality from the
            //standard persistence functionality since it complicates things considerably.
            //We should define a paging store which is separate from the persistence store, and
            //typically not using the database for the paging store - probably use a file based store
            //e.g HOWL or some other logger
            
            //Note when running this with two or more competing channels in the same process, then
            //we do not need a FOR UPDATE on the select since we lock the messages in memory
            //However for competing nodes, we do, therefore we require a database that supports
            //this, this is another reason why we cannot use HSQL in a clustered environment
            //since it does not have a for update equivalent
            
            boolean added;
            
            psMessageExists = conn.prepareStatement(getSQLStatement("MESSAGE_EXISTS"));
            
            psMessageExists.setLong(1, m.getMessageID());
            
            rsMessageExists = psMessageExists.executeQuery();
             
            if (rsMessageExists.next())
            {
               //Message exists
               
               // Update the message with the new channel count
               incrementChannelCount(m, psUpdateMessage);
                  
               added = false;              
            }
            else
            {
               //Hasn't been persisted before so need to persist the message
               storeMessage(m, psInsertMessage);
               
               added = true;
            }    
            
            if (usingBatchUpdates)
            {
               if (added)
               {
                  psInsertMessage.addBatch();
                  messageInsertsInBatch = true;
               }
               else
               {
                  psUpdateMessage.addBatch();
                  messageUpdatesInBatch = true;
               }
            }
            else
            {
               if (added)
               {
                  int rows = updateWithRetry(psInsertMessage);
                                      
                  if (trace)
                  {
                     log.trace("Inserted " + rows + " rows");
                  }
               }
               else
               {               
                  int rows = updateWithRetry(psUpdateMessage);
                  
                  if (trace)
                  {
                     log.trace("Updated " + rows + " rows");
                  }
               }
            }      
         }         
         
         if (usingBatchUpdates)
         {
            int[] rowsReference = updateWithRetryBatch(psInsertReference);
            
            if (trace) { logBatchUpdate(getSQLStatement("INSERT_MESSAGE_REF"), rowsReference, "inserted"); }
            
            if (messageInsertsInBatch)
            {
               int[] rowsMessage = updateWithRetryBatch(psInsertMessage);
               
               if (trace) { logBatchUpdate(getSQLStatement("INSERT_MESSAGE"), rowsMessage, "inserted"); }
            }
            if (messageUpdatesInBatch)
            {
               int[] rowsMessage = updateWithRetryBatch(psUpdateMessage);
               
               if (trace) { logBatchUpdate(getSQLStatement("INC_CHANNEL_COUNT"), rowsMessage, "updated"); }
            }
         }        
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeStatement(psInsertReference);
      	closeStatement(psInsertMessage);
      	closeStatement(psUpdateMessage);
      	closeConnection(conn);         
         try
         {
            wrap.end();                       
         }
         finally
         {            
            //And then release locks
            this.releaseLocks(references);
         }         
      }      
   }
         
   public void removeDepagedReferences(long channelID, List references) throws Exception
   {
      if (trace) { log.trace(this + " Removing " + references.size() + " refs from channel " + channelID); }
          
      Connection conn = null;
      PreparedStatement psDeleteReference = null;  
      PreparedStatement psDeleteMessage = null;
      PreparedStatement psUpdateMessage = null;
      TransactionWrapper wrap = new TransactionWrapper();
        
      //We order the references
      orderReferences(references);
             
      try
      {
         //We get locks on all the messages - since they are ordered we avoid deadlock
         getLocks(references);
         
         conn = ds.getConnection();
         
         Iterator iter = references.iterator();
         
         psDeleteReference = conn.prepareStatement(getSQLStatement("DELETE_MESSAGE_REF"));
         psDeleteMessage = conn.prepareStatement(getSQLStatement("DELETE_MESSAGE"));
         psUpdateMessage = conn.prepareStatement(getSQLStatement("DEC_CHANNEL_COUNT"));

         while (iter.hasNext())
         {
            MessageReference ref = (MessageReference) iter.next();
                                                             
            removeReference(channelID, ref, psDeleteReference);
            
            if (usingBatchUpdates)
            {
               psDeleteReference.addBatch();
            }
            else
            {
               int rows = updateWithRetry(psDeleteReference);
               
               if (trace) { log.trace("Deleted " + rows + " rows"); }
            }
            
            Message m = ref.getMessage();
                                    
            //Maybe we need to delete the message itself
              
            //Update the message with the new channel count
            decrementChannelCount(m, psUpdateMessage);
            

            //Run the remove message update
            removeMessage(m, psDeleteMessage);
                        
            if (usingBatchUpdates)
            {
               psUpdateMessage.addBatch();
               
               psDeleteMessage.addBatch();
            }
            else
            {  
               int rows = updateWithRetry(psUpdateMessage);
                                                 
               if (trace) { log.trace("Updated " + rows + " rows"); }
               
               rows = updateWithRetry(psDeleteMessage);
        
               if (trace) { log.trace("Deleted " + rows + " rows"); }
            }  
            
         }         
         
         if (usingBatchUpdates)
         {
            int[] rowsReference = updateWithRetryBatch(psDeleteReference);
            
            if (trace) { logBatchUpdate(getSQLStatement("DELETE_MESSAGE_REF"), rowsReference, "deleted"); }
            
            rowsReference = updateWithRetryBatch(psUpdateMessage);
            
            if (trace) { logBatchUpdate(getSQLStatement("DEC_CHANNEL_COUNT"), rowsReference, "updated"); }
            
            rowsReference = updateWithRetryBatch(psDeleteMessage);
            
            if (trace) { logBatchUpdate(getSQLStatement("DELETE_MESSAGE"), rowsReference, "deleted"); }
         }              
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeStatement(psDeleteReference);
      	closeStatement(psDeleteMessage);
      	closeStatement(psUpdateMessage);
      	closeConnection(conn);         
         try
         {
            wrap.end();
         }
         finally
         {     
            //And then release locks
            this.releaseLocks(references);
         }         
      }      
   }
   
   public void updateReferencesNotPagedInRange(long channelID, long orderStart, long orderEnd, long num) throws Exception
   {
      if (trace) { log.trace("Updating paaged references for channel " + channelID + " between " + orderStart + " and " + orderEnd); }
      
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();
    
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(getSQLStatement("UPDATE_REFS_NOT_PAGED"));
                 
         ps.setLong(1, orderStart);
         
         ps.setLong(2, orderEnd);
         
         ps.setLong(3, channelID);
         
         int rows = updateWithRetry(ps);
           
         if (trace) { log.trace(JDBCUtil.statementToString(getSQLStatement("UPDATE_REFS_NOT_PAGED"), new Long(channelID),
                                new Long(orderStart), new Long(orderEnd)) + " updated " + rows + " rows"); }

         //Sanity check
         if (rows != num)
         {
            throw new IllegalStateException("Did not update correct number of rows");
         }            
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeStatement(ps);
      	closeConnection(conn);       
         wrap.end();
      }
   }

   public void mergeTransactions (long fromChannelID, long toChannelID) throws Exception
   {
      if (trace) { log.trace("Merging transactions from channel " + fromChannelID + " to " + toChannelID); }

      // Sanity check
      
      if (fromChannelID == toChannelID)
      {
      	throw new IllegalArgumentException("Cannot merge transactions - they have the same channel id!!");
      }

      Connection conn = null;
      PreparedStatement statement = null;
      TransactionWrapper wrap = new TransactionWrapper();
      try
      {
         conn = ds.getConnection();
         statement = conn.prepareStatement(getSQLStatement("UPDATE_TX"));
         statement.setLong(1, toChannelID);
         statement.setLong(2, fromChannelID);
         int affected = statement.executeUpdate();

         log.debug("Merged " + affected + " transactions from channel " + fromChannelID + " into node " + toChannelID);
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         closeConnection(conn);
         closeStatement(statement);
         wrap.end();
      }
   }
   
   public InitialLoadInfo mergeAndLoad(long fromChannelID, long toChannelID, int numberToLoad, long firstPagingOrder, long nextPagingOrder) throws Exception
   {
      if (trace) { log.trace("Merging channel from " + fromChannelID + " to " + toChannelID + " numberToLoad:" + numberToLoad + " firstPagingOrder:" + firstPagingOrder + " nextPagingOrder:" + nextPagingOrder); }
      
      //Sanity
      
      if (fromChannelID == toChannelID)
      {
      	throw new IllegalArgumentException("Cannot merge queues - they have the same channel id!!");
      }
      
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      PreparedStatement ps2 = null;
    
      try
      {
         conn = ds.getConnection();
         
         /*
          * If channel is paging and has full size f
          * 
          * then we don't need to load any refs but we need to:
          * 
          * make sure the page ord is correct across the old paged and new refs
          * 
          * we know the max page ord (from the channel) for the old refs so we just need to:
          * 
          * 1) Iterate through the failed channel and update page_ord = max + 1, max + 2 etc
          * 
          * 2) update channel id
          * 
          * 
          * If channel is not paging and the total refs before and after <=f
          * 
          * 1) Load all refs from failed channel
          * 
          * 2) Update channel id
          * 
          * return those refs
          * 
          * 
          * If channel is not paging but total new refs > f
          * 
          * 1) Iterate through failed channel refs and take the first x to make the channel full
          * 
          * 2) Update the others with page_ord starting at zero 
          * 
          * 3) Update channel id
          * 
          * In general:
          * 
          * We have number to load n, max page size p
          * 
          * 1) Iterate through failed channel refs in page_ord order
          * 
          * 2) Put the first n in a List.
          * 
          * 3) Initialise page_ord_count to be p or 0 depending on whether it was specified
          * 
          * 4) Update the page_ord of the remaining refs accordiningly
          * 
          * 5) Update the channel id
          * 
          */
         
         //First load the refs from the failed channel

         List refs = new ArrayList();
         
         ps = conn.prepareStatement(getSQLStatement("LOAD_REFS"));
         
         ps.setLong(1, fromChannelID);
                 
         rs = ps.executeQuery();
         
         int count = 0;
         
         boolean arePaged = false;
         
         long pageOrd = nextPagingOrder;
         
         while (rs.next())
         {
            long msgId = rs.getLong(1);            
            int deliveryCount = rs.getInt(2);
            long sched = rs.getLong(3);
            
            if (count < numberToLoad)
            {           
               ReferenceInfo ri = new ReferenceInfo(msgId, deliveryCount, sched);
               
               refs.add(ri);
            }
            
            // Set page ord
            
            if (ps2 == null)
            {
               ps2 = conn.prepareStatement(getSQLStatement("UPDATE_PAGE_ORDER"));
            }
                
            if (count < numberToLoad)
            {
               ps2.setNull(1, Types.BIGINT);
               
               if (trace) { log.trace("Set page ord to null"); }
            }
            else
            {                                 
               ps2.setLong(1, pageOrd);
               
               if (trace) { log.trace("Set page ord to " + pageOrd); }
               
               arePaged = true; 
               
               pageOrd++;                      
            }
            
            ps2.setLong(2, msgId);
            
            ps2.setLong(3, fromChannelID);
            
            int rows = updateWithRetry(ps2);
            
            if (trace) { log.trace("Update page ord updated " + rows + " rows"); }

            count++;            
         }
         
         ps.close();
         
         // Now swap the channel id
         
         ps = conn.prepareStatement(getSQLStatement("UPDATE_CHANNEL_ID"));
         
         ps.setLong(1, toChannelID);
         
         ps.setLong(2, fromChannelID);
         
         int rows = updateWithRetry(ps);
         
         if (trace) { log.trace("Update channel id updated " + rows + " rows"); }
                           
         if (arePaged)
         {            
            return new InitialLoadInfo(new Long(firstPagingOrder), new Long(pageOrd - 1), refs);
         }
         else
         {
            return new InitialLoadInfo(null, null, refs);
         }         
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeStatement(ps);
      	closeStatement(ps2);
      	closeConnection(conn);       
         wrap.end();
      }
   }
   
   public void updatePageOrder(long channelID, List references) throws Exception
   {
      Connection conn = null;
      PreparedStatement psUpdateReference = null;  
      TransactionWrapper wrap = new TransactionWrapper();
      
      if (trace) { log.trace("Updating page order for channel:" + channelID); }
        
      try
      {
         conn = ds.getConnection();
         
         Iterator iter = references.iterator();
         
         psUpdateReference = conn.prepareStatement(getSQLStatement("UPDATE_PAGE_ORDER"));

         while (iter.hasNext())
         {
            MessageReference ref = (MessageReference) iter.next();
                 
            psUpdateReference.setLong(1, ref.getPagingOrder());

            psUpdateReference.setLong(2, ref.getMessage().getMessageID());
            
            psUpdateReference.setLong(3, channelID);
            
            if (usingBatchUpdates)
            {
               psUpdateReference.addBatch();
            }
            else
            {
               int rows = updateWithRetry(psUpdateReference);
               
               if (trace) { log.trace("Updated " + rows + " rows"); }
            }
         }
                     
         if (usingBatchUpdates)
         {
            int[] rowsReference = updateWithRetryBatch(psUpdateReference);
            
            if (trace) { logBatchUpdate(getSQLStatement("UPDATE_PAGE_ORDER"), rowsReference, "updated"); }
         }
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeStatement(psUpdateReference);
      	closeConnection(conn);       
         wrap.end();
      }    
   }
      
   public List getPagedReferenceInfos(long channelID, long orderStart, int number) throws Exception
   {
      if (trace) { log.trace("loading message reference info for channel " + channelID + " from " + orderStart + " number " + number);      }
                 
      List refs = new ArrayList();
      
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(getSQLStatement("LOAD_PAGED_REFS"));
         
         ps.setLong(1, channelID);
         
         ps.setLong(2, orderStart);
         
         ps.setLong(3, orderStart + number - 1);
         
         rs = ps.executeQuery();
         
         long ord = orderStart;
         
         while (rs.next())
         {
            long msgId = rs.getLong(1);     
            int deliveryCount = rs.getInt(2);
            int pageOrd = rs.getInt(3);
            long sched = rs.getLong(4);
            
            //Sanity check
            if (pageOrd != ord)
            {
               throw new IllegalStateException("Unexpected pageOrd: " + pageOrd + " expected: " + ord);
            }
            
            ReferenceInfo ri = new ReferenceInfo(msgId, deliveryCount, sched);
            
            refs.add(ri);
            ord++;
         }
         
         //Sanity check
         if (ord != orderStart + number)
         {
            throw new IllegalStateException("Didn't load expected number of references, loaded: " + (ord - orderStart) +
                                            " expected: " + number);
         }
         
         return refs;
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeResultSet(rs);
      	closeStatement(ps);
      	closeConnection(conn);       
         wrap.end();
      }      
   }   
   
   /*
    * Load the initial, non paged refs
    */
   public InitialLoadInfo loadFromStart(long channelID, int number) throws Exception
   {
      if (trace) { log.trace("loading initial reference infos for channel " + channelID);  }
                    
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
                                
      try
      {
         conn = ds.getConnection();         
         
         //First we get the values for min() and max() page order
         ps = conn.prepareStatement(getSQLStatement("SELECT_MIN_MAX_PAGE_ORD"));
         
         ps.setLong(1, channelID);
         
         rs = ps.executeQuery();
                  
         rs.next();
         
         Long minOrdering = new Long(rs.getLong(1));
         
         if (rs.wasNull())
         {
            minOrdering = null;
         }
         
         Long maxOrdering = new Long(rs.getLong(2));
         
         if (rs.wasNull())
         {
            maxOrdering = null;
         }
         
         ps.close();
         
         conn.close();
         
         conn = ds.getConnection();         
         
         ps = conn.prepareStatement(getSQLStatement("LOAD_UNPAGED_REFS"));
         
         ps.setLong(1, channelID);
                 
         rs = ps.executeQuery();
         
         List refs = new ArrayList();
         
         int count = 0;
         while (rs.next())
         {
            long msgId = rs.getLong(1);            
            int deliveryCount = rs.getInt(2);
            long sched = rs.getLong(3);
            
            ReferenceInfo ri = new ReferenceInfo(msgId, deliveryCount, sched);
            
            if (count < number)
            {
               refs.add(ri);
            }            
            
            count++;
         }
                  
         //No refs paged
            
         if (count > number)
         {
            throw new IllegalStateException("Cannot load channel " + channelID + " since the fullSize parameter is too small to load " +
                     " all the required references, fullSize needs to be at least " + count + " it is currently " + number);
         }
                         
         return new InitialLoadInfo(minOrdering, maxOrdering, refs);
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeResultSet(rs);
      	closeStatement(ps);      	
      	closeConnection(conn);       
         wrap.end();
      }      
   }   
   
   
   // End of paging functionality
   // ===========================
   
   public void addReference(long channelID, MessageReference ref, Transaction tx) throws Exception
   {      
      if (tx != null)
      {
         //In a tx so we just add the ref in the tx in memory for now

         TransactionCallback callback = getCallback(tx);

         callback.addReferenceToAdd(channelID, ref);
      }
      else
      {         
         //No tx so add the ref directly in the db
         
         TransactionWrapper wrap = new TransactionWrapper();
         
         PreparedStatement psReference = null;
         PreparedStatement psMessage = null;
         
         Connection conn = ds.getConnection();
         
         Message m = ref.getMessage();     
           
         try
         {            
            // Get lock on message
            LockMap.instance.obtainLock(m);
                                    
            psReference = conn.prepareStatement(getSQLStatement("INSERT_MESSAGE_REF"));
            
            // Add the reference
            addReference(channelID, ref, psReference, false);
            
            int rows = updateWithRetry(psReference);      
            
            if (trace) { log.trace("Inserted " + rows + " rows"); }
              
            if (!m.isPersisted())
            {
               // First time so persist the message
               psMessage = conn.prepareStatement(getSQLStatement("INSERT_MESSAGE"));
               
               storeMessage(m, psMessage);
               
               m.setPersisted(true);
            }
            else
            {
               //Update the message's channel count
               psMessage = conn.prepareStatement(getSQLStatement("INC_CHANNEL_COUNT"));
               
               incrementChannelCount(m, psMessage);
            }
                           
            rows = updateWithRetry(psMessage);
            
            if (trace) { log.trace("Inserted/updated " + rows + " rows"); }     
            
            log.trace("message Inserted/updated " + rows + " rows");            
         }
         catch (Exception e)
         {
            wrap.exceptionOccurred();
            throw e;
         }
         finally
         {
         	closeStatement(psReference);
         	closeStatement(psMessage);
         	closeConnection(conn);  
            try
            {
               wrap.end();
            }
            finally
            {   
               //Release Lock
               LockMap.instance.releaseLock(m);
            }
         }      
      }
   }
   
   public void updateDeliveryCount(long channelID, MessageReference ref) throws Exception
   {
      TransactionWrapper wrap = new TransactionWrapper();
      
      PreparedStatement psReference = null;
      
      Connection conn = ds.getConnection();
       
      try
      {                                    
         psReference = conn.prepareStatement(getSQLStatement("UPDATE_DELIVERY_COUNT"));
         
         psReference.setInt(1, ref.getDeliveryCount());
         
         psReference.setLong(2, channelID);
         
         psReference.setLong(3, ref.getMessage().getMessageID());
         
         int rows = updateWithRetry(psReference);

         if (trace) { log.trace("Updated " + rows + " rows"); }
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeStatement(psReference);
      	closeConnection(conn);
         wrap.end();                        
      }  
   }
   
   public void removeReference(long channelID, MessageReference ref, Transaction tx) throws Exception
   {      
      if (tx != null)
      {
         //In a tx so we just add the ref in the tx in memory for now

         TransactionCallback callback = getCallback(tx);

         callback.addReferenceToRemove(channelID, ref);
      }
      else
      {         
         //No tx so we remove the reference directly from the db
         
         TransactionWrapper wrap = new TransactionWrapper();
         
         PreparedStatement psReference = null;
         PreparedStatement psUpdate = null;
         PreparedStatement psMessage = null;
         
         Connection conn = ds.getConnection();
         
         Message m = ref.getMessage();         
         
         try
         {
            //get lock on message
            LockMap.instance.obtainLock(m);
                              
            psReference = conn.prepareStatement(getSQLStatement("DELETE_MESSAGE_REF"));
            
            //Remove the message reference
            removeReference(channelID, ref, psReference);
            
            int rows = updateWithRetry(psReference);
            
            if (rows != 1)
            {
               log.warn("Failed to remove row for: " + ref);
               return;
            }
            
            if (trace) { log.trace("Deleted " + rows + " rows"); }
            
            //Update the messages channel count
            
            psUpdate = conn.prepareStatement(getSQLStatement("DEC_CHANNEL_COUNT"));
            
            decrementChannelCount(m, psUpdate);
            
            rows = updateWithRetry(psUpdate);
            
            if (trace) { log.trace("Updated " + rows + " rows"); } 
            
            //Delete the message (if necessary)
            
            psMessage = conn.prepareStatement(getSQLStatement("DELETE_MESSAGE"));
            
            removeMessage(m, psMessage);
                       
            rows = updateWithRetry(psMessage);
            
            if (trace) { log.trace("Delete " + rows + " rows"); }                           
         }
         catch (Exception e)
         {
            wrap.exceptionOccurred();
            throw e;
         }
         finally
         {
         	closeStatement(psReference);
         	closeStatement(psUpdate);
         	closeStatement(psMessage);
         	closeConnection(conn);
            try
            {
               wrap.end();               
            }
            finally
            {      
               //release the lock
               LockMap.instance.releaseLock(m);
            }
         }      
      }
   }
   
   public boolean referenceExists(long messageID) throws Exception
   {
      Connection conn = null;
      PreparedStatement st = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();

         st = conn.prepareStatement(getSQLStatement("SELECT_EXISTS_REF_MESSAGE_ID"));
         st.setLong(1, messageID);

         rs = st.executeQuery();

         if (rs.next())
         {
            return true;
         }
         else
         {
            return false;
         }
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeResultSet(rs);
      	closeStatement(st);
      	closeConnection(conn);
         wrap.end();
      }
   }

   // Public --------------------------------------------------------
   
   public String toString()
   {
      return "JDBCPersistenceManager[" + Integer.toHexString(hashCode()) + "]";
   }   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
      
   protected TransactionCallback getCallback(Transaction tx)
   {
      TransactionCallback callback = (TransactionCallback) tx.getCallback(this);

      if (callback == null)
      {
         callback = new TransactionCallback(tx);

         tx.addCallback(callback, this);
      }

      return callback;
   }
   
   /**
    * We order the list of references in ascending message order thus preventing deadlock when 2 or
    * more channels are updating the same messages in different transactions.
    */
   protected void orderReferences(List references)
   {      
      Collections.sort(references, MessageOrderComparator.instance);
   }
   
   protected void handleBeforeCommit1PC(List refsToAdd, List refsToRemove, Transaction tx)
      throws Exception
   {
      //TODO - A slight optimisation - it's possible we have refs referring to the same message
      //       so we will end up acquiring the lock more than once which is unnecessary. If find
      //       unique set of messages can avoid this.

      List allRefs = new ArrayList(refsToAdd.size() + refsToRemove.size());

      for(Iterator i = refsToAdd.iterator(); i.hasNext(); )
      {
         ChannelRefPair pair = (ChannelRefPair)i.next();
         allRefs.add(pair.ref);
      }

      for(Iterator i = refsToRemove.iterator(); i.hasNext(); )
      {
         ChannelRefPair pair = (ChannelRefPair)i.next();
         allRefs.add(pair.ref);
      }
            
      orderReferences(allRefs);
      
      // For one phase we simply add rows corresponding to the refs and remove rows corresponding to
      // the deliveries in one jdbc tx. We also need to store or remove messages as necessary,
      // depending on whether they've already been stored or still referenced by other channels.
         
      Connection conn = null;
      PreparedStatement psReference = null;
      PreparedStatement psInsertMessage = null;
      PreparedStatement psIncMessage = null;
      PreparedStatement psDecMessage = null;
      PreparedStatement psDeleteMessage = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         // Obtain locks on all messages
         getLocks(allRefs);
         
         // First the adds

         boolean messageInsertsInBatch = false;
         boolean messageUpdatesInBatch = false;
         boolean batch = usingBatchUpdates && refsToAdd.size() > 0;

         if (batch)
         {
            psReference = conn.prepareStatement(getSQLStatement("INSERT_MESSAGE_REF"));
            psInsertMessage = conn.prepareStatement(getSQLStatement("INSERT_MESSAGE"));
            psIncMessage = conn.prepareStatement(getSQLStatement("INC_CHANNEL_COUNT"));
         }

         for(Iterator i = refsToAdd.iterator(); i.hasNext(); )
         {
            ChannelRefPair pair = (ChannelRefPair)i.next();
            MessageReference ref = pair.ref;
                                                
            if (!batch)
            {
               psReference = conn.prepareStatement(getSQLStatement("INSERT_MESSAGE_REF"));
            }

            // Now store the reference
            addReference(pair.channelID, ref, psReference, false);
              
            if (batch)
            {
               psReference.addBatch();
            }
            else
            {
               int rows = updateWithRetry(psReference);
               
               if (trace) { log.trace("Inserted " + rows + " rows"); }                              

               psReference.close();
               psReference = null;
            }
            
            Message m = ref.getMessage();        
            
            if (!batch)
            {
               psInsertMessage = conn.prepareStatement(getSQLStatement("INSERT_MESSAGE"));
               psIncMessage = conn.prepareStatement(getSQLStatement("INC_CHANNEL_COUNT"));
            }

            boolean added;
            if (!m.isPersisted())
            {               
               // First time so add message
               storeMessage(m, psInsertMessage);
               added = true;
               m.setPersisted(true);
            }
            else
            {               
               // Update message channel count
               incrementChannelCount(m, psIncMessage);
               added = false;
            }
            
            if (batch)
            {
               if (added)
               {
                  psInsertMessage.addBatch();
                  if (trace) { log.trace("Message does not already exist so inserting it"); }
                  messageInsertsInBatch = true;
               }
               else
               { 
                  if (trace) { log.trace("Message already exists so updating count"); }
                  psIncMessage.addBatch();
                  messageUpdatesInBatch = true;
               }
            }
            else
            {
               if (added)
               {
                  if (trace) { log.trace("Message does not already exist so inserting it"); }
                  int rows = updateWithRetry(psInsertMessage);
                  if (trace) { log.trace("Inserted " + rows + " rows"); }
               }
               else
               {
                  if (trace) { log.trace("Message already exists so updating count"); }
                  int rows = updateWithRetry(psIncMessage);
                  if (trace) { log.trace("Updated " + rows + " rows"); }
               }
               psInsertMessage.close();
               psInsertMessage = null;
               psIncMessage.close();
               psIncMessage = null;
            }
         }         
         
         if (batch)
         {
            // Process the add batch

            int[] rowsReference = updateWithRetryBatch(psReference);
            
            if (trace) { logBatchUpdate(getSQLStatement("INSERT_MESSAGE_REF"), rowsReference, "inserted"); }
            
            if (messageInsertsInBatch)
            {
               int[] rowsMessage = updateWithRetryBatch(psInsertMessage);
               if (trace) { logBatchUpdate(getSQLStatement("INSERT_MESSAGE"), rowsMessage, "inserted"); }
            }

            if (messageUpdatesInBatch)
            {
               int[] rowsMessage = updateWithRetryBatch(psIncMessage);
               if (trace) { logBatchUpdate(getSQLStatement("INC_CHANNEL_COUNT"), rowsMessage, "updated"); }
            }

            psReference.close();
            psReference = null;
            psInsertMessage.close();
            psInsertMessage = null;
            psIncMessage.close();
            psIncMessage = null;
         }

         // Now the removes

         psReference = null;
         psDeleteMessage = null;
         batch = usingBatchUpdates && refsToRemove.size() > 0;

         if (batch)
         {
            psReference = conn.prepareStatement(getSQLStatement("DELETE_MESSAGE_REF"));
            psDeleteMessage = conn.prepareStatement(getSQLStatement("DELETE_MESSAGE"));
            psDecMessage = conn.prepareStatement(getSQLStatement("DEC_CHANNEL_COUNT"));
         }

         
         for(Iterator i = refsToRemove.iterator(); i.hasNext(); )
         {
            ChannelRefPair pair = (ChannelRefPair)i.next();
            
            if (!batch)
            {
               psReference = conn.prepareStatement(getSQLStatement("DELETE_MESSAGE_REF"));
            }

            removeReference(pair.channelID, pair.ref, psReference);
            
            if (batch)
            {
               psReference.addBatch();
            }
            else
            {
               int rows = updateWithRetry(psReference);
               if (trace) { log.trace("Deleted " + rows + " rows"); }
               psReference.close();
               psReference = null;
            }
            
            if (!batch)
            {
               psDeleteMessage = conn.prepareStatement(getSQLStatement("DELETE_MESSAGE"));
               psDecMessage = conn.prepareStatement(getSQLStatement("DEC_CHANNEL_COUNT"));
            }

            Message m = pair.ref.getMessage();
                                
            // Update the channel count
            
            decrementChannelCount(m, psDecMessage);
            
            // Delete the message (if necessary)
            
            removeMessage(m, psDeleteMessage);
                       
            if (batch)
            {
               psDecMessage.addBatch();
               psDeleteMessage.addBatch();
            }
            else
            {
               int rows = updateWithRetry(psDecMessage);
               if (trace) { log.trace("Updated " + rows + " rows"); }
               
               rows = updateWithRetry(psDeleteMessage);
               if (trace) { log.trace("Deleted " + rows + " rows"); }

               psDeleteMessage.close();
               psDeleteMessage = null;
               psDecMessage.close();
               psDecMessage = null;
            }
         }
         
         if (batch)
         {
            // Process the remove batch

            int[] rows = updateWithRetryBatch(psReference);
            
            if (trace) { logBatchUpdate(getSQLStatement("DELETE_MESSAGE_REF"), rows, "deleted"); }
            
            rows = updateWithRetryBatch(psDecMessage);
            
            if (trace) { logBatchUpdate(getSQLStatement("DEC_CHANNEL_COUNT"), rows, "updated"); }

            rows = updateWithRetryBatch(psDeleteMessage);
            
            if (trace) { logBatchUpdate(getSQLStatement("DELETE_MESSAGE"), rows, "deleted"); }

            psReference.close();
            psReference = null;
            psDeleteMessage.close();
            psDeleteMessage = null;
            psDecMessage.close();
            psDecMessage = null;
         }
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();                  
         throw e;
      }
      finally
      {
      	closeStatement(psReference);
      	closeStatement(psInsertMessage);
      	closeStatement(psIncMessage);
      	closeStatement(psDecMessage);
      	closeStatement(psDeleteMessage);
      	closeConnection(conn);        
         try
         {
            wrap.end();                        
         }
         finally
         {  
            //Release the locks
            this.releaseLocks(allRefs);
         }
      }
   }
   
   protected void handleBeforeCommit2PC(List refsToRemove, Transaction tx)
      throws Exception
   {          
      Connection conn = null;
      PreparedStatement psUpdateMessage = null;
      PreparedStatement psDeleteMessage = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      List refs = new ArrayList(refsToRemove.size());
      Iterator iter = refsToRemove.iterator();
      while (iter.hasNext())
      {
         ChannelRefPair pair = (ChannelRefPair)iter.next();
         refs.add(pair.ref);
      }
            
      orderReferences(refs);      
      
      try
      {
         //get locks on all the refs
         this.getLocks(refs);
         
         conn = ds.getConnection();
                  
         //2PC commit
         
         //First we commit any refs in state "+" to "C" and delete any
         //refs in state "-", then we
         //remove any messages due to refs we just removed
         //if they're not referenced elsewhere
         
         commitPreparedTransaction(tx, conn);
         
         boolean batch = usingBatchUpdates && refsToRemove.size() > 0;

         if (batch)
         {
            psDeleteMessage = conn.prepareStatement(getSQLStatement("DELETE_MESSAGE"));
            psUpdateMessage = conn.prepareStatement(getSQLStatement("DEC_CHANNEL_COUNT"));
         }
                  
         iter = refsToRemove.iterator();
         while (iter.hasNext())
         {
            ChannelRefPair pair = (ChannelRefPair) iter.next();
            
            MessageReference ref = pair.ref;
            
            if (!batch)
            {
               psDeleteMessage = conn.prepareStatement(getSQLStatement("DELETE_MESSAGE"));
               psUpdateMessage = conn.prepareStatement(getSQLStatement("DEC_CHANNEL_COUNT"));
            }
            
            Message m = ref.getMessage();
                                   
            //We may need to remove the message itself
            
            //Update the channel count
            
            decrementChannelCount(m, psUpdateMessage);
            
            //Remove the message (if necessary)
            
            removeMessage(m, psDeleteMessage);          
                           
            if (batch)
            {
               psUpdateMessage.addBatch();
                
               psDeleteMessage.addBatch(); 
            }
            else
            {
               int rows = updateWithRetry(psUpdateMessage);
               
               if (trace) { log.trace("Updated " + rows + " rows"); }
               
               rows = updateWithRetry(psDeleteMessage);
               
               if (trace) { log.trace("Deleted " + rows + " rows"); }
               
               psDeleteMessage.close();
               psDeleteMessage = null;
               psUpdateMessage.close();
               psUpdateMessage = null;
            }
         }         
         
         if (batch)
         {
            int[] rows = updateWithRetryBatch(psUpdateMessage);
            
            if (trace) { logBatchUpdate(getSQLStatement("DEC_CHANNEL_COUNT"), rows, "updated"); }
            
            psUpdateMessage.close();
            psUpdateMessage = null;
            
            rows = updateWithRetryBatch(psDeleteMessage);
            
            if (trace) { logBatchUpdate(getSQLStatement("DELETE_MESSAGE"), rows, "deleted"); }
            
            psDeleteMessage.close();
            psDeleteMessage = null;                           
         }         
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeStatement(psDeleteMessage);
      	closeStatement(psUpdateMessage);
      	closeConnection(conn);        
         try
         {
            wrap.end();
         }
         finally
         {
            //release the locks
            this.releaseLocks(refs);
         }
      }
   }
   
   protected void handleBeforePrepare(List refsToAdd, List refsToRemove, Transaction tx) throws Exception
   {
      //We only need to lock on the adds
      List refs = new ArrayList(refsToAdd.size());
      
      Iterator iter = refsToAdd.iterator();
      while (iter.hasNext())
      {
         ChannelRefPair pair = (ChannelRefPair)iter.next();
         
         refs.add(pair.ref);
      }
      
      orderReferences(refs);
      
      //We insert a tx record and
      //a row for each ref with +
      //and update the row for each delivery with "-"
      
      PreparedStatement psReference = null;
      PreparedStatement psInsertMessage = null;
      PreparedStatement psUpdateMessage = null;
      Connection conn = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         //get the locks
         getLocks(refs);
         
         conn = ds.getConnection();
         
         //Insert the tx record
         if (!refsToAdd.isEmpty() || !refsToRemove.isEmpty())
         {
            addTXRecord(conn, tx);
         }
         
         iter = refsToAdd.iterator();
         
         boolean batch = usingBatchUpdates && refsToAdd.size() > 1;
         boolean messageInsertsInBatch = false;
         boolean messageUpdatesInBatch = false;
         if (batch)
         {
            psReference = conn.prepareStatement(getSQLStatement("INSERT_MESSAGE_REF"));
            psInsertMessage = conn.prepareStatement(getSQLStatement("INSERT_MESSAGE"));
            psUpdateMessage = conn.prepareStatement(getSQLStatement("INC_CHANNEL_COUNT"));
         }

         while (iter.hasNext())
         {
            ChannelRefPair pair = (ChannelRefPair) iter.next();
            
            if (!batch)
            {
               psReference = conn.prepareStatement(getSQLStatement("INSERT_MESSAGE_REF"));
            }

            prepareToAddReference(pair.channelID, pair.ref, tx, psReference);
            
            if (batch)
            {
               psReference.addBatch();
            }
            else
            {
               int rows = updateWithRetry(psReference);
               
               if (trace) { log.trace("Inserted " + rows + " rows"); }

               psReference.close();
               psReference = null;
            }
            
            if (!batch)
            {
               psInsertMessage = conn.prepareStatement(getSQLStatement("INSERT_MESSAGE"));
               psUpdateMessage = conn.prepareStatement(getSQLStatement("INC_CHANNEL_COUNT"));
            }

            Message m = pair.ref.getMessage();
                   
            boolean added;         
            
            if (!m.isPersisted())
            {
               //First time so persist the message
               storeMessage(m, psInsertMessage);
               
               m.setPersisted(true);
               
               added = true;
            }
            else
            {
               //Update message channel count
               incrementChannelCount(m, psUpdateMessage);
               
               added = false;
            }
            
            if (batch)
            {
               if (added)
               {
                  psInsertMessage.addBatch();
                  messageInsertsInBatch = true;
               }
               else
               {
                  psUpdateMessage.addBatch();
                  messageUpdatesInBatch = true;
               }
            }
            else
            {
               if (added)
               {
                  int rows = updateWithRetry(psInsertMessage);
                  
                  if (trace) { log.trace("Inserted " + rows + " rows"); }
               }
               else
               {
                  int rows = updateWithRetry(psUpdateMessage);
                  
                  if (trace) { log.trace("Updated " + rows + " rows"); }
               }
               psInsertMessage.close();
               psInsertMessage = null;
               psUpdateMessage.close();
               psUpdateMessage = null;
            }
         }         
         
         if (batch)
         {
            int[] rowsReference = updateWithRetryBatch(psReference);
            
            if (trace) { logBatchUpdate(getSQLStatement("INSERT_MESSAGE_REF"), rowsReference, "inserted"); }
            
            if (messageInsertsInBatch)
            {
               int[] rowsMessage = updateWithRetryBatch(psInsertMessage);
               
               if (trace) { logBatchUpdate(getSQLStatement("INSERT_MESSAGE"), rowsMessage, "inserted"); }
            }
            if (messageUpdatesInBatch)
            {
               int[] rowsMessage = updateWithRetryBatch(psUpdateMessage);
               
               if (trace) { logBatchUpdate(getSQLStatement("INC_CHANNEL_COUNT"), rowsMessage, "updated"); }
            }

            psReference.close();
            psReference = null;
            psInsertMessage.close();
            psInsertMessage = null;
            psUpdateMessage.close();
            psUpdateMessage = null;
         }
         
         //Now the removes
         
         iter = refsToRemove.iterator();
         
         batch = usingBatchUpdates && refsToRemove.size() > 1;
         if (batch)
         {
            psReference = conn.prepareStatement(getSQLStatement("UPDATE_MESSAGE_REF"));
         }

         while (iter.hasNext())
         {
            ChannelRefPair pair = (ChannelRefPair) iter.next();
            
            if (!batch)
            {
               psReference = conn.prepareStatement(getSQLStatement("UPDATE_MESSAGE_REF"));
            }
            
            prepareToRemoveReference(pair.channelID, pair.ref, tx, psReference);
            
            if (batch)
            {
               psReference.addBatch();
            }
            else
            {
               int rows = updateWithRetry(psReference);
               
               if (trace) { log.trace("updated " + rows + " rows"); }
               
               psReference.close();
               psReference = null;
            }
         }
         
         if (batch)
         {
            int[] rows = updateWithRetryBatch(psReference);
            
            if (trace) { logBatchUpdate(getSQLStatement("UPDATE_MESSAGE_REF"), rows, "updated"); }
            
            psReference.close();
            psReference = null;
         }
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeStatement(psReference);
      	closeStatement(psInsertMessage);
      	closeStatement(psUpdateMessage);
      	closeConnection(conn);         
         try
         {
            wrap.end();            
         }
         finally
         {
            //release the locks
            
            this.releaseLocks(refs);
         }
      }
   }
   
   protected void handleBeforeRollback(List refsToAdd, Transaction tx) throws Exception
   {
      //remove refs marked with +
      //and update rows marked with - to C
            
      PreparedStatement psDeleteMessage = null;
      PreparedStatement psUpdateMessage = null;
      Connection conn = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      List refs = new ArrayList(refsToAdd.size());
      
      Iterator iter = refsToAdd.iterator();
      
      while (iter.hasNext())
      {
         ChannelRefPair pair = (ChannelRefPair)iter.next();
         refs.add(pair.ref);
      }
      
      orderReferences(refs);
      
      try
      {
         this.getLocks(refs);
         
         conn = ds.getConnection();
         
         rollbackPreparedTransaction(tx, conn);
         
         iter = refsToAdd.iterator();
         
         boolean batch = usingBatchUpdates && refsToAdd.size() > 1;

         if (batch)
         {
            psDeleteMessage = conn.prepareStatement(getSQLStatement("DELETE_MESSAGE"));
            psUpdateMessage = conn.prepareStatement(getSQLStatement("DEC_CHANNEL_COUNT"));
         }
                                 
         while (iter.hasNext())
         {
            ChannelRefPair pair = (ChannelRefPair) iter.next();
            
            if (!batch)
            {
               psDeleteMessage = conn.prepareStatement(getSQLStatement("DELETE_MESSAGE"));
               psUpdateMessage = conn.prepareStatement(getSQLStatement("DEC_CHANNEL_COUNT"));
            }
            
            Message m = pair.ref.getMessage();
                                         
            //We may need to remove the message for messages added during the prepare stage
                        
            //update the channel count
            
            decrementChannelCount(m, psUpdateMessage);
            
            //remove the message (if necessary)
            
            removeMessage(m, psDeleteMessage);
                                        
            if (batch)
            {
               psUpdateMessage.addBatch();
               
               psDeleteMessage.addBatch();
            }
            else
            {
               int rows = updateWithRetry(psUpdateMessage);
               
               if (trace) { log.trace("updated " + rows + " rows"); }
               
               rows = updateWithRetry(psDeleteMessage);
               
               if (trace) { log.trace("deleted " + rows + " rows"); }
               
               psDeleteMessage.close();
               psDeleteMessage = null;
               psUpdateMessage.close();
               psUpdateMessage = null;
            }            
         }
         
         if (batch)
         {
            int[] rows = updateWithRetryBatch(psUpdateMessage);
            
            if (trace) { logBatchUpdate(getSQLStatement("DEC_CHANNEL_COUNT"), rows, "updated"); }
            
            rows = updateWithRetryBatch(psDeleteMessage);
            
            if (trace) { logBatchUpdate(getSQLStatement("DELETE_MESSAGE"), rows, "deleted"); }
            
            psDeleteMessage.close();
            psDeleteMessage = null;
            
            psUpdateMessage.close();
            psUpdateMessage = null;            
         }
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeStatement(psDeleteMessage);
      	closeStatement(psUpdateMessage);      	
      	closeConnection(conn);                
         try
         {
            wrap.end();
         }
         finally
         {
            //release locks
            this.releaseLocks(refs);
         }
      }      
   }
   
   
   protected void addTXRecord(Connection conn, Transaction tx) throws Exception
   {
      if (trace)
      {
         log.trace("Inserting tx record for " + tx);
      }
      
      if (!this.nodeIDSet)
      {
      	//Sanity
      	throw new IllegalStateException("Node id has not been set");
      }
      
      PreparedStatement ps = null;
      String statement = "UNDEFINED";
      int rows = -1;
      int formatID = -1;
      try
      {
         statement = getSQLStatement("INSERT_TRANSACTION");
         
         ps = conn.prepareStatement(statement);
         
         ps.setInt(1, nodeID);
         
         ps.setLong(2, tx.getId());
         
         Xid xid = tx.getXid();
         
         formatID = xid.getFormatId();
         
         setVarBinaryColumn(3, ps, xid.getBranchQualifier());
         
         ps.setInt(4, formatID);
         
         setVarBinaryColumn(5, ps, xid.getGlobalTransactionId());
         
         rows = updateWithRetry(ps);
         
      }
      finally
      {
         if (trace)
         {
            String s = JDBCUtil.statementToString(statement, new Integer(nodeID), new Long(tx.getId()), "<byte-array>",
                  new Integer(formatID), "<byte-array>");
            log.trace(s + (rows == -1 ? " failed!" : " inserted " + rows + " row(s)"));
         }
         closeStatement(ps);      
      }
   }
   
   protected void removeTXRecord(Connection conn, Transaction tx) throws Exception
   {
      if (!this.nodeIDSet)
      {
      	//Sanity
      	throw new IllegalStateException("Node id has not been set");
      }
   	
      PreparedStatement ps = null;
      try
      {
         ps = conn.prepareStatement(getSQLStatement("DELETE_TRANSACTION"));
         
         ps.setInt(1, nodeID);
         
         ps.setLong(2, tx.getId());
         
         int rows = updateWithRetry(ps);
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(getSQLStatement("DELETE_TRANSACTION"), new Integer(nodeID), new Long(tx.getId())) + " removed " + rows + " row(s)");
         }
      }
      finally
      {
      	closeStatement(ps);    
      }
   }  
   
   protected void addReference(long channelID, MessageReference ref,
                               PreparedStatement ps, boolean paged) throws Exception
   {
      if (trace) { log.trace("adding " + ref + " to channel " + channelID); }
      
      ps.setLong(1, channelID);
      ps.setLong(2, ref.getMessage().getMessageID());
      ps.setNull(3, Types.BIGINT);
      ps.setString(4, "C");
      ps.setLong(5, getOrdering());
      if (paged)
      {
         ps.setLong(6, ref.getPagingOrder());
      }
      else
      {
         ps.setNull(6, Types.BIGINT);
      }
      ps.setInt(7, ref.getDeliveryCount());
      ps.setLong(8, ref.getScheduledDeliveryTime());
   }
   
   protected void removeReference(long channelID, MessageReference ref, PreparedStatement ps)
      throws Exception
   {
      if (trace) { log.trace("removing " + ref + " from channel " + channelID); }
      
      ps.setLong(1, ref.getMessage().getMessageID());
      ps.setLong(2, channelID);      
   }
   
   protected void prepareToAddReference(long channelID, MessageReference ref, Transaction tx, PreparedStatement ps)
     throws Exception
   {
      if (trace) { log.trace("adding " + ref + " to channel " + channelID + (tx == null ? " non-transactionally" : " on transaction: " + tx)); }
      
      ps.setLong(1, channelID);
      ps.setLong(2, ref.getMessage().getMessageID());
      ps.setLong(3, tx.getId());
      ps.setString(4, "+");
      ps.setLong(5, getOrdering());
      ps.setNull(6, Types.BIGINT);      
      ps.setInt(7, ref.getDeliveryCount());
      ps.setLong(8, ref.getScheduledDeliveryTime());
   }
   
   protected void prepareToRemoveReference(long channelID, MessageReference ref, Transaction tx, PreparedStatement ps)
      throws Exception
   {
      if (trace)
      {
         log.trace("removing " + ref + " from channel " + channelID
               + (tx == null ? " non-transactionally" : " on transaction: " + tx));
      }
      
      ps.setLong(1, tx.getId()); 
      ps.setLong(2, ref.getMessage().getMessageID());
      ps.setLong(3, channelID);           
   }
   
   protected void commitPreparedTransaction(Transaction tx, Connection conn) throws Exception
   {
      PreparedStatement ps = null;
      
      if (trace) { log.trace(this + " commitPreparedTransaction, tx= " + tx); }
        
      try
      {
         ps = conn.prepareStatement(getSQLStatement("COMMIT_MESSAGE_REF1"));
         
         ps.setLong(1, tx.getId());        
         
         int rows = updateWithRetry(ps);
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(getSQLStatement("COMMIT_MESSAGE_REF1"), new Long(tx.getId())) + " removed " + rows + " row(s)");
         }
         
         ps.close();
         ps = conn.prepareStatement(getSQLStatement("COMMIT_MESSAGE_REF2"));
         ps.setLong(1, tx.getId());         
         
         rows = updateWithRetry(ps);
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(getSQLStatement("COMMIT_MESSAGE_REF2"), new Long(tx.getId())) + " updated " + rows
                  + " row(s)");
         }
         
         removeTXRecord(conn, tx);
      }
      finally
      {
      	closeStatement(ps);
      }
   }
   
   protected void rollbackPreparedTransaction(Transaction tx, Connection conn) throws Exception
   {
      PreparedStatement ps = null;
      
      try
      {
         ps = conn.prepareStatement(getSQLStatement("ROLLBACK_MESSAGE_REF1"));
         
         ps.setLong(1, tx.getId());         
         
         int rows = updateWithRetry(ps);
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(getSQLStatement("ROLLBACK_MESSAGE_REF1"), new Long(tx.getId())) + " removed " + rows + " row(s)");
         }
         
         ps.close();
         
         ps = conn.prepareStatement(getSQLStatement("ROLLBACK_MESSAGE_REF2"));
         ps.setLong(1, tx.getId());
         
         rows = updateWithRetry(ps);
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(getSQLStatement("ROLLBACK_MESSAGE_REF2"), new Long(tx.getId())) + " updated " + rows
                  + " row(s)");
         }
         
         removeTXRecord(conn, tx);
      }
      finally
      {
      	closeStatement(ps);
      }
   }
   
   protected byte[] mapToBytes(Map map) throws Exception
   {
      if (map == null || map.isEmpty())
      {
         return null;
      }
      
      final int BUFFER_SIZE = 1024;
       
      ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
      
      DataOutputStream oos = new DataOutputStream(bos);
      
      StreamUtils.writeMap(oos, map, true);
      
      oos.close();
      
      return bos.toByteArray();
   }
   
   protected HashMap bytesToMap(byte[] bytes) throws Exception
   {
      if (bytes == null)
      {
         return new HashMap();
      }
       
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      
      DataInputStream dais = new DataInputStream(bis);
      
      HashMap map = StreamUtils.readMap(dais, true);
      
      dais.close();
            
      return map;
   }
   

   //TODO - combine these
   protected void incrementChannelCount(Message m, PreparedStatement ps) throws Exception
   {
      ps.setLong(1, m.getMessageID());
   }
         
   protected void decrementChannelCount(Message m, PreparedStatement ps) throws Exception
   {
      ps.setLong(1, m.getMessageID());
   }
   
   /**
    * Stores the message in the MESSAGE table.
    */
   protected void storeMessage(Message m, PreparedStatement ps) throws Exception
   {      
      // physically insert the row in the database
      // first set the fields from org.jboss.messaging.core.Routable
      ps.setLong(1, m.getMessageID());
      ps.setString(2, m.isReliable() ? "Y" : "N");
      ps.setLong(3, m.getExpiration());
      ps.setLong(4, m.getTimestamp());
      ps.setByte(5, m.getPriority());
      
      //headers
      byte[] bytes = mapToBytes(((MessageSupport) m).getHeaders());
      if (bytes != null)
      {
         setBytes(ps, 6, bytes);
      }
      else
      {
         ps.setNull(6, Types.LONGVARBINARY);
      }
      
      byte[] payload = m.getPayloadAsByteArray();
      if (payload != null)
      {
         setBytes(ps, 7, payload);
      }
      else
      {
         ps.setNull(7, Types.LONGVARBINARY);
      }
      
      //The number of channels that hold a reference to the message - initially always 1
      ps.setInt(8, 1);     
      
      ps.setByte(9, m.getType());
   }
   
   /**
    * Removes the message from the MESSAGE table.
    */
   protected void removeMessage(Message message, PreparedStatement ps) throws Exception
   {
      // physically delete the row in the database
      ps.setLong(1, message.getMessageID());      
   }
   
   protected void setVarBinaryColumn(int column, PreparedStatement ps, byte[] bytes) throws Exception
   {
      if (usingTrailingByte)
      {
         // Sybase has the stupid characteristic of truncating all trailing in zeros
         // in varbinary columns
         // So we add an extra byte on the end when we store the varbinary data
         // otherwise we might lose data
         // http://jira.jboss.org/jira/browse/JBMESSAGING-825
         
         byte[] res = new byte[bytes.length + 1];
         
         System.arraycopy(bytes, 0, res, 0, bytes.length);
         
         res[bytes.length] = 127;

         bytes = res;
      }
      
      ps.setBytes(column, bytes);            
      
      if (trace) { log.trace("Setting varbinary column of length: " + bytes.length); }
   }
   
   protected byte[] getVarBinaryColumn(ResultSet rs, int columnIndex) throws Exception
   {
      byte[] bytes = rs.getBytes(columnIndex);
      
      if (usingTrailingByte)
      {
         // Get rid of the trailing byte
         
         // http://jira.jboss.org/jira/browse/JBMESSAGING-825
         
         byte[] newBytes = new byte[bytes.length - 1];
         
         System.arraycopy(bytes, 0, newBytes, 0, bytes.length - 1);
         
         bytes = newBytes;
      }
      
      return bytes;
   }
     
   // Used for storing message headers and bodies
   protected void setBytes(PreparedStatement ps, int columnIndex, byte[] bytes) throws Exception
   {
      if (usingBinaryStream)
      {
         //Set the bytes using a binary stream - likely to be better for large byte[]
         
         InputStream is = null;
         
         try
         {
            is = new ByteArrayInputStream(bytes);
            
            ps.setBinaryStream(columnIndex, is, bytes.length);
         }
         finally
         {
            if (is != null)
            {
               is.close();
            }
         }
      }
      else
      {
         //Set the bytes using setBytes() - likely to be better for smaller byte[]
         
         setVarBinaryColumn(columnIndex, ps, bytes);
      }
   }
   
   protected byte[] getBytes(ResultSet rs, int columnIndex) throws Exception
   {
      if (usingBinaryStream)
      {
         //Get the bytes using a binary stream - likely to be better for large byte[]
         
         InputStream is = null;
         ByteArrayOutputStream os = null;
         
         final int BUFFER_SIZE = 4096;
         
         try
         {
            InputStream i = rs.getBinaryStream(columnIndex);
            
            if (i == null)
            {
               return null;
            }
            
            is = new BufferedInputStream(rs.getBinaryStream(columnIndex), BUFFER_SIZE);
            
            os = new ByteArrayOutputStream(BUFFER_SIZE);
            
            int b;
            while ((b = is.read()) != -1)
            {
               os.write(b);
            }
            
            return os.toByteArray();
         }
         finally
         {
            if (is != null)
            {
               is.close();
            }
            if (os != null)
            {
               os.close();
            }
         }
      }
      else
      {
         //Get the bytes using getBytes() - better for smaller byte[]
         return getVarBinaryColumn(rs, columnIndex);
      }
   }
   
   protected void getLocks(List refs)
   {
      Iterator iter = refs.iterator();
      while (iter.hasNext())
      {
         MessageReference ref = (MessageReference)iter.next();
         Message m = ref.getMessage();
         LockMap.instance.obtainLock(m);        
      }
   }
   
   protected void releaseLocks(List refs)
   {
      Iterator iter = refs.iterator();
      while (iter.hasNext())
      {
         MessageReference ref = (MessageReference)iter.next();
         Message m = ref.getMessage();
         LockMap.instance.releaseLock(m);         
      }
   }
   
   protected void logBatchUpdate(String name, int[] rows, String action)
   {
      int count = 0;
      for (int i = 0; i < rows.length; i++)
      {
         count += rows[i];
      }
      log.trace("Batch update " + name + ", " + action + " total of " + count + " rows");
   }
   
   protected int updateWithRetry(PreparedStatement ps) throws Exception
   {
      return updateWithRetry(ps, false)[0];
   }
   
   protected int[] updateWithRetryBatch(PreparedStatement ps) throws Exception
   {
      return updateWithRetry(ps, true);
   }
   
   //PersistentServiceSupport overrides ----------------------------
   
   protected Map getDefaultDDLStatements()
   {
      Map map = new LinkedHashMap();
      //Message reference
      map.put("CREATE_MESSAGE_REFERENCE",
              "CREATE TABLE JBM_MSG_REF (CHANNEL_ID BIGINT, " +
              "MESSAGE_ID BIGINT, TRANSACTION_ID BIGINT, STATE CHAR(1), ORD BIGINT, PAGE_ORD BIGINT, " +
              "DELIVERY_COUNT INTEGER, SCHED_DELIVERY BIGINT, PRIMARY KEY(CHANNEL_ID, MESSAGE_ID))");
      map.put("CREATE_IDX_MESSAGE_REF_TX", "CREATE INDEX JBM_MSG_REF_TX ON JBM_MSG_REF (TRANSACTION_ID)");
      map.put("CREATE_IDX_MESSAGE_REF_ORD", "CREATE INDEX JBM_MSG_REF_ORD ON JBM_MSG_REF (ORD)");
      map.put("CREATE_IDX_MESSAGE_REF_PAGE_ORD", "CREATE INDEX JBM_MSG_REF__PAGE_ORD ON JBM_MSG_REF (PAGE_ORD)");
      map.put("CREATE_IDX_MESSAGE_REF_MESSAGE_ID", "CREATE INDEX JBM_MSG_REF_MESSAGE_ID ON JBM_MSG_REF (MESSAGE_ID)");      
      map.put("CREATE_IDX_MESSAGE_REF_SCHED_DELIVERY", "CREATE INDEX JBM_MSG_REF_SCHED_DELIVERY ON JBM_MSG_REF (SCHED_DELIVERY)");
      //Message
      map.put("CREATE_MESSAGE",
              "CREATE TABLE JBM_MSG (MESSAGE_ID BIGINT, RELIABLE CHAR(1), " +
              "EXPIRATION BIGINT, TIMESTAMP BIGINT, PRIORITY TINYINT, HEADERS LONGVARBINARY, " +
              "PAYLOAD LONGVARBINARY, CHANNEL_COUNT INTEGER, TYPE TINYINT, " +
              "PRIMARY KEY (MESSAGE_ID))"); 
      //Transaction
      map.put("CREATE_TRANSACTION",
              "CREATE TABLE JBM_TX (" +
              "NODE_ID INTEGER, TRANSACTION_ID BIGINT, BRANCH_QUAL VARBINARY(254), " +
              "FORMAT_ID INTEGER, GLOBAL_TXID VARBINARY(254), PRIMARY KEY (TRANSACTION_ID))");
      //Counter
      map.put("CREATE_COUNTER",
              "CREATE TABLE JBM_COUNTER (NAME VARCHAR(255), NEXT_ID BIGINT, PRIMARY KEY(NAME))");
      return map;
   }
      
   protected Map getDefaultDMLStatements()
   {                
      Map map = new LinkedHashMap();
      //Message reference
      map.put("INSERT_MESSAGE_REF",
              "INSERT INTO JBM_MSG_REF (CHANNEL_ID, MESSAGE_ID, TRANSACTION_ID, STATE, ORD, PAGE_ORD, DELIVERY_COUNT, SCHED_DELIVERY) " +
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
      map.put("DELETE_MESSAGE_REF", "DELETE FROM JBM_MSG_REF WHERE MESSAGE_ID=? AND CHANNEL_ID=? AND STATE='C'");
      map.put("UPDATE_MESSAGE_REF",
              "UPDATE JBM_MSG_REF SET TRANSACTION_ID=?, STATE='-' " +
              "WHERE MESSAGE_ID=? AND CHANNEL_ID=? AND STATE='C'");
      map.put("UPDATE_PAGE_ORDER", "UPDATE JBM_MSG_REF SET PAGE_ORD = ? WHERE MESSAGE_ID=? AND CHANNEL_ID=?");
      map.put("COMMIT_MESSAGE_REF1", "UPDATE JBM_MSG_REF SET STATE='C', TRANSACTION_ID = NULL WHERE TRANSACTION_ID=? AND STATE='+'");
      map.put("COMMIT_MESSAGE_REF2", "DELETE FROM JBM_MSG_REF WHERE TRANSACTION_ID=? AND STATE='-'");
      map.put("ROLLBACK_MESSAGE_REF1", "DELETE FROM JBM_MSG_REF WHERE TRANSACTION_ID=? AND STATE='+'");
      map.put("ROLLBACK_MESSAGE_REF2", "UPDATE JBM_MSG_REF SET STATE='C', TRANSACTION_ID = NULL WHERE TRANSACTION_ID=? AND STATE='-'");
      map.put("LOAD_PAGED_REFS",
              "SELECT MESSAGE_ID, DELIVERY_COUNT, PAGE_ORD, SCHED_DELIVERY FROM JBM_MSG_REF " +
              "WHERE CHANNEL_ID = ? AND PAGE_ORD BETWEEN ? AND ? ORDER BY PAGE_ORD");
      map.put("LOAD_UNPAGED_REFS",
              "SELECT MESSAGE_ID, DELIVERY_COUNT, SCHED_DELIVERY FROM JBM_MSG_REF WHERE STATE = 'C' " +
              "AND CHANNEL_ID = ? AND PAGE_ORD IS NULL ORDER BY ORD");
      map.put("LOAD_REFS",
              "SELECT MESSAGE_ID, DELIVERY_COUNT, SCHED_DELIVERY FROM JBM_MSG_REF WHERE STATE = 'C' " +
              "AND CHANNEL_ID = ? ORDER BY ORD");      
      
      map.put("UPDATE_REFS_NOT_PAGED", "UPDATE JBM_MSG_REF SET PAGE_ORD = NULL WHERE PAGE_ORD BETWEEN ? AND ? AND CHANNEL_ID=?");       
      map.put("SELECT_MIN_MAX_PAGE_ORD", "SELECT MIN(PAGE_ORD), MAX(PAGE_ORD) FROM JBM_MSG_REF WHERE CHANNEL_ID = ?");     
      map.put("SELECT_EXISTS_REF_MESSAGE_ID", "SELECT MESSAGE_ID FROM JBM_MSG_REF WHERE MESSAGE_ID = ?");
      map.put("UPDATE_DELIVERY_COUNT", "UPDATE JBM_MSG_REF SET DELIVERY_COUNT = ? WHERE CHANNEL_ID = ? AND MESSAGE_ID = ?");
      map.put("UPDATE_CHANNEL_ID", "UPDATE JBM_MSG_REF SET CHANNEL_ID = ? WHERE CHANNEL_ID = ?");
      //Message
      map.put("LOAD_MESSAGES",
              "SELECT MESSAGE_ID, RELIABLE, EXPIRATION, TIMESTAMP, " +
              "PRIORITY, HEADERS, PAYLOAD, TYPE " +
              "FROM JBM_MSG");
      map.put("INSERT_MESSAGE",
              "INSERT INTO JBM_MSG (MESSAGE_ID, RELIABLE, EXPIRATION, " +
              "TIMESTAMP, PRIORITY, HEADERS, PAYLOAD, CHANNEL_COUNT, TYPE) " +           
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)" );
      map.put("INC_CHANNEL_COUNT", "UPDATE JBM_MSG SET CHANNEL_COUNT = CHANNEL_COUNT + 1 WHERE MESSAGE_ID=?");
      map.put("DEC_CHANNEL_COUNT", "UPDATE JBM_MSG SET CHANNEL_COUNT = CHANNEL_COUNT - 1 WHERE MESSAGE_ID=?");
      map.put("DELETE_MESSAGE", "DELETE FROM JBM_MSG WHERE MESSAGE_ID=? AND CHANNEL_COUNT = 0");
      map.put("MESSAGE_ID_COLUMN", "MESSAGE_ID");
      map.put("MESSAGE_EXISTS", "SELECT MESSAGE_ID FROM JBM_MSG WHERE MESSAGE_ID = ?");
      //Transaction
      map.put("INSERT_TRANSACTION",
              "INSERT INTO JBM_TX (NODE_ID, TRANSACTION_ID, BRANCH_QUAL, FORMAT_ID, GLOBAL_TXID) " +
              "VALUES(?, ?, ?, ?, ?)");
      map.put("DELETE_TRANSACTION", "DELETE FROM JBM_TX WHERE NODE_ID = ? AND TRANSACTION_ID = ?");
      map.put("SELECT_PREPARED_TRANSACTIONS", "SELECT TRANSACTION_ID, BRANCH_QUAL, FORMAT_ID, GLOBAL_TXID FROM JBM_TX WHERE NODE_ID = ?");
      map.put("SELECT_MESSAGE_ID_FOR_REF", "SELECT MESSAGE_ID, CHANNEL_ID FROM JBM_MSG_REF WHERE TRANSACTION_ID = ? AND STATE = '+' ORDER BY ORD");
      map.put("SELECT_MESSAGE_ID_FOR_ACK", "SELECT MESSAGE_ID, CHANNEL_ID FROM JBM_MSG_REF WHERE TRANSACTION_ID = ? AND STATE = '-' ORDER BY ORD");
      map.put("UPDATE_TX", "UPDATE JBM_TX SET NODE_ID=? WHERE NODE_ID=?");
      
      //Counter
      map.put("UPDATE_COUNTER", "UPDATE JBM_COUNTER SET NEXT_ID = ? WHERE NAME=?");
      map.put("SELECT_COUNTER", "SELECT NEXT_ID FROM JBM_COUNTER WHERE NAME=?");
      map.put("INSERT_COUNTER", "INSERT INTO JBM_COUNTER (NAME, NEXT_ID) VALUES (?, ?)");
      //Other
      map.put("SELECT_ALL_CHANNELS", "SELECT DISTINCT(CHANNEL_ID) FROM JBM_MSG_REF");

      return map;
   }
   
   // Private -------------------------------------------------------
   

   
   private int[] updateWithRetry(PreparedStatement ps, boolean batch) throws Exception
   {
      final int MAX_TRIES = 25;      
      
      int rows = 0;
      
      int[] rowsArr = null;
      
      int tries = 0;
      
      while (true)
      {
         try
         {
            if (batch)
            {
               rowsArr = ps.executeBatch();
            }
            else
            {
               rows = ps.executeUpdate();
            }
            
            if (tries > 0)
            {
               log.warn("Update worked after retry");
            }
            break;
         }
         catch (SQLException e)
         {
            log.warn("SQLException caught - assuming deadlock detected, try:" + (tries + 1), e);
            tries++;
            if (tries == MAX_TRIES)
            {
               log.error("Retried " + tries + " times, now giving up");
               throw new IllegalStateException("Failed to update references");
            }
            log.warn("Trying again after a pause");
            //Now we wait for a random amount of time to minimise risk of deadlock
            Thread.sleep((long)(Math.random() * 500));
         }  
      }
      
      if (batch)
      {
         return rowsArr;
      }
      else
      {
         return new int[] { rows };
      }
   }
   
   private List getMessageChannelPair(String sqlQuery, long transactionId) throws Exception
   {
      if (trace) log.trace("loading message and channel ids for tx [" + transactionId + "]");
      
      if (!this.nodeIDSet)
      {
      	//Sanity
      	throw new IllegalStateException("Node id has not been set");
      }
      
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         log.info("********** sql query:" + sqlQuery);
         
         ps = conn.prepareStatement(sqlQuery);
         
         ps.setLong(1, transactionId);
           
         rs = ps.executeQuery();
         
         //Don't use a Map. A message could be in multiple channels in a tx, so if you use a map
         //when you put the same message again it's going to overwrite the previous put!!
         
         List holders = new ArrayList();
         
         //Unique set of messages
         Set msgIds = new HashSet();
         
         //TODO it would probably have been simpler just to have done all this in a SQL JOIN rather
         //than do the join in memory.....
         
         class Holder
         {
            long messageId;
            long channelId;
            Holder(long messageId, long channelId)
            {
               this.messageId = messageId;
               this.channelId = channelId;
            }
         }
                  
         while(rs.next())
         {            
            long messageId = rs.getLong(1);
            long channelId = rs.getLong(2);
            
            Holder holder = new Holder(messageId, channelId);
            
            holders.add(holder);
                        
            msgIds.add(new Long(messageId));
            
            if (trace) log.trace("Loaded MsgID: " + messageId + " and ChannelID: " + channelId);
         }
         
         Map messageMap = new HashMap();
         
         List messages = getMessages(new ArrayList(msgIds));
         
         for (Iterator iter = messages.iterator(); iter.hasNext(); )
         {
            Message msg = (Message)iter.next();
            
            messageMap.put(new Long(msg.getMessageID()), msg);            
         }
         
         List returnList = new ArrayList();
         
         for (Iterator iter = holders.iterator(); iter.hasNext(); )
         {
            Holder holder = (Holder)iter.next();
            
            Message msg = (Message)messageMap.get(new Long(holder.messageId));
            
            if (msg == null)
            {
               throw new IllegalStateException("Cannot find message " + holder.messageId);
            }
            
            MessageChannelPair pair = new MessageChannelPair(msg, holder.channelId);
            
            returnList.add(pair);
         }
         
         return returnList;
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
      	closeResultSet(rs);
      	closeStatement(ps);
      	closeConnection(conn);
         wrap.end();
      }
   }
   
   private synchronized long getOrdering()
   {
      //We generate the ordering for the message reference by taking the lowest 48 bits of the current time and
      //concatenating with a 15 bit rotating counter to form a string of 63 bits which we then place
      //in the right most bits of a long, giving a positive signed 63 bit integer.
      
      //Having a time element in the ordering means we don't have to maintain a counter in the database
      //It also helps with failover since if two queues merge after failover then, the ordering will mean
      //their orderings interleave nicely and they still get consumed in pretty much time order
      
      //We only have to guarantee ordering per session, so having slight differences of time on different nodes is
      //not a problem
      
      //The time element is good for about 8919 years - if you're still running JBoss Messaging then, I suggest you need an
      //upgrade!
      
      long order = System.currentTimeMillis();
      
      order = order << 15;
      
      order = order | orderCount;
      
      if (orderCount == Short.MAX_VALUE)
      {
         orderCount = 0;
      }
      else
      {
         orderCount++;
      }
      
      return order;
   }

   // Inner classes -------------------------------------------------
        
   private static class ChannelRefPair
   {
      private long channelID;
      private MessageReference ref;
      
      private ChannelRefPair(long channelID, MessageReference ref)
      {
         this.channelID = channelID;
         this.ref = ref;
      }
   }
   
   private class TransactionCallback implements TxCallback
   {
      private Transaction tx;
      
      private List refsToAdd;
      
      private List refsToRemove;
      
      private TransactionCallback(Transaction tx)
      {
         this.tx = tx;
         
         refsToAdd = new ArrayList();
         
         refsToRemove = new ArrayList();
      }
      
      private void addReferenceToAdd(long channelId, MessageReference ref)
      {
         refsToAdd.add(new ChannelRefPair(channelId, ref));
      }
      
      private void addReferenceToRemove(long channelId, MessageReference ref)
      {
         refsToRemove.add(new ChannelRefPair(channelId, ref));
      }
      
      public void afterCommit(boolean onePhase)
      {
         //NOOP
      }
      
      public void afterPrepare()
      {
         //NOOP
      }
      
      public void afterRollback(boolean onePhase)
      {
         //NOOP
      }
      
      public void beforeCommit(boolean onePhase) throws Exception
      {
         if (onePhase)
         {
            handleBeforeCommit1PC(refsToAdd, refsToRemove, tx);
         }
         else
         {
            handleBeforeCommit2PC(refsToRemove, tx);
         }
      }
      
      public void beforePrepare() throws Exception
      {
         handleBeforePrepare(refsToAdd, refsToRemove, tx);
      }
      
      public void beforeRollback(boolean onePhase) throws Exception
      {
         if (onePhase)
         {
            //NOOP - nothing in db
         }
         else
         {
            handleBeforeRollback(refsToAdd, tx);
         }
      }
   }
   
   static class MessageOrderComparator implements Comparator
   {
      static MessageOrderComparator instance = new MessageOrderComparator();
      
      public int compare(Object o1, Object o2)
      {        
         MessageReference ref1 = (MessageReference)o1;
         MessageReference ref2 = (MessageReference)o2;

         long id1 = ref1.getMessage().getMessageID();         
         long id2 = ref2.getMessage().getMessageID(); 
         
         return (id1 < id2 ? -1 : (id1 == id2 ? 0 : 1));
      }      
   }
   
}
