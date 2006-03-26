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
package org.jboss.messaging.core.plugin;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.TransactionManager;
import javax.transaction.xa.Xid;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.plugin.contract.ChannelMapper;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.message.MessageSupport;
import org.jboss.messaging.core.message.RoutableSupport;
import org.jboss.messaging.core.persistence.JDBCUtil;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TxCallback;
import org.jboss.messaging.core.tx.XidImpl;
import org.jboss.serial.io.JBossObjectInputStream;
import org.jboss.serial.io.JBossObjectOutputStream;
import org.jboss.system.ServiceMBeanSupport;
import org.jboss.tm.TransactionManagerServiceMBean;

/**
 *  
 * JDBC implementation of PersistenceManager 
 *  
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 *
 * @version <tt>1.1</tt>
 *
 * JDBCPersistenceManager.java,v 1.1 2006/02/22 17:33:41 timfox Exp
 */
public class JDBCPersistenceManager extends ServiceMBeanSupport implements PersistenceManager
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(JDBCPersistenceManager.class);
   
   
   /* The default DML and DDL works with HSQLDB */
   
   //JMS_MESSAGE_REFERENCE
   
   protected String createMessageReference = "CREATE TABLE JMS_MESSAGE_REFERENCE (" + "CHANNELID BIGINT, "
   + "MESSAGEID BIGINT, " + "TRANSACTIONID BIGINT, " + "STATE CHAR(1), " + "ORD BIGINT, "
   + "DELIVERYCOUNT INTEGER, " + "RELIABLE CHAR(1), LOADED CHAR(1), " + "PRIMARY KEY(CHANNELID, MESSAGEID))";
   
   protected String createIdxMessageRefTx = "CREATE INDEX JMS_MESSAGE_REF_TX ON JMS_MESSAGE_REFERENCE (TRANSACTIONID)";
   
   protected String createIdxMessageRefOrd = "CREATE INDEX JMS_MESSAGE_REF_ORD ON JMS_MESSAGE_REFERENCE (ORD)";
   
   protected String createIdxMessageRefMessageId = "CREATE INDEX JMS_MESSAGE_REF_MESSAGEID ON JMS_MESSAGE_REFERENCE (MESSAGEID)";
   
   protected String insertMessageRef = "INSERT INTO JMS_MESSAGE_REFERENCE (CHANNELID, MESSAGEID, TRANSACTIONID, STATE, ORD, DELIVERYCOUNT, RELIABLE, LOADED) "
      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
   
   protected String deleteMessageRef = "DELETE FROM JMS_MESSAGE_REFERENCE WHERE MESSAGEID=? AND CHANNELID=? AND STATE='C'";
   
   protected String updateMessageRef = "UPDATE JMS_MESSAGE_REFERENCE SET TRANSACTIONID=?, STATE='-' "
      + "WHERE MESSAGEID=? AND CHANNELID=? AND STATE='C'";
   
   protected String updateMessageRefNotLoaded = "UPDATE JMS_MESSAGE_REFERENCE SET LOADED='N' WHERE MESSAGEID=? AND CHANNELID=?";
   
   protected String commitMessageRef1 = "UPDATE JMS_MESSAGE_REFERENCE SET STATE='C', TRANSACTIONID = NULL WHERE TRANSACTIONID=? AND STATE='+' ";
   
   protected String commitMessageRef2 = "DELETE FROM JMS_MESSAGE_REFERENCE WHERE TRANSACTIONID=? AND STATE='-'";
   
   protected String rollbackMessageRef1 = "DELETE FROM JMS_MESSAGE_REFERENCE WHERE TRANSACTIONID=? AND STATE='+'";
   
   protected String rollbackMessageRef2 = "UPDATE JMS_MESSAGE_REFERENCE SET STATE='C', TRANSACTIONID = NULL WHERE TRANSACTIONID=? AND STATE='-'";
   
   protected String loadReferenceInfo = "SELECT MESSAGEID, ORD, DELIVERYCOUNT FROM JMS_MESSAGE_REFERENCE "
      + "WHERE CHANNELID=? AND STATE <> '+' AND LOADED = 'N' ORDER BY ORD";
   
   protected String selectCountReferences = "SELECT COUNT(MESSAGEID) FROM JMS_MESSAGE_REFERENCE WHERE CHANNELID=? AND STATE <> '+' AND LOADED='N'";
   
   protected String selectMaxOrdering = "SELECT MAX(ORD) FROM JMS_MESSAGE_REFERENCE WHERE CHANNELID=?";
     
   protected String updateReliableRefs = 
      "UPDATE JMS_MESSAGE_REFERENCE SET LOADED='Y' WHERE ORD BETWEEN ? AND ? AND CHANNELID=? AND RELIABLE='Y' AND STATE <> '+'";
   
   protected String deleteChannelMessageRefs = "DELETE FROM JMS_MESSAGE_REFERENCE WHERE CHANNELID=?";
   
   protected String removeAllNonReliableRefs = "DELETE FROM JMS_MESSAGE_REFERENCE WHERE RELIABLE='N'";
   
   protected String removeAllNonReliableMessages = "DELETE FROM JMS_MESSAGE WHERE RELIABLE='N'";
   
   protected String updateAllReliableRefs = "UPDATE JMS_MESSAGE_REFERENCE SET LOADED='N'";
   
   
   //JMS_MESSAGE
   
   protected String createMessage = "CREATE TABLE JMS_MESSAGE (" + "MESSAGEID BIGINT, " + "RELIABLE CHAR(1), "
   + "EXPIRATION BIGINT, " + "TIMESTAMP BIGINT, " + "PRIORITY TINYINT, " + "COREHEADERS LONGVARBINARY, "
   + "PAYLOAD LONGVARBINARY, " + "CHANNELCOUNT INTEGER, " + "TYPE TINYINT, " + "JMSTYPE VARCHAR(255), " + "CORRELATIONID VARCHAR(255), "
   + "CORRELATIONID_BYTES VARBINARY, " + "DESTINATION_ID BIGINT, " + "REPLYTO_ID BIGINT, "
   + "JMSPROPERTIES LONGVARBINARY, " + "REFERENCECOUNT TINYINT, "
   + "PRIMARY KEY (MESSAGEID))";
   
   protected String loadMessages = "SELECT " + "MESSAGEID, " + "RELIABLE, " + "EXPIRATION, " + "TIMESTAMP, "
   + "PRIORITY, " + "COREHEADERS, " + "PAYLOAD, " + "CHANNELCOUNT, " + "TYPE, "  + "JMSTYPE, " + "CORRELATIONID, "
   + "CORRELATIONID_BYTES, " + "DESTINATION_ID, " + "REPLYTO_ID, " + "JMSPROPERTIES "
   + "FROM JMS_MESSAGE ";
   
   protected String insertMessage = "INSERT INTO JMS_MESSAGE (" + "MESSAGEID, " + "RELIABLE, " + "EXPIRATION, "
   + "TIMESTAMP, " + "PRIORITY, " + "COREHEADERS, " + "PAYLOAD, " + "CHANNELCOUNT, " + "TYPE, " +  "JMSTYPE, " + "CORRELATIONID, "
   + "CORRELATIONID_BYTES, " + "DESTINATION_ID, " + "REPLYTO_ID, " + "JMSPROPERTIES, "
   + "REFERENCECOUNT) "
   + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
   
   protected String updateMessageChannelCount = 
      "UPDATE JMS_MESSAGE SET CHANNELCOUNT=? WHERE MESSAGEID=?";
   
   protected String deleteMessage = "DELETE FROM JMS_MESSAGE WHERE MESSAGEID=?";
   
   protected String messageIdColumn = "MESSAGEID";
   
   //JMS_TRANSACTION
   
   protected String createTransaction = "CREATE TABLE JMS_TRANSACTION ("
      + "TRANSACTIONID BIGINT, " + "BRANCH_QUAL VARBINARY(254), "
      + "FORMAT_ID INTEGER, " + "GLOBAL_TXID VARBINARY(254), " + "PRIMARY KEY (TRANSACTIONID))";
   
   protected String insertTransaction = "INSERT INTO JMS_TRANSACTION (TRANSACTIONID, BRANCH_QUAL, FORMAT_ID, GLOBAL_TXID) "
      + "VALUES(?, ?, ?, ?)";
   
   protected String deleteTransaction = "DELETE FROM JMS_TRANSACTION WHERE TRANSACTIONID = ?";
   
   protected String selectPreparedTransactions = "SELECT TRANSACTIONID, BRANCH_QUAL, FORMAT_ID, GLOBAL_TXID FROM JMS_TRANSACTION";
   
   
   //JMS_COUNTER
   
   protected String createCounter = "CREATE TABLE JMS_COUNTER (NAME VARCHAR(255), NEXT_ID BIGINT, PRIMARY KEY(NAME))";
   
   protected String updateCounter = "UPDATE JMS_COUNTER SET NEXT_ID = ? WHERE NAME=?";
   
   protected String selectCounter = "SELECT NEXT_ID FROM JMS_COUNTER WHERE NAME=?";
   
   protected String insertCounter = "INSERT INTO JMS_COUNTER (NAME, NEXT_ID) VALUES (?, ?)";
   
   
   
   //   protected String selectReferenceCount =
   //      "SELECT REFERENCECOUNT FROM MESSAGE WHERE MESSAGEID = ?";
   //   
   //   protected String updateReferenceCount =
   //      "UPDATE MESSAGE SET REFERENCECOUNT=? WHERE MESSAGEID=?";
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
   
   protected String dataSourceJNDIName;
   
   protected DataSource ds;
   
   protected ObjectName tmObjectName;
   
   protected TransactionManager tm;
   
   protected ObjectName cmObjectName;
   
   protected ChannelMapper cm;
   
   protected Properties sqlProperties;
   
   protected boolean createTablesOnStartup = true;
   
   protected boolean usingBatchUpdates = true;
   
   protected boolean usingBinaryStream = true;
   
   protected int maxParams = 100;
   
   // Constructors --------------------------------------------------
   
   public JDBCPersistenceManager() throws Exception
   {
      this(null, null, null);
   }
   
   /**
    * Only used for testing. In a real deployment, the data source and the transaction manager are
    * injected as dependencies.
    */
   public JDBCPersistenceManager(DataSource ds, TransactionManager tm, ChannelMapper cm) throws Exception
   {
      this.ds = ds;
      this.tm = tm;
      this.cm = cm;
      sqlProperties = new Properties();
   }
   
   /**
    * Only used for testing. In a real deployment, the data source and the transaction manager are
    * injected as dependencies.
    */
   public JDBCPersistenceManager(DataSource ds, TransactionManager tm) throws Exception
   {
      this.ds = ds;
      this.tm = tm;
      sqlProperties = new Properties();
   }
   
   // ServiceMBeanSupport overrides ---------------------------------
   
   protected void startService() throws Exception
   {
      if (ds == null)
      {
         InitialContext ic = new InitialContext();
         ds = (DataSource) ic.lookup(dataSourceJNDIName);
         ic.close();
      }
      
      if (ds == null)
      {
         throw new Exception("No DataSource found. This service dependencies must "
               + "have not been enforced correctly!");
      }
      if (tm == null)
      {
         throw new Exception("No TransactionManager found. This service dependencies must "
               + "have not been enforced correctly!");
      }
      
      if (cmObjectName != null)
      {
         MBeanServer server = getServer();
         
         cm = (ChannelMapper) server.getAttribute(cmObjectName, "Instance");
      }
      
      initSqlProperties();
      
      if (createTablesOnStartup)
      {
         createSchema();
      }
      
      resetMessageData();
      
      log.debug(this + " started");
      
      this.usingBatchUpdates = false;
   }
   
   protected void stopService() throws Exception
   {
      log.debug(this + " stopped");
   }
   
   // PersistenceManager implementation -------------------------
   
   public Object getInstance()
   {
      return this;
   }
   
   
   public long reserveIDBlock(String counterName, int size) throws Exception
   {
      //TODO This will need locking (e.g. SELECT ... FOR UPDATE...) in the clustered case
      
      if (trace)
      {
         log.trace("Getting id block for counter: " + counterName + " ,size: " + size);
      }
      
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
         
         ps = conn.prepareStatement(selectCounter);
         
         ps.setString(1, counterName);
         
         rs = ps.executeQuery();
         
         if (!rs.next())
         {
            rs.close();
            rs = null;
            
            ps.close();
            
            ps = conn.prepareStatement(insertCounter);
            
            ps.setString(1, counterName);
            
            ps.setLong(2, size);
            
            int rows = ps.executeUpdate();
            
            if (trace)
            {
               log.trace(JDBCUtil.statementToString(insertCounter, counterName)
                     + " inserted " + rows + " rows");
            }  
            
            ps.close();            
            ps = null;
            
            return 0;
         }
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(selectCounter, counterName));
         }
         
         long nextId = rs.getLong(1);
         
         rs.close();
         rs = null;
         
         ps.close();
         
         ps = conn.prepareStatement(updateCounter);
         
         ps.setLong(1, nextId + size);
         
         ps.setString(2, counterName);
         
         int rows = ps.executeUpdate();
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(updateCounter, new Long(nextId + size),
                  counterName)
                  + " updated " + rows + " rows");
         }        
         
         return nextId;
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }     
   }
   
      
   public void updateReliableReferencesLoadedInRange(long channelID, long orderStart, long orderEnd) throws Exception
   {
      if (trace)
      {
         log.trace("Updating reliable references for channel " + channelID + " between " + orderStart + " and " + orderEnd);
      }
      
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(updateReliableRefs);
         
         ps.setLong(1, orderStart);
         
         ps.setLong(2, orderEnd);
         
         ps.setLong(3, channelID);
         
         int rows = ps.executeUpdate();
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(updateReliableRefs, new Long(channelID),
                  new Long(orderStart), new Long(orderEnd))
                  + " updated " + rows + " rows");
         }
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }
   }
   
   
   public int getNumberOfUnloadedReferences(long channelID) throws Exception
   {
      if (trace) { log.trace("getting number of unloaded references for channel [" + channelID + "]"); }
      
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(selectCountReferences);
         
         ps.setLong(1, channelID);
         
         rs = ps.executeQuery();
         
         rs.next();
         
         int count = rs.getInt(1);
         
         if (trace) { log.trace("There are " + count + " unloaded references"); }
         
         return count;
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (rs != null)
         {
            try
            {
               rs.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
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
      if (trace)
      {
         log.trace("Getting batch of messages for " + messageIds);
      }
      
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
               StringBuffer buff = new StringBuffer(loadMessages);
               buff.append("WHERE ").append(messageIdColumn).append(" IN (");
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
               
               int innerCount = 0;
               
               while (rs.next())
               {
                  long messageId = rs.getLong(1);
                  boolean reliable = rs.getString(2).equals("Y");
                  long expiration = rs.getLong(3);
                  long timestamp = rs.getLong(4);
                  byte priority = rs.getByte(5);
                  byte[] bytes = getLongVarBinary(rs, 6);
                  HashMap coreHeaders = bytesToMap(bytes);
                  byte[] payload = getLongVarBinary(rs, 7);
                  int persistentChannelCount = rs.getInt(8);
                  
                  //FIXME - We are mixing concerns here
                  //The basic JDBCPersistencManager should *only* know about core messages - not 
                  //JBossMessages - we should subclass JBDCPersistenceManager and the JBossMessage
                  //specific code in a subclass
                  
                  byte type = rs.getByte(9);
                  
                  Message m;
                  
                  if (!rs.wasNull())
                  {
                     //JBossMessage
                     String jmsType = rs.getString(10);
                     String correlationID = rs.getString(11);
                     byte[] correlationIDBytes = rs.getBytes(12);
                     long destinationId = rs.getLong(13);
                     long replyToId = rs.getLong(14);
                     boolean replyToExists = rs.wasNull();
                     bytes = getLongVarBinary(rs, 15);
                     HashMap jmsProperties = bytesToMap(bytes);
                     JBossDestination dest = cm.getJBossDestination(destinationId);
                     JBossDestination replyTo = replyToExists ? cm.getJBossDestination(replyToId) : null;
                     
                     m = MessageFactory.createJBossMessage(messageId, reliable, expiration, timestamp, priority,
                           coreHeaders, payload, persistentChannelCount,
                           type, jmsType, correlationID, correlationIDBytes, dest, replyTo,
                           jmsProperties);
                  }
                  else
                  {
                     //Core message
                     m = MessageFactory.createCoreMessage(messageId, reliable, expiration, timestamp, priority,
                           coreHeaders, payload, persistentChannelCount);
                  }
                  
                  msgs.add(m);
                  innerCount++;
               }
               
               rs.close();
               rs = null;
               
               ps.close();
               ps = null;
            }
         }
         
         if (trace)
         {
            log.trace("Loaded " + msgs.size() + " messages in total");
         }
         
         return msgs;
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (rs != null)
         {
            try
            {
               rs.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }
   }  
   
   public List getReferenceInfos(long channelID, int number) throws Exception
   {
      if (trace)
      {
         log.trace("loading message reference info for channel " + channelID + " for " + number + " refs");
      }
      
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         //First we select the references
         
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(loadReferenceInfo);
         
         ps.setLong(1, channelID);
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(loadReferenceInfo));
         }
         
         rs = ps.executeQuery();
         
         int totalRows = 0;
         
         List infos = new ArrayList();
         
         while (totalRows < number && rs.next())
         {
            long msgId = rs.getLong(1);
            long ordering = rs.getLong(2);
            int deliveryCount = rs.getInt(3);
            
            ReferenceInfo ri = new ReferenceInfo(msgId, ordering, deliveryCount);
            
            infos.add(ri);
            
            totalRows++;
         }
         
         return infos;
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (rs != null)
         {
            try
            {
               rs.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }
   }
   

   public void addReferences(long channelID, List references, boolean loaded) throws Exception
   {
      Connection conn = null;
      PreparedStatement psInsertReference = null;  
      PreparedStatement psInsertMessage = null;
      PreparedStatement psUpdateMessage = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         Iterator iter = references.iterator();
         
         boolean messageInsertsInBatch = false;
         boolean messageUpdatesInBatch = false;
         
         if (usingBatchUpdates)
         {
            psInsertReference = conn.prepareStatement(insertMessageRef);
            psInsertMessage = conn.prepareStatement(insertMessage);
            psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
         }
         
         while (iter.hasNext())
         {
            //We may need to persist the message itself 
            MessageReference ref = (MessageReference) iter.next();
            
            Message m = ref.getMessage();
                                    
            //For non reliable refs we insert the ref (and maybe the message) itself
                           
            if (!usingBatchUpdates)
            {
               psInsertReference = conn.prepareStatement(insertMessageRef);
            }
            
            //Now store the reference
            addReference(channelID, ref, psInsertReference, loaded);
                        
            if (usingBatchUpdates)
            {
               psInsertReference.addBatch();
            }
            else
            {
               int rows = psInsertReference.executeUpdate();
               
               if (trace)
               {
                  log.trace("Inserted " + rows + " rows");
               }
               
               psInsertReference.close();
               psInsertReference = null;
            }
            
            if (!usingBatchUpdates)
            {
               psInsertMessage = conn.prepareStatement(insertMessage);
               psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
            }
               
            synchronized (m)
            {                     
               m.incPersistentChannelCount();
                                             
               //Maybe we need to persist the message itself
  
               boolean added;
               if (m.getPersistentChannelCount() == 1)
               {
                  //Hasn't been persisted before so need to persist the message
                  storeMessage(m, psInsertMessage);
                  
                  added = true;
               }
               else
               {
                  //Update the message with the new channel count
                  updateMessageChannelCount(m, psUpdateMessage);
                  
                  added = false;
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
                     int rows = psInsertMessage.executeUpdate();
                                         
                     if (trace)
                     {
                        log.trace("Inserted " + rows + " rows");
                     }
                  }
                  else
                  {
                     int rows = psUpdateMessage.executeUpdate();
                    
                     if (trace)
                     {
                        log.trace("Updated " + rows + " rows");
                     }
                  }
                  psInsertMessage.close();
                  psInsertMessage = null;
                  psUpdateMessage.close();
                  psUpdateMessage = null;
               }      
            }
         }
         
         if (usingBatchUpdates)
         {
            int[] rowsReference = psInsertReference.executeBatch();
            
            if (trace)
            {
               logBatchUpdate(insertMessageRef, rowsReference, "inserted");
            }
            
            if (messageInsertsInBatch)
            {
               int[] rowsMessage = psInsertMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(insertMessage, rowsMessage, "inserted");
               }
            }
            if (messageUpdatesInBatch)
            {
               int[] rowsMessage = psUpdateMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(updateMessageChannelCount, rowsMessage, "updated");
               }
            }
            
            psInsertReference.close();
            psInsertReference = null;
            psInsertMessage.close();
            psInsertMessage = null;
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
         if (psInsertReference != null)
         {
            try
            {
               psInsertReference.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (psInsertMessage != null)
         {
            try
            {
               psInsertMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (psUpdateMessage != null)
         {
            try
            {
               psUpdateMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable t)
            {
            }
         }
         wrap.end();
      }      
   }
   
   public void removeReferences(long channelID, List references) throws Exception
   {
      if (trace) { log.trace(this + " Removing " + references.size() + " refs from channel " + channelID); }
      
      Connection conn = null;
      PreparedStatement psDeleteReference = null;  
      PreparedStatement psDeleteMessage = null;
      PreparedStatement psUpdateMessage = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         Iterator iter = references.iterator();
         
         boolean messageDeletionsInBatch = false;
         boolean messageUpdatesInBatch = false;
         
         if (usingBatchUpdates)
         {
            psDeleteReference = conn.prepareStatement(deleteMessageRef);
            psDeleteMessage = conn.prepareStatement(deleteMessage);
            psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
         }
         
         while (iter.hasNext())
         {
            MessageReference ref = (MessageReference) iter.next();
                                                             
            if (!usingBatchUpdates)
            {
               psDeleteReference = conn.prepareStatement(deleteMessageRef);
            }
            
            removeReference(channelID, ref, psDeleteReference);
            
            if (usingBatchUpdates)
            {
               psDeleteReference.addBatch();
            }
            else
            {
               int rows = psDeleteReference.executeUpdate();
               
               if (trace)
               {
                  log.trace("Deleted " + rows + " rows");
               }
               
               psDeleteReference.close();
               psDeleteReference = null;
            }
            
            if (!usingBatchUpdates)
            {
               psDeleteMessage = conn.prepareStatement(deleteMessage);
               psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
            }
               
            Message m = ref.getMessage();
                        
            synchronized (m)
            {                     
               m.decPersistentChannelCount();                              
               
               //Maybe we need to delete the message itself
  
               boolean removed;
               if (m.getPersistentChannelCount() == 0)
               {
                  //No more refs so remove the message
                  removeMessage(m, psDeleteMessage);
                  
                  removed = true;
               }
               else
               {
                  //Update the message with the new channel count
                  updateMessageChannelCount(m, psUpdateMessage);
                  
                  removed = false;
               }
               
               if (usingBatchUpdates)
               {
                  if (removed)
                  {
                     psDeleteMessage.addBatch();
                     messageDeletionsInBatch = true;
                  }
                  else
                  {
                     psUpdateMessage.addBatch();
                     messageUpdatesInBatch = true;
                  }
               }
               else
               {
                  if (removed)
                  {
                     int rows = psDeleteMessage.executeUpdate();
                     
                     if (trace)
                     {
                        log.trace("Deleted " + rows + " rows");
                     }
                  }
                  else
                  {
                     int rows = psUpdateMessage.executeUpdate();
                     
                     if (trace)
                     {
                        log.trace("Updated " + rows + " rows");
                     }
                  }
                  psDeleteMessage.close();
                  psDeleteMessage = null;
                  psUpdateMessage.close();
                  psUpdateMessage = null;
               }      
            }
         }
         
         if (usingBatchUpdates)
         {
            int[] rowsReference = psDeleteReference.executeBatch();
            
            if (trace)
            {
               logBatchUpdate(deleteMessageRef, rowsReference, "deleted");
            }
            
            if (messageDeletionsInBatch)
            {
               int[] rowsMessage = psDeleteMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(deleteMessage, rowsMessage, "deleted");
               }
            }
            if (messageUpdatesInBatch)
            {
               int[] rowsMessage = psUpdateMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(updateMessageChannelCount, rowsMessage, "updated");
               }
            }
            
            psDeleteReference.close();
            psDeleteReference = null;
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
         if (psDeleteReference != null)
         {
            try
            {
               psDeleteReference.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (psDeleteMessage != null)
         {
            try
            {
               psDeleteMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (psUpdateMessage != null)
         {
            try
            {
               psUpdateMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable t)
            {
            }
         }
         wrap.end();
      }      
   }
   
   public void updateReferencesNotLoaded(long channelID, List references) throws Exception
   {
      Connection conn = null;
      PreparedStatement psUpdateReference = null;  
      TransactionWrapper wrap = new TransactionWrapper();
      
      if (trace)
      {
         log.trace("Updating references to not loaded for channel:" + channelID);
      }
        
      try
      {
         conn = ds.getConnection();
         
         Iterator iter = references.iterator();
         
         if (usingBatchUpdates)
         {
            psUpdateReference = conn.prepareStatement(updateMessageRefNotLoaded);
         }
         
         while (iter.hasNext())
         {
            MessageReference ref = (MessageReference) iter.next();
                 
            if (!usingBatchUpdates)
            {
               psUpdateReference = conn.prepareStatement(updateMessageRefNotLoaded);
            }
            
            psUpdateReference.setLong(1, ref.getMessageID());
            
            psUpdateReference.setLong(2, channelID);
            
            if (usingBatchUpdates)
            {
               psUpdateReference.addBatch();
            }
            else
            {
               int rows = psUpdateReference.executeUpdate();
               
               if (trace)
               {
                  log.trace("Updated " + rows + " rows");
               }
               
               psUpdateReference.close();
               psUpdateReference = null;
            }
         }
                     
         if (usingBatchUpdates)
         {
            int[] rowsReference = psUpdateReference.executeBatch();
            
            if (trace)
            {
               logBatchUpdate(updateMessageRefNotLoaded, rowsReference, "updated");
            }
                        
            psUpdateReference.close();
            psUpdateReference = null;
         }
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (psUpdateReference != null)
         {
            try
            {
               psUpdateReference.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable t)
            {
            }
         }
         wrap.end();
      }
      
   }
   
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
         try
         {
            Message m = ref.getMessage();
                                    
            psReference = conn.prepareStatement(insertMessageRef);
            
            //Add the reference
            addReference(channelID, ref, psReference, true);
            
            int rows = psReference.executeUpdate();
            
            if (trace)
            {
               log.trace("Inserted " + rows + " rows");
            }
               
            synchronized (m)
            {                 
               m.incPersistentChannelCount();
               
               if (m.getPersistentChannelCount() == 1)
               {
                  //First time so persist the message
                  psMessage = conn.prepareStatement(insertMessage);
                  
                  storeMessage(m, psMessage);        
               }
               else
               {
                  //Update the message's channel count
                  psMessage = conn.prepareStatement(updateMessageChannelCount);
                  
                  updateMessageChannelCount(m, psMessage);
               }
                              
               rows = psMessage.executeUpdate();
               if (trace)
               {
                  log.trace("Inserted/updated " + rows + " rows");
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
            if (psReference != null)
            {
               try
               {
                  psReference.close();
               }
               catch (Throwable t)
               {
               }
            }
            if (psMessage != null)
            {
               try
               {
                  psMessage.close();
               }
               catch (Throwable t)
               {
               }
            }
            if (conn != null)
            {
               try
               {
                  conn.close();
               }
               catch (Throwable t)
               {
               }
            }
            wrap.end();
         }      
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
         PreparedStatement psMessage = null;
         
         Connection conn = ds.getConnection();
         try
         {
            Message m = ref.getMessage();
                                     
            psReference = conn.prepareStatement(deleteMessageRef);
            
            //Remove the message reference
            removeReference(channelID, ref, psReference);
            
            int rows = psReference.executeUpdate();
            
            if (trace)
            {
               log.trace("Deleted " + rows + " rows");
            }
               
            synchronized (m)
            {               
               m.decPersistentChannelCount();
               
               if (m.getPersistentChannelCount() == 0)
               {
                  //No other channels have a reference so we can delete the message
                  psMessage = conn.prepareStatement(deleteMessage);
                                    
                  removeMessage(m, psMessage);
               }
               else
               {
                  //Other channel(s) still have hold references so update the channel count
                  psMessage = conn.prepareStatement(updateMessageChannelCount);
                  
                  updateMessageChannelCount(m, psMessage);
               }
               
               
               rows = psMessage.executeUpdate();
               
               if (trace)
               {
                  log.trace("Delete/updated " + rows + " rows");
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
            if (psReference != null)
            {
               try
               {
                  psReference.close();
               }
               catch (Throwable t)
               {
               }
            }
            if (psMessage != null)
            {
               try
               {
                  psMessage.close();
               }
               catch (Throwable t)
               {
               }
            }
            if (conn != null)
            {
               try
               {
                  conn.close();
               }
               catch (Throwable t)
               {
               }
            }
            wrap.end();
         }      
      }
   }
   
   public void removeAllChannelData(long channelID) throws Exception
   {
      if (trace)
      {
         log.trace("removing all references for channel " + channelID);
      }
      
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();
      boolean success = false;
      
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(deleteChannelMessageRefs);
         
         ps.setLong(1, channelID);
         
         ps.executeUpdate();
         success = true;
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (trace)
         {
            String s = JDBCUtil.statementToString(deleteChannelMessageRefs, new Long(channelID));
            log.trace(s + (success ? " successful" : "failed"));
         }
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }
   }
   
   public long getMaxOrdering(long channelID) throws Exception
   {
      if (trace)
      {
         log.trace("getting max ordering for channel " + channelID);
      }
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(selectMaxOrdering);
         ps.setLong(1, channelID);
         
         rs = ps.executeQuery();
         
         rs.next();
         
         long maxOrdering = rs.getLong(1);
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(selectMaxOrdering, new Long(channelID)));
         }
         
         return maxOrdering;
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (rs != null)
         {
            try
            {
               rs.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }
   }
   
   public List retrievePreparedTransactions() throws Exception
   {
      Connection conn = null;
      Statement st = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         List transactions = new ArrayList();
         
         conn = ds.getConnection();
         st = conn.createStatement();
         rs = st.executeQuery(selectPreparedTransactions);
         
         while (rs.next())
         {
            byte[] branchQual = rs.getBytes(2);
            int formatId = rs.getInt(3);
            byte[] globalTxId = rs.getBytes(4);
            Xid xid = new XidImpl(branchQual, formatId, globalTxId);
            
            transactions.add(xid);
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
         if (rs != null)
         {
            try
            {
               rs.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (st != null)
         {
            try
            {
               st.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }
   }
   
   /*
    
    Reference counting code commented out until 1.2
    
    public int getMessageReferenceCount(Serializable messageID) throws Exception
    {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    TransactionWrapper wrap = new TransactionWrapper();
    
    try
    {
    conn = ds.getConnection();
    
    ps = conn.prepareStatement(selectReferenceCount);
    ps.setString(1, (String)messageID);
    
    rs = ps.executeQuery();
    
    int count = 0;
    if (rs.next())
    {
    count = rs.getInt(1);
    }
    
    if (trace) { log.trace(JDBCUtil.statementToString(selectReferenceCount, messageID) + " returned " + (count == 0 ? "no rows" : Integer.toString(count))); }
    
    return count;
    }
    catch (Exception e)
    {
    wrap.exceptionOccurred();
    throw e;
    }
    finally
    {
    if (rs != null)
    {
    try
    {
    rs.close();
    }
    catch (Throwable e)
    {}
    }
    if (ps != null)
    {
    try
    {
    ps.close();
    }
    catch (Throwable e)
    {}
    }
    if (conn != null)
    {
    try
    {
    conn.close();
    }
    catch (Throwable e)
    {}
    }
    wrap.end();
    }
    }
    
    */
   
   // Public --------------------------------------------------------
   /**
    * Managed attribute.
    */
   public void setDataSource(String dataSourceJNDIName) throws Exception
   {
      this.dataSourceJNDIName = dataSourceJNDIName;
   }
   
   /**
    * Managed attribute.
    */
   public String getDataSource()
   {
      return dataSourceJNDIName;
   }
   
   /**
    * Managed attribute.
    */
   public void setTransactionManager(ObjectName tmObjectName) throws Exception
   {
      this.tmObjectName = tmObjectName;
      
      TransactionManagerServiceMBean tms = (TransactionManagerServiceMBean) MBeanServerInvocationHandler
      .newProxyInstance(getServer(), tmObjectName, TransactionManagerServiceMBean.class, false);
      
      tm = tms.getTransactionManager();
   }
   
   /**
    * Managed attribute.
    */
   public ObjectName getTransactionManager()
   {
      return tmObjectName;
   }
   
   /**
    * Managed attribute.
    */
   public void setChannelMapper(ObjectName cmObjectName) throws Exception
   {
      this.cmObjectName = cmObjectName;
   }
   
   /**
    * Managed attribute.
    */
   public ObjectName getChannelMapper()
   {
      return cmObjectName;
   }
   
   public String getSqlProperties()
   {
      try
      {
         ByteArrayOutputStream boa = new ByteArrayOutputStream();
         sqlProperties.store(boa, "");
         return new String(boa.toByteArray());
      }
      catch (IOException shouldnothappen)
      {
         return "";
      }
   }
   
   public void setSqlProperties(String value)
   {
      try
      {
         ByteArrayInputStream is = new ByteArrayInputStream(value.getBytes());
         sqlProperties = new Properties();
         sqlProperties.load(is);
      }
      catch (IOException shouldnothappen)
      {
      }
   }
   
   public void setSqlProperties(Properties props)
   {
      sqlProperties = new Properties(props);
   }
   
   /**
    * Managed attribute.
    */
   public boolean isCreateTablesOnStartup() throws Exception
   {
      return createTablesOnStartup;
   }
   
   /**
    * Managed attribute.
    */
   public void setCreateTablesOnStartup(boolean b) throws Exception
   {
      createTablesOnStartup = b;
   }
   
   /**
    * Managed attribute.
    */
   public boolean isUsingBatchUpdates() throws Exception
   {
      return usingBatchUpdates;
   }
   
   /**
    * Managed attribute.
    */
   public void setUsingBatchUpdates(boolean b) throws Exception
   {
      usingBatchUpdates = b;
   }
   
   public int getMaxParams()
   {
      return maxParams;
   }
   
   public void setMaxParams(int maxParams)
   {
      this.maxParams = maxParams;
   }
   
   public String toString()
   {
      return "JDBCPersistenceManager[" + Integer.toHexString(hashCode()) + "]";
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void createSchema() throws Exception
   {
      Connection conn = null;
      TransactionWrapper tx = new TransactionWrapper();
      
      log.info("creating tables");
      
      try
      {
         conn = ds.getConnection();
         
         try
         {
            conn.createStatement().executeUpdate(createTransaction);
            if (trace)
            {
               log.trace(createTransaction + " succeeded");
            }
         }
         catch (SQLException e)
         {
            log.debug(createTransaction + " failed!", e);
         }
         
         try
         {
            conn.createStatement().executeUpdate(createMessageReference);
            if (trace)
            {
               log.trace(createMessageReference + " succeeded");
            }
         }
         catch (SQLException e)
         {
            log.debug(createMessageReference + " failed!", e);
         }
         
         try
         {
            conn.createStatement().executeUpdate(createIdxMessageRefTx);
            if (trace)
            {
               log.trace(createIdxMessageRefTx + " succeeded");
            }
         }
         catch (SQLException e)
         {
            log.debug(createIdxMessageRefTx + " failed!", e);
         }
         
         try
         {
            conn.createStatement().executeUpdate(createIdxMessageRefOrd);
            if (trace)
            {
               log.trace(createIdxMessageRefOrd + " succeeded");
            }
         }
         catch (SQLException e)
         {
            log.debug(createIdxMessageRefOrd + " failed!", e);
         }
         
         try
         {
            conn.createStatement().executeUpdate(createIdxMessageRefMessageId);
            if (trace)
            {
               log.trace(createIdxMessageRefMessageId + " succeeded");
            }
         }
         catch (SQLException e)
         {
            log.debug(createIdxMessageRefOrd + " failed!", e);
         }
         
         try
         {
            conn.createStatement().executeUpdate(createMessage);
            if (trace)
            {
               log.trace(createMessage + " succeeded");
            }
         }
         catch (SQLException e)
         {
            log.debug(createMessage + " failed!", e);
         }
         
         try
         {
            conn.createStatement().executeUpdate(createCounter);
            if (trace)
            {
               log.trace(createCounter + " succeeded");
            }
         }
         catch (SQLException e)
         {
            log.debug(createCounter + " failed!", e);
         }
      }
      finally
      {
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable t)
            {
            }
         }
         tx.end();
      }
   }
   
   protected void initSqlProperties()
   {
      insertMessageRef = sqlProperties.getProperty("INSERT_MESSAGE_REF", insertMessageRef);
      deleteMessageRef = sqlProperties.getProperty("DELETE_MESSAGE_REF", deleteMessageRef);
      updateMessageRef = sqlProperties.getProperty("UPDATE_MESSAGE_REF", updateMessageRef);
      updateMessageRefNotLoaded = sqlProperties.getProperty("UPDATE_MESSAGE_REF_NOT_LOADED", updateMessageRefNotLoaded);                
      deleteChannelMessageRefs = sqlProperties.getProperty("DELETE_CHANNEL_MESSAGE_REFS", deleteChannelMessageRefs);
      loadReferenceInfo = sqlProperties.getProperty("LOAD_REFERENCE_INFO2", loadReferenceInfo);
      messageIdColumn = sqlProperties.getProperty("MESSAGE_ID_COLUMN", messageIdColumn);
      selectMaxOrdering = sqlProperties.getProperty("SELECT_MAX_ORDERING", selectMaxOrdering);
      selectCountReferences = sqlProperties.getProperty("SELECT_COUNT_REFERENCES", selectCountReferences);
      updateReliableRefs = sqlProperties.getProperty("UPDATE_RELIABLE_REFERENCES", updateReliableRefs);
      removeAllNonReliableRefs = sqlProperties.getProperty("REMOVE_ALL_NONRELIABLE_REFS", removeAllNonReliableRefs);
      removeAllNonReliableMessages = sqlProperties.getProperty("REMOVE_ALL_NONRELIABLE_MSGS", removeAllNonReliableMessages); 
      updateAllReliableRefs = sqlProperties.getProperty("UPDATE_ALL_RELIABLE_REFS", updateAllReliableRefs);            
      createTransaction = sqlProperties.getProperty("CREATE_TRANSACTION", createTransaction);
      createMessageReference = sqlProperties.getProperty("CREATE_MESSAGE_REF", createMessageReference);
      insertTransaction = sqlProperties.getProperty("INSERT_TRANSACTION", insertTransaction);
      deleteTransaction = sqlProperties.getProperty("DELETE_TRANSACTION", deleteTransaction);
      selectPreparedTransactions = sqlProperties.getProperty("SELECT_PREPARED_TRANSACTIONS", selectPreparedTransactions);
      createIdxMessageRefTx = sqlProperties.getProperty("CREATE_IDX_MESSAGE_REF_TX", createIdxMessageRefTx);
      createIdxMessageRefOrd = sqlProperties.getProperty("CREATE_IDX_MESSAGE_REF_ORD", createIdxMessageRefOrd);
      createIdxMessageRefMessageId = sqlProperties.getProperty("CREATE_IDX_MESSAGE_REF_MESSAGEID", createIdxMessageRefMessageId);      
      commitMessageRef1 = sqlProperties.getProperty("COMMIT_MESSAGE_REF1", commitMessageRef1);
      commitMessageRef2 = sqlProperties.getProperty("COMMIT_MESSAGE_REF2", commitMessageRef2);
      rollbackMessageRef1 = sqlProperties.getProperty("ROLLBACK_MESSAGE_REF1", rollbackMessageRef1);
      rollbackMessageRef2 = sqlProperties.getProperty("ROLLBACK_MESSAGE_REF2", rollbackMessageRef2);
      deleteMessage = sqlProperties.getProperty("DELETE_MESSAGE", deleteMessage);
      insertMessage = sqlProperties.getProperty("INSERT_MESSAGE", insertMessage);
      createMessage = sqlProperties.getProperty("CREATE_MESSAGE", createMessage);
      updateMessageChannelCount = sqlProperties.getProperty("UPDATE_MESSAGE_CHANNEL_COUNT", updateMessageChannelCount);
      createCounter = sqlProperties.getProperty("CREATE_COUNTER", createCounter);
      updateCounter = sqlProperties.getProperty("UPDATE_COUNTER", updateCounter);
      selectCounter = sqlProperties.getProperty("SELECT_COUNTER", selectCounter);
      insertCounter = sqlProperties.getProperty("INSERT_COUNTER", insertCounter);
      //selectReferenceCount = sqlProperties.getProperty("SELECT_REF_COUNT", selectReferenceCount);
      //updateReferenceCount = sqlProperties.getProperty("UPDATE_REF_COUNT", updateReferenceCount);       
   }
   
   protected TransactionCallback getCallback(Transaction tx)
   {
      TransactionCallback callback = (TransactionCallback) tx.getKeyedCallback(this);

      if (callback == null)
      {
         callback = new TransactionCallback(tx);

         tx.addKeyedCallback(callback, this);
      }

      return callback;
   }
   
   /*
    * Remove any non-persistent message data
    * Update any persistent refs to LOADED='N'
    */
   protected void resetMessageData() throws Exception
   {
      if (trace) { log.trace("Removing all non-persistent data"); }
      
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(removeAllNonReliableRefs);
         
         int rows = ps.executeUpdate();
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(removeAllNonReliableRefs)
                  + " removed " + rows + " rows");
         }
         
         ps.close();
         
         ps = null;
         
         ps = conn.prepareStatement(removeAllNonReliableMessages);
         
         rows = ps.executeUpdate();
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(removeAllNonReliableMessages)
                  + " removed " + rows + " rows");
         }
         
         ps.close();
         
         ps = null;
         
         ps = conn.prepareStatement(updateAllReliableRefs);
         
         rows = ps.executeUpdate();
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(updateAllReliableRefs)
                  + " updated " + rows + " rows");
         }
         
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }
   }
   
   protected void handleBeforeCommit1PC(List refsToAdd, List refsToRemove, Transaction tx)
      throws Exception
   {
      //For one phase we simply add rows corresponding to the refs
      //and remove rows corresponding to the deliveries in one jdbc tx
      //We also need to store or remove messages as necessary, depending
      //on whether they've already been stored or still referenced by other
      //channels
         
      Connection conn = null;
      PreparedStatement psReference = null;
      PreparedStatement psInsertMessage = null;
      PreparedStatement psUpdateMessage = null;
      PreparedStatement psDeleteMessage = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         //First the adds
         
         Iterator iter = refsToAdd.iterator();
         
         boolean batch = usingBatchUpdates && refsToAdd.size() > 0;
         
         boolean messageInsertsInBatch = false;
         boolean messageUpdatesInBatch = false;
         
         if (batch)
         {
            psReference = conn.prepareStatement(insertMessageRef);
            psInsertMessage = conn.prepareStatement(insertMessage);
            psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
         }
         
         while (iter.hasNext())
         {
            ChannelRefPair pair = (ChannelRefPair) iter.next();
            
            MessageReference ref = pair.ref;
                                                
            if (!batch)
            {
               psReference = conn.prepareStatement(insertMessageRef);
            }
            
            //Now store the reference
            addReference(pair.channelId, ref, psReference, true);
                        
            if (batch)
            {
               psReference.addBatch();
            }
            else
            {
               int rows = psReference.executeUpdate();
               
               if (trace)
               {
                  log.trace("Inserted " + rows + " rows");
               }
               
               psReference.close();
               psReference = null;
            }
            
            Message m = ref.getMessage();        
            
            if (!batch)
            {
               psInsertMessage = conn.prepareStatement(insertMessage);
               psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
            }
             
            synchronized (m)
            {                     
               m.incPersistentChannelCount();                                             
               
               boolean added;
               if (m.getPersistentChannelCount() == 1)
               {
                  //First time so add message
                  storeMessage(m, psInsertMessage);
                  
                  added = true;
               }
               else
               {
                  //Update message channel count
                  updateMessageChannelCount(m, psUpdateMessage);
                  
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
                     int rows = psInsertMessage.executeUpdate();
                     
                     if (trace)
                     {
                        log.trace("Inserted " + rows + " rows");
                     }
                  }
                  else
                  {
                     int rows = psUpdateMessage.executeUpdate();
                     
                     if (trace)
                     {
                        log.trace("Updated " + rows + " rows");
                     }
                  }
                  psInsertMessage.close();
                  psInsertMessage = null;
                  psUpdateMessage.close();
                  psUpdateMessage = null;
               }
            }
         }
         
         if (batch)
         {
            int[] rowsReference = psReference.executeBatch();
            
            if (trace)
            {
               logBatchUpdate(insertMessageRef, rowsReference, "inserted");
            }
            
            if (messageInsertsInBatch)
            {
               int[] rowsMessage = psInsertMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(insertMessage, rowsMessage, "inserted");
               }
            }
            if (messageUpdatesInBatch)
            {
               int[] rowsMessage = psUpdateMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(updateMessageChannelCount, rowsMessage, "updated");
               }
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
         
         batch = usingBatchUpdates && refsToRemove.size() > 0;
         boolean messageDeletionsInBatch = false;
         messageUpdatesInBatch = false;
         psReference = null;
         psUpdateMessage = null;
         psDeleteMessage = null;
         
         if (batch)
         {
            psReference = conn.prepareStatement(deleteMessageRef);
            psDeleteMessage = conn.prepareStatement(deleteMessage);
            psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
         }
         
         while (iter.hasNext())
         {
            ChannelRefPair pair = (ChannelRefPair) iter.next();
            
            if (!batch)
            {
               psReference = conn.prepareStatement(deleteMessageRef);
            }
            
            removeReference(pair.channelId, pair.ref, psReference);
            
            if (batch)
            {
               psReference.addBatch();
            }
            else
            {
               int rows = psReference.executeUpdate();
               
               if (trace)
               {
                  log.trace("Deleted " + rows + " rows");
               }
               
               psReference.close();
               psReference = null;
            }
            
            if (!batch)
            {
               psDeleteMessage = conn.prepareStatement(deleteMessage);
               psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
            }
            
            Message m = pair.ref.getMessage();
            
            synchronized (m)
            {                  
               m.decPersistentChannelCount();
                               
               boolean removed;
               
               if (m.getPersistentChannelCount() == 0)
               {
                  //No more refs - message can be deleted
                  removeMessage(m, psDeleteMessage);
                     
                  removed = true;
               }
               else
               {
                  //Update channel count for message
                  updateMessageChannelCount(m, psUpdateMessage);
                  
                  removed = false;
               }
               
               if (batch)
               {
                  if (removed)
                  {
                     psDeleteMessage.addBatch();
                     messageDeletionsInBatch = true;
                  }
                  else
                  {
                     psUpdateMessage.addBatch();
                     messageUpdatesInBatch = true;                        
                  }
               }
               else
               {
                  if (removed)
                  {
                     int rows = psDeleteMessage.executeUpdate();
                     
                     if (trace)
                     {
                        log.trace("Deleted " + rows + " rows");
                     }
                  }
                  else
                  {
                     int rows = psUpdateMessage.executeUpdate();
                     
                     if (trace)
                     {
                        log.trace("Updated " + rows + " rows");
                     }
                  }
                  psDeleteMessage.close();
                  psDeleteMessage = null;
                  psUpdateMessage.close();
                  psUpdateMessage = null;
               }
            }
         }
         
         if (batch)
         {
            int[] rowsReference = psReference.executeBatch();
            
            if (trace)
            {
               logBatchUpdate(deleteMessageRef, rowsReference, "deleted");
            }
            
            if (messageDeletionsInBatch)
            {
               int[] rowsMessage = psDeleteMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(deleteMessage, rowsMessage, "deleted");
               }
            }
            if (messageUpdatesInBatch)
            {
               int[] rowsMessage = psUpdateMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(updateMessageChannelCount, rowsMessage, "updated");
               }
            }
            
            psReference.close();
            psReference = null;
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
         if (psReference != null)
         {
            try
            {
               psReference.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (psInsertMessage != null)
         {
            try
            {
               psInsertMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (psUpdateMessage != null)
         {
            try
            {
               psUpdateMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (psDeleteMessage != null)
         {
            try
            {
               psDeleteMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }
   }
   
   protected void handleBeforeCommit2PC(List refsToRemove, Transaction tx)
      throws Exception
   {      

      Connection conn = null;
      PreparedStatement psUpdateMessage = null;
      PreparedStatement psDeleteMessage = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
                  
         //2PC commit
         
         //First we commit any refs in state "+" to "C" and delete any
         //refs in state "-", then we
         //remove any messages due to refs we just removed
         //if they're not referenced elsewhere
         
         commitPreparedTransaction(tx, conn);
         
         boolean batch = usingBatchUpdates && refsToRemove.size() > 0;
         boolean messageDeletionsInBatch = false;
         boolean messageUpdatesInBatch = false;
      
         if (batch)
         {
            psDeleteMessage = conn.prepareStatement(deleteMessage);
            psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
         }
                  
         Iterator iter = refsToRemove.iterator();
         while (iter.hasNext())
         {
            ChannelRefPair pair = (ChannelRefPair) iter.next();
            
            MessageReference ref = pair.ref;
            
            if (!batch)
            {
               psDeleteMessage = conn.prepareStatement(deleteMessage);
               psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
            }
            
            Message m = ref.getMessage();
            
            synchronized (m)
            {                  
               //We may need to remove the message itself
               
               m.decPersistentChannelCount();
               
               boolean removed;
               if (m.getPersistentChannelCount() == 0)
               {
                  //We can remove the message
                  removeMessage(m, psDeleteMessage);
                  
                  removed = true;
               }
               else
               {
                  //Decrement channel count
                  updateMessageChannelCount(m, psUpdateMessage);
                  
                  removed = false;
               }
                              
               if (batch)
               {
                  if (removed)
                  {
                     psDeleteMessage.addBatch();
                     
                     messageDeletionsInBatch = true;
                  }
                  else
                  {
                     psUpdateMessage.addBatch();
                     
                     messageUpdatesInBatch = true;
                  }
               }
               else
               {
                  if (removed)
                  {
                     int rows = psDeleteMessage.executeUpdate();
                     
                     if (trace)
                     {
                        log.trace("Deleted " + rows + " rows");
                     }
                  }
                  else
                  {
                     int rows = psUpdateMessage.executeUpdate();
                     
                     if (trace)
                     {
                        log.trace("Updated " + rows + " rows");
                     }
                  }
                  psDeleteMessage.close();
                  psDeleteMessage = null;
                  psUpdateMessage.close();
                  psUpdateMessage = null;
               }
            }
         }
         
         if (batch)
         {
            if (messageDeletionsInBatch)
            {
               int[] rows = psDeleteMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(deleteMessage, rows, "deleted");
               }
               
               psDeleteMessage.close();
               psDeleteMessage = null;
            }
            if (messageUpdatesInBatch)
            {
               int[] rows = psUpdateMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(updateMessageChannelCount, rows, "updated");
               }
               
               psUpdateMessage.close();
               psUpdateMessage = null;
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
         if (psDeleteMessage != null)
         {
            try
            {
               psDeleteMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (psUpdateMessage != null)
         {
            try
            {
               psUpdateMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }
   }
   
   protected void handleBeforePrepare(List refsToAdd, List refsToRemove, Transaction tx) throws Exception
   {
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
         conn = ds.getConnection();
         
         //Insert the tx record
         if (!refsToAdd.isEmpty() || !refsToRemove.isEmpty())
         {
            addTXRecord(conn, tx);
         }
         
         Iterator iter = refsToAdd.iterator();
         
         boolean batch = usingBatchUpdates && refsToAdd.size() > 1;
         boolean messageInsertsInBatch = false;
         boolean messageUpdatesInBatch = false;
         if (batch)
         {
            psReference = conn.prepareStatement(insertMessageRef);
            psInsertMessage = conn.prepareStatement(insertMessage);
            psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
         }
         
         while (iter.hasNext())
         {
            ChannelRefPair pair = (ChannelRefPair) iter.next();
            
            if (!batch)
            {
               psReference = conn.prepareStatement(insertMessageRef);
            }
            
            prepareToAddReference(pair.channelId, pair.ref, tx, psReference);
            
            if (batch)
            {
               psReference.addBatch();
            }
            else
            {
               int rows = psReference.executeUpdate();
               
               if (trace)
               {
                  log.trace("Inserted " + rows + " rows");
               }
               
               psReference.close();
               psReference = null;
            }
            
            if (!batch)
            {
               psInsertMessage = conn.prepareStatement(insertMessage);
               psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
            }
            
            Message m = pair.ref.getMessage();
            
            synchronized (m)
            {
               m.incPersistentChannelCount();
               
               boolean added;
               if (m.getPersistentChannelCount() == 1)
               {
                  //First time so persist the message
                  storeMessage(m, psInsertMessage);
                  
                  added = true;
               }
               else
               {
                  //Update message channel count
                  updateMessageChannelCount(m, psUpdateMessage);
                  
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
                     int rows = psInsertMessage.executeUpdate();
                     
                     if (trace)
                     {
                        log.trace("Inserted " + rows + " rows");
                     }
                  }
                  else
                  {
                     int rows = psUpdateMessage.executeUpdate();
                     
                     if (trace)
                     {
                        log.trace("Updated " + rows + " rows");
                     }
                  }
                  psInsertMessage.close();
                  psInsertMessage = null;
                  psUpdateMessage.close();
                  psUpdateMessage = null;
               }
            }
         }
         
         if (batch)
         {
            int[] rowsReference = psReference.executeBatch();
            
            if (trace)
            {
               logBatchUpdate(insertMessageRef, rowsReference, "inserted");
            }
            
            if (messageInsertsInBatch)
            {
               int[] rowsMessage = psInsertMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(insertMessage, rowsMessage, "inserted");
               }
            }
            if (messageUpdatesInBatch)
            {
               int[] rowsMessage = psUpdateMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(updateMessageChannelCount, rowsMessage, "updated");
               }
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
            psReference = conn.prepareStatement(updateMessageRef);
         }
         
         while (iter.hasNext())
         {
            ChannelRefPair pair = (ChannelRefPair) iter.next();
            
            if (!batch)
            {
               psReference = conn.prepareStatement(updateMessageRef);
            }
            
            prepareToRemoveReference(pair.channelId, pair.ref, tx, psReference);
            
            if (batch)
            {
               psReference.addBatch();
            }
            else
            {
               int rows = psReference.executeUpdate();
               
               if (trace)
               {
                  log.trace("updated " + rows + " rows");
               }
               
               psReference.close();
               psReference = null;
            }
         }
         
         if (batch)
         {
            int[] rows = psReference.executeBatch();
            
            if (trace)
            {
               logBatchUpdate(updateMessageRef, rows, "updated");
            }
            
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
         if (psReference != null)
         {
            try
            {
               psReference.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (psInsertMessage != null)
         {
            try
            {
               psInsertMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (psUpdateMessage != null)
         {
            try
            {
               psUpdateMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
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
      
      try
      {
         conn = ds.getConnection();
         
         rollbackPreparedTransaction(tx, conn);
         
         Iterator iter = refsToAdd.iterator();
         
         boolean batch = usingBatchUpdates && refsToAdd.size() > 1;
         boolean messageDeletionsInBatch = false;
         boolean messageUpdatesInBatch = false;
         if (batch)
         {
            psDeleteMessage = conn.prepareStatement(deleteMessage);
            psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
         }
                                 
         while (iter.hasNext())
         {
            ChannelRefPair pair = (ChannelRefPair) iter.next();
            
            if (!batch)
            {
               psDeleteMessage = conn.prepareStatement(deleteMessage);
               psUpdateMessage = conn.prepareStatement(updateMessageChannelCount);
            }
            
            Message m = pair.ref.getMessage();
            
            synchronized (m)
            {                  
               //We may need to remove the message for messages added during the prepare stage
               
               m.decPersistentChannelCount();
               
               boolean removed;
               if (m.getPersistentChannelCount() == 0)
               {
                  //remove message
                  removeMessage(m, psDeleteMessage);
                  
                  removed = true;                    
               }
               else
               {
                  //update message channel count
                  updateMessageChannelCount(m, psUpdateMessage);
                  
                  removed = false;
               }
                                 
               if (batch)
               {
                  if (removed)
                  {
                     psDeleteMessage.addBatch();
                     messageDeletionsInBatch = true;
                  }
                  else
                  {
                     psUpdateMessage.addBatch();
                     messageUpdatesInBatch = true;
                  }
               }
               else
               {
                  if (removed)
                  {
                     int rows = psDeleteMessage.executeUpdate();
                     
                     if (trace)
                     {
                        log.trace("deleted " + rows + " rows");
                     }
                  }
                  else
                  {
                     int rows = psUpdateMessage.executeUpdate();
                     
                     if (trace)
                     {
                        log.trace("updated " + rows + " rows");
                     }
                  }
                  psDeleteMessage.close();
                  psDeleteMessage = null;
                  psUpdateMessage.close();
                  psUpdateMessage = null;
               }
            }
         }
         
         if (batch)
         {
            if (messageDeletionsInBatch)
            {
               int[] rows = psDeleteMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(deleteMessage, rows, "deleted");
               }
               
               psDeleteMessage.close();
               psDeleteMessage = null;
            }
            if (messageUpdatesInBatch)
            {
               int[] rows = psUpdateMessage.executeBatch();
               
               if (trace)
               {
                  logBatchUpdate(updateMessageChannelCount, rows, "updated");
               }
               
               psUpdateMessage.close();
               psUpdateMessage = null;
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
         if (psDeleteMessage != null)
         {
            try
            {
               psDeleteMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (psUpdateMessage != null)
         {
            try
            {
               psUpdateMessage.close();
            }
            catch (Throwable t)
            {
            }
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable e)
            {
            }
         }
         wrap.end();
      }      
   }
   
   
   protected void addTXRecord(Connection conn, Transaction tx) throws Exception
   {
      if (trace)
      {
         log.trace("Inserting tx record for " + tx);
      }
      
      PreparedStatement ps = null;
      String statement = "UNDEFINED";
      int rows = -1;
      int formatID = -1;
      try
      {
         statement = insertTransaction;
         
         ps = conn.prepareStatement(statement);
         
         ps.setLong(1, tx.getId());
         
         Xid xid = tx.getXid();
         formatID = xid.getFormatId();
         ps.setBytes(2, xid.getBranchQualifier());
         ps.setInt(3, formatID);
         ps.setBytes(4, xid.getGlobalTransactionId());
         
         rows = ps.executeUpdate();
         
      }
      finally
      {
         if (trace)
         {
            String s = JDBCUtil.statementToString(insertTransaction, new Long(tx.getId()), "<byte-array>",
                  new Integer(formatID), "<byte-array>");
            log.trace(s + (rows == -1 ? " failed!" : " inserted " + rows + " row(s)"));
         }
         try
         {
            if (ps != null)
            {
               ps.close();
            }
         }
         catch (Throwable e)
         {
            //Ignore
         }
      }
   }
   
   protected void removeTXRecord(Connection conn, Transaction tx) throws SQLException
   {
      PreparedStatement ps = null;
      try
      {
         ps = conn.prepareStatement(deleteTransaction);
         
         ps.setLong(1, tx.getId());
         
         int rows = ps.executeUpdate();
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(deleteTransaction, new Long(tx.getId())) + " removed " + rows + " row(s)");
         }
      }
      finally
      {
         try
         {
            if (ps != null)
            {
               ps.close();
            }
         }
         catch (Throwable e)
         {
            //Ignore
         }
      }
   }  
   
   protected void addReference(long channelID, MessageReference ref, PreparedStatement ps, boolean loaded) throws Exception
   {
      if (trace)
      {
         log.trace("adding " + ref + " to channel " + channelID);
      }
      
      ps.setLong(1, channelID);
      ps.setLong(2, ref.getMessageID());
      ps.setNull(3, java.sql.Types.BIGINT);
      ps.setString(4, "C");
      ps.setLong(5, ref.getOrdering());
      ps.setInt(6, ref.getDeliveryCount());
      ps.setString(7, ref.isReliable() ? "Y" : "N");
      ps.setString(8, loaded ? "Y" : "N");
   }
   
   protected void removeReference(long channelID, MessageReference ref, PreparedStatement ps) throws Exception
   {
      if (trace)
      {
         log.trace("removing " + ref + " from channel " + channelID);
      }
      
      ps.setLong(1, ref.getMessageID());
      ps.setLong(2, channelID);      
   }
   
   protected void prepareToAddReference(long channelID, MessageReference ref, Transaction tx, PreparedStatement ps)
   throws Exception
   {
      if (trace)
      {
         log.trace("adding " + ref + " to channel " + channelID
               + (tx == null ? " non-transactionally" : " on transaction: " + tx));
      }
      
      ps.setLong(1, channelID);
      ps.setLong(2, ref.getMessageID());
      ps.setLong(3, tx.getId());
      ps.setString(4, "+");
      ps.setLong(5, ref.getOrdering());
      ps.setInt(6, ref.getDeliveryCount());
      ps.setString(7, ref.isReliable() ? "Y" : "N");
      ps.setString(8, "Y");
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
      ps.setLong(2, ref.getMessageID());
      ps.setLong(3, channelID);           
   }
   
   protected void commitPreparedTransaction(Transaction tx, Connection conn) throws Exception
   {
      PreparedStatement ps = null;
      
      try
      {
         ps = conn.prepareStatement(commitMessageRef1);
         
         ps.setLong(1, tx.getId());        
         
         int rows = ps.executeUpdate();
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(commitMessageRef1, new Long(tx.getId())) + " removed " + rows + " row(s)");
         }
         
         ps.close();
         ps = conn.prepareStatement(commitMessageRef2);
         ps.setLong(1, tx.getId());         
         
         rows = ps.executeUpdate();
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(commitMessageRef2, null, new Long(tx.getId())) + " updated " + rows
                  + " row(s)");
         }
         
         removeTXRecord(conn, tx);
      }
      finally
      {
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
      }
   }
   
   protected void rollbackPreparedTransaction(Transaction tx, Connection conn) throws Exception
   {
      PreparedStatement ps = null;
      
      try
      {
         ps = conn.prepareStatement(rollbackMessageRef1);
         
         ps.setLong(1, tx.getId());         
         
         int rows = ps.executeUpdate();
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(rollbackMessageRef1, new Long(tx.getId())) + " removed " + rows + " row(s)");
         }
         
         ps.close();
         
         ps = conn.prepareStatement(rollbackMessageRef2);
         ps.setLong(1, tx.getId());
         
         rows = ps.executeUpdate();
         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(rollbackMessageRef2, null, new Long(tx.getId())) + " updated " + rows
                  + " row(s)");
         }
         
         removeTXRecord(conn, tx);
      }
      finally
      {
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable e)
            {
            }
         }
      }
   }
   
   protected byte[] mapToBytes(Map map) throws Exception
   {
      if (map == null || map.isEmpty())
      {
         return null;
      }
      
      final int BUFFER_SIZE = 1024;
      
      JBossObjectOutputStream oos = null;
      
      try
      {
         ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
         
         oos = new JBossObjectOutputStream(bos);
         
         RoutableSupport.writeMap(oos, map, true);
         
         return bos.toByteArray();
      }
      finally
      {
         if (oos != null)
         {
            oos.close();
         }
      }
   }
   
   protected HashMap bytesToMap(byte[] bytes) throws Exception
   {
      if (bytes == null)
      {
         return new HashMap();
      }
      
      JBossObjectInputStream ois = null;
      
      try
      {
         ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
         
         ois = new JBossObjectInputStream(bis);
         
         Map m = RoutableSupport.readMap(ois, true);
         HashMap map;
         if (!(m instanceof HashMap))
         {
            map = new HashMap(m);
         }
         else
         {
            map = (HashMap) m;
         }
         
         return map;
      }
      finally
      {
         if (ois != null)
         {
            ois.close();
         }
      }
   }
   
   protected void updateMessageChannelCount(Message m, PreparedStatement ps) throws Exception
   {
      ps.setInt(1, m.getPersistentChannelCount());
      ps.setLong(2, m.getMessageID());
   }
   
   /**
    * Stores the message in the MESSAGE table.
    */
   protected void storeMessage(Message m, PreparedStatement ps) throws Exception
   {
      /*
       
       Reference counting code commented out until 1.2
       
       // get the reference count from the database
        // TODO Probably this can be done smarter than that incrementing directly in the database
         ps = conn.prepareStatement(selectReferenceCount);
         ps.setString(1, id);
         
         int referenceCount = 0;
         ResultSet rs = ps.executeQuery();
         if (rs.next())
         {
         referenceCount = rs.getInt(1);
         }
         
         if (trace) { log.trace(JDBCUtil.statementToString(selectReferenceCount, id) + " returned " + (referenceCount == 0 ? "no rows" : Integer.toString(referenceCount))); }
         
         if (referenceCount == 0)
         {
         
         */
      
      // physically insert the row in the database
      //First set the fields from org.jboss.messaging.core.Routable
      ps.setLong(1, m.getMessageID());
      ps.setString(2, m.isReliable() ? "Y" : "N");
      ps.setLong(3, m.getExpiration());
      ps.setLong(4, m.getTimestamp());
      ps.setByte(5, m.getPriority());
      
      byte[] bytes = mapToBytes(((MessageSupport) m).getHeaders());
      if (bytes != null)
      {
         setLongVarBinary(ps, 6, bytes);
      }
      else
      {
         ps.setNull(6, Types.LONGVARBINARY);
      }
      
      //Now set the fields from org.jboss.messaging.core.Message
      
      byte[] payload = m.getPayloadAsByteArray();
      if (payload != null)
      {
         setLongVarBinary(ps, 7, payload);
      }
      else
      {
         ps.setNull(7, Types.LONGVARBINARY);
      }
      
      //The number of channels that hold a reference to the message
      ps.setInt(8, m.getPersistentChannelCount());
      
      //Now set the fields from org.joss.jms.message.JBossMessage if appropriate
      
      //FIXME - We are mixing concerns here
      //The basic JDBCPersistencManager should *only* know about core messages - not 
      //JBossMessages - we should subclass JBDCPersistenceManager and the JBossMessage
      //specific code in a subclass
      if (m instanceof JBossMessage)
      {
         JBossMessage jbm = (JBossMessage) m;
         
         ps.setByte(9, jbm.getType());
         ps.setString(10, jbm.getJMSType());
         ps.setString(11, jbm.getJMSCorrelationID());
         ps.setBytes(12, jbm.getJMSCorrelationIDAsBytes());
         
         JBossDestination jbd = (JBossDestination) jbm.getJMSDestination();
         
         CoreDestination dest = cm.getCoreDestination(jbd);
         
         long destId = dest.getId();
         ps.setLong(13, destId);
         
         JBossDestination replyTo = (JBossDestination) jbm.getJMSReplyTo();
         if (replyTo == null)
         {
            ps.setNull(14, Types.BIGINT);
         }
         else
         {
            long replyToId = cm.getCoreDestination(replyTo).getId();
            ps.setLong(14, replyToId);
         }
         
         bytes = mapToBytes(jbm.getJMSProperties());
         if (bytes != null)
         {
            setLongVarBinary(ps, 15, bytes);
         }
         else
         {
            ps.setNull(15, Types.LONGVARBINARY);
         }
      }
            
      //reference count - not currently used (and probably never will be)
      ps.setInt(16, 1);
   }
   
   /**
    * Removes the message from the MESSAGE table.
    */
   protected void removeMessage(Message message, PreparedStatement ps) throws Exception
   {
      /*
       
       Reference counting code commented out until 1.2
       
       // get the reference count from the database
        ps = conn.prepareStatement(selectReferenceCount);
        ps.setString(1, (String)message.getMessageID());
        
        //TODO this can be combined into one query
         int referenceCount = 0;
         ResultSet rs = ps.executeQuery();
         if (rs.next())
         {
         referenceCount = rs.getInt(1);
         }
         
         if (trace) { log.trace(JDBCUtil.statementToString(selectReferenceCount, message.getMessageID()) + " returned " + (referenceCount == 0 ? "no rows" : Integer.toString(referenceCount))); }
         
         if (referenceCount == 0)
         {
         if (trace) { log.trace("no message " + message.getMessageID() + " to delete in the database"); }
         return false;
         }
         else if (referenceCount == 1)
         {
         
         */
      
      // physically delete the row in the database
      ps.setLong(1, message.getMessageID());
      
      /*   
       
       Reference counting code commented out until 1.2   
       
       }
       else
       {
       // decrement the reference count
        ps = conn.prepareStatement(updateReferenceCount);
        ps.setInt(1, --referenceCount);
        ps.setString(2, (String)message.getMessageID());
        
        ps.executeUpdate();
        if (trace) { log.trace(JDBCUtil.statementToString(updateReferenceCount, new Integer(referenceCount), message.getMessageID()) + " executed successfully"); }
        return true;
        }
        
        */
      
   }
     
   protected void setLongVarBinary(PreparedStatement ps, int columnIndex, byte[] bytes) throws Exception
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
         ps.setBytes(columnIndex, bytes);
      }
   }
   
   protected byte[] getLongVarBinary(ResultSet rs, int columnIndex) throws Exception
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
         return rs.getBytes(columnIndex);
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
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
   private class TransactionWrapper
   {
      private javax.transaction.Transaction oldTx;
      
      private TransactionWrapper() throws Exception
      {
         oldTx = tm.suspend();
         
         tm.begin();
      }
      
      private void end() throws Exception
      {
         try
         {
            if (Status.STATUS_MARKED_ROLLBACK == tm.getStatus())
            {
               tm.rollback();
            }
            else
            {
               tm.commit();
            }
         }
         finally
         {
            if (oldTx != null)
            {
               tm.resume(oldTx);
            }
         }
      }
      
      private void exceptionOccurred() throws Exception
      {
         tm.setRollbackOnly();
      }
   }
   
   private static class ChannelRefPair
   {
      private long channelId;
      
      private MessageReference ref;
      
      private ChannelRefPair(long channelId, MessageReference ref)
      {
         this.channelId = channelId;
         
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
   
}
