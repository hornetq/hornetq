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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.TransactionManager;
import javax.transaction.xa.Xid;

import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.message.MessageSupport;
import org.jboss.messaging.core.persistence.JDBCUtil;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.XidImpl;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.system.ServiceMBeanSupport;
import org.jboss.tm.TransactionManagerServiceMBean;
import org.jboss.util.id.GUID;

/**
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

   /* The default DML and DDL will work with HSQLDB */
   
   private static final Logger log = Logger.getLogger(JDBCPersistenceManager.class);
   
   protected static final int BYTES_AS_OBJECT = 0;

   protected static final int BYTES_AS_BYTES = 1;

   protected static final int BYTES_AS_BINARY_STREAM = 2;

   protected static final int BYTES_AS_BLOB = 3; 
   
   protected String insertMessageRef =
      "INSERT INTO MESSAGE_REFERENCE (CHANNELID, MESSAGEID, STOREID, TRANSACTIONID, STATE, ORD) " +
      "VALUES (?, ?, ?, ?, ?, ?)";
   
   protected String deleteMessageRef =
      "DELETE FROM MESSAGE_REFERENCE WHERE CHANNELID=? AND MESSAGEID=? AND STATE='C'";
   
   protected String updateMessageRef =
      "UPDATE MESSAGE_REFERENCE SET TRANSACTIONID=?, STATE='-' " +
      "WHERE CHANNELID=? AND MESSAGEID=? AND STATE='C'";

   protected String commitMessageRef1 =
      "UPDATE MESSAGE_REFERENCE SET STATE='C', TRANSACTIONID = NULL WHERE STATE='+' AND TRANSACTIONID=?";
   
   protected String commitMessageRef2 =
      "DELETE FROM MESSAGE_REFERENCE WHERE STATE='-' AND TRANSACTIONID=?";
   
   protected String rollbackMessageRef1 =
      "DELETE FROM MESSAGE_REFERENCE WHERE TRANSACTIONID=? AND STATE='+'";
   
   protected String rollbackMessageRef2 =
      "UPDATE MESSAGE_REFERENCE SET STATE='C', TRANSACTIONID = NULL WHERE STATE='-' AND TRANSACTIONID=?";
      
   protected String rollbackNonPreparedTx1 =
      "DELETE FROM MESSAGE_REFERENCE WHERE STATE='+' AND TRANSACTIONID IN (SELECT TRANSACTIONID FROM TRANSACTION WHERE STATE IS NULL)";
   
   protected String rollbackNonPreparedTx2 =
      "UPDATE MESSAGE_REFERENCE SET STATE='C', TRANSACTIONID = NULL WHERE STATE='-' AND TRANSACTIONID IN (SELECT TRANSACTIONID FROM TRANSACTION WHERE STATE IS NULL)";
         
   protected String deleteNonPreparedTx = 
      "DELETE FROM TRANSACTION WHERE STATE IS NULL";
   
   protected String deleteChannelMessageRefs =
      "DELETE FROM MESSAGE_REFERENCE WHERE CHANNELID=?";
   
   protected String selectChannelMessageRefs =
      "SELECT MESSAGEID FROM MESSAGE_REFERENCE WHERE CHANNELID=? AND STOREID=? AND STATE <> '+' ORDER BY ORD";
   
   protected String createTransaction =
      "CREATE TABLE TRANSACTION (" +
      "TRANSACTIONID BIGINT GENERATED BY DEFAULT AS IDENTITY, " +
      "BRANCH_QUAL OBJECT, " +
      "FORMAT_ID INTEGER, " +
      "GLOBAL_TXID OBJECT, " +
      "STATE CHAR(1), " +
      "PRIMARY KEY (TRANSACTIONID))";
   
   protected String createMessageReference =
      "CREATE TABLE MESSAGE_REFERENCE (" +
      "CHANNELID VARCHAR(256), " +
      "MESSAGEID VARCHAR(256), " +
      "STOREID VARCHAR(256), " +
      "TRANSACTIONID BIGINT, " +
      "STATE CHAR(1), " +
      "ORD BIGINT, " +
      "PRIMARY KEY(STOREID, CHANNELID, MESSAGEID))";

   protected String insertTransactionXA =
      "INSERT INTO TRANSACTION (TRANSACTIONID, BRANCH_QUAL, FORMAT_ID, GLOBAL_TXID, STATE) " +
      "VALUES(?, ?, ?, ?, 'U')";
   
   protected String prepareTransaction =
      "UPDATE TRANSACTION SET STATE = 'P' WHERE TRANSACTIONID = ?";

   protected String insertTransaction =
      "INSERT INTO TRANSACTION (TRANSACTIONID) VALUES (?)";
   
   protected String deleteTransaction =
      "DELETE FROM TRANSACTION WHERE TRANSACTIONID = ?";
   
   protected String selectPreparedTransactions =
      "SELECT TRANSACTIONID, BRANCH_QUAL, FORMAT_ID, GLOBAL_TXID FROM TRANSACTION " +
      "WHERE STATE='P'";
   
   protected String createIdxMessageRefTx = "CREATE INDEX MESSAGE_REF_TX ON MESSAGE_REFERENCE (STATE, TRANSACTIONID)";

   protected String selectTransactionId = "CALL IDENTITY()";
   
   protected String insertMessage =
      "INSERT INTO MESSAGE (" +
      "MESSAGEID, " +
      "RELIABLE, " +
      "EXPIRATION, " +
      "TIMESTAMP, " +
      "PRIORITY, " +
      "DELIVERYCOUNT, " +
      "COREHEADERS, " +
      "PAYLOAD, " +
      "TYPE, " +
      "JMSTYPE, " +
      "CORRELATIONID, " +
      "DESTINATIONISQUEUE, " +
      "DESTINATION, " +
      "REPLYTOISQUEUE, " +
      "REPLYTO, " +
      "CONNECTIONID, " +
      "JMSPROPERTIES, " +
      "REFERENCECOUNT) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

   protected String deleteMessage =
      "DELETE FROM MESSAGE WHERE MESSAGEID=?";

   protected String selectMessage =
      "SELECT " +
      "MESSAGEID, " +
      "RELIABLE, " +
      "EXPIRATION, " +
      "TIMESTAMP, " +
      "PRIORITY, " +
      "DELIVERYCOUNT, " +
      "COREHEADERS, " +
      "PAYLOAD, " +
      "TYPE, " +
      "JMSTYPE, " +
      "CORRELATIONID, " +
      "DESTINATIONISQUEUE, " +
      "DESTINATION, " +
      "REPLYTOISQUEUE, " +
      "REPLYTO, " +
      "CONNECTIONID, " +
      "JMSPROPERTIES, " +
      "REFERENCECOUNT " +
      "FROM MESSAGE WHERE MESSAGEID = ?";

   protected String createMessage =
      "CREATE TABLE MESSAGE (" +
      "MESSAGEID VARCHAR(255), " +
      "RELIABLE CHAR(1), " +
      "EXPIRATION BIGINT, " +
      "TIMESTAMP BIGINT, " +
      "PRIORITY TINYINT, " + 
      "DELIVERYCOUNT INTEGER, " +
      "COREHEADERS OBJECT, " +
      "PAYLOAD OBJECT, " +
      "TYPE TINYINT, " +
      "JMSTYPE VARCHAR(255), " +
      "CORRELATIONID OBJECT, " +
      "DESTINATIONISQUEUE CHAR(1), " +
      "DESTINATION VARCHAR(255), " +
      "REPLYTOISQUEUE CHAR(1), " +
      "REPLYTO VARCHAR(255), " +
      "CONNECTIONID INTEGER, " +
      "JMSPROPERTIES OBJECT, " +
      "REFERENCECOUNT TINYINT, " +
      "PRIMARY KEY (MESSAGEID))";

   protected String selectReferenceCount =
      "SELECT REFERENCECOUNT FROM MESSAGE WHERE MESSAGEID = ?";

   protected String updateReferenceCount =
      "UPDATE MESSAGE SET REFERENCECOUNT=? WHERE MESSAGEID=?";
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   protected String dataSourceJNDIName;
   protected DataSource ds;
   protected ObjectName tmObjectName;
   protected TransactionManager tm;

   protected boolean storeXid;
   protected int bytesStoredAs;
   protected Properties sqlProperties;
   protected boolean createTablesOnStartup;
   protected boolean txIdGUID;
   protected boolean performRecovery;
   
   // Constructors --------------------------------------------------

   public JDBCPersistenceManager() throws Exception
   {
      this(null, null);
   }

   /**
    * Only used for testing. In a real deployment, the data source and the transaction manager are
    * injected as dependencies.
    */
   public JDBCPersistenceManager(DataSource ds, TransactionManager tm) throws Exception
   {
      this.ds = ds;
      this.tm = tm;
      bytesStoredAs = BYTES_AS_BYTES;
      sqlProperties = new Properties();
   }

   // ServiceMBeanSupport overrides ---------------------------------

   protected void startService() throws Exception
   {
      if (ds == null)
      {
         InitialContext ic = new InitialContext();
         ds = (DataSource)ic.lookup(dataSourceJNDIName);
         ic.close();
      }

      if (ds == null)
      {
         throw new Exception("No DataSource found. This service dependencies must " +
                             "have not been enforced correctly!");
      }
      if (tm == null)
      {
         throw new Exception("No TransactionManager found. This service dependencies must " +
                             "have not been enforced correctly!");
      }

      initSqlProperties();

      if (createTablesOnStartup)
      {
         createSchema();
      }

      if (performRecovery)
      {
         resolveAllUncommitedTXs();
      }

      log.debug(this + " started");
   }

   protected void stopService() throws Exception
   {
      log.debug(this + " stopped");
   }

   // PesistenceManager implementation -------------------------

   public Object getInstance()
   {
      return this;
   }

   /**
    * We add the message reference in a JDBC transaction (which may in turn be inside a JMS
    * transaction).
    */
   public void addReference(Serializable channelID, MessageReference ref, Transaction tx) throws Exception
   {
      TransactionWrapper wrap = new TransactionWrapper();
      Connection conn = ds.getConnection();
      try
      {
         //Add the reference
         addReference(channelID, ref, tx, conn);
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
    
   /*
    * A message is being acknowledged.
    * We remove the message reference in a JDBC transaction (which may in turn be inside a JMS transaction)
    */
   public void removeReference(Serializable channelID, MessageReference ref, Transaction tx) throws Exception
   {
      TransactionWrapper wrap = new TransactionWrapper();
      Connection conn = ds.getConnection();
      try
      {
         //Remove the message reference
         removeReference(channelID, ref, tx, conn);
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

   /*
    * We have prepared the transaction so we write a flag into the transaction record saying this is so
    */
   public void prepareTx(Transaction tx) throws Exception
   {
      if (!storeXid || tx.getXid() == null)
      {
         return;
      }
      
      if (trace) { log.trace("writing prepared flag for " + tx); }
      
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();

         ps = conn.prepareStatement(prepareTransaction);
         
         if (tx.getGuidTxId() != null)
         {
            ps.setString(1, tx.getGuidTxId());
         }
         else
         {
            ps.setLong(1, tx.getLongTxId());
         }
                  
         int rows = ps.executeUpdate();

         if (trace) { log.trace(JDBCUtil.statementToString(prepareTransaction, getTxId(tx)) + " updated " + rows + " row(s)"); }
                    
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
   
   /*
    * The jms transaction is committing.
    * We needs to remove any message refs marked as "-" and update any message refs marked as "+" to "C"
    */
   public void commitTx(Transaction tx) throws Exception
   {
      if (trace) { log.trace("updating database for committing transaction " + tx); }
      
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();

         ps = conn.prepareStatement(commitMessageRef1);
         if (tx.getGuidTxId() != null)
         {
            ps.setString(1, tx.getGuidTxId());
         }
         else
         {
            ps.setLong(1, tx.getLongTxId());
         }
         
         int rows = ps.executeUpdate();

         if (trace) { log.trace(JDBCUtil.statementToString(commitMessageRef1, getTxId(tx)) + " removed " + rows + " row(s)"); }
                  
         ps.close();
         ps = conn.prepareStatement(commitMessageRef2);

         if (tx.getGuidTxId() != null)
         {
            ps.setString(1, tx.getGuidTxId());
         }
         else
         {
            ps.setLong(1, tx.getLongTxId());
         }
         
         rows = ps.executeUpdate();

         if (trace) { log.trace(JDBCUtil.statementToString(commitMessageRef2, null, getTxId(tx)) + " updated " + rows + " row(s)"); }

         removeTXRecord(conn, tx);
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
   
   /*
    * The jms transaction is rolling back.
    * We needs to remove any message refs marked as '+'
    * and update any message refs marked as "-" to "C"
    */
   public void rollbackTx(Transaction tx) throws Exception
   {
      if (trace) { log.trace("rolling back " + tx); }
      
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();

         ps = conn.prepareStatement(rollbackMessageRef1);
         
         if (tx.getGuidTxId() != null)
         {
            ps.setString(1, tx.getGuidTxId());
         }
         else
         {
            ps.setLong(1, tx.getLongTxId());
         }
         
         int rows = ps.executeUpdate();
         
         if (trace) { log.trace(JDBCUtil.statementToString(rollbackMessageRef1, getTxId(tx)) + " removed " + rows + " row(s)"); }
         
         ps.close();
         
         ps = conn.prepareStatement(rollbackMessageRef2);
         if (tx.getGuidTxId() != null)
         {
            ps.setString(1, tx.getGuidTxId());
         }
         else
         {
            ps.setLong(1, tx.getLongTxId());
         }
         
         rows = ps.executeUpdate();
         
         if (trace) { log.trace(JDBCUtil.statementToString(rollbackMessageRef2, null, getTxId(tx)) + " updated " + rows + " row(s)"); }
                  
         removeTXRecord(conn, tx);
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
   

   public void removeAllMessageData(Serializable channelID) throws Exception
   {
      if (trace) { log.trace("removing all message data for channel " + channelID); }

      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();
      boolean success = false;

      try
      {
         conn = ds.getConnection();         
   
         ps = conn.prepareStatement(deleteChannelMessageRefs);
         
         ps.setString(1, (String)channelID);
         
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
         if (log.isTraceEnabled())
         {
            String s = JDBCUtil.statementToString(deleteChannelMessageRefs, channelID);
            log.trace(s + (success ? " successful" : "failed"));
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

   public List messageRefs(Serializable storeID, Serializable channelID) throws Exception
   {
      if (trace) { log.trace("getting message references for channel " + channelID + " and store " + storeID); }
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         List result = new ArrayList();
         conn = ds.getConnection();         
   
         ps = conn.prepareStatement(selectChannelMessageRefs);
         ps.setString(1, (String)channelID);
         ps.setString(2, (String)storeID);
         
         rs = ps.executeQuery();

         while (rs.next())
         {
            String id = rs.getString(1);            
            result.add(id);
         }
         
         if (trace) { log.trace(JDBCUtil.statementToString(selectChannelMessageRefs, channelID) + " selected " + result.size() + " row(s)"); }

         return result;
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

   public List retrievePreparedTransactions() throws Exception
   {
      if (!storeXid)
      {
         return Collections.EMPTY_LIST;
      }
      
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
            byte[] branchQual = this.getBytes(rs, 2, 0);
            int formatId = rs.getInt(3);
            byte[] globalTxId = this.getBytes(rs, 4, 0);
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
            {}
         }
         if (st != null)
         {
            try
            {
               st.close();
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
   
   
   public boolean isTxIdGUID() throws Exception
   {
      return txIdGUID;
   }
   
   public void setTxIdGuid(boolean isGUID) throws Exception
   {
      txIdGUID = isGUID;
   }
   
   public boolean isStoringXid() throws Exception
   {
      return storeXid;
   }
   
   public void setStoringXid(boolean storingXid) throws Exception
   {
      storeXid = storingXid;
   }
   
   /**
    * Stores the message in the MESSAGE table.
    */
   public void storeMessage(Message m) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();
      String id = (String)m.getMessageID();

      try
      {
         conn = ds.getConnection();

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
            // physically insert the row in the database

            //First set the fields from org.jboss.messaging.core.Routable
            ps = conn.prepareStatement(insertMessage);
            ps.setString(1, id);
            ps.setString(2, m.isReliable() ? "Y" : "N");
            ps.setLong(3, m.getExpiration());
            ps.setLong(4, m.getTimestamp());
            ps.setByte(5, m.getPriority());
            ps.setInt(6, m.getDeliveryCount());

            ps.setObject(7, ((MessageSupport)m).getHeaders());

            //Now set the fields from org.jboss.messaging.core.Message
            ps.setObject(8, m.getPayload());

            //Now set the fields from org.joss.jms.message.JBossMessage if appropriate
            int type = -1;
            String jmsType = null;
            Object correlationID = null;
            boolean destIsQueue = false;
            String dest = null;
            boolean replyToIsQueue = false;
            String replyTo = null;
            Map jmsProperties = null;
            int connectionID = -1;

            if (m instanceof JBossMessage)
            {
               JBossMessage jbm = (JBossMessage)m;
               type = jbm.getType();
               jmsType = jbm.getJMSType();
               connectionID = jbm.getConnectionID();

               if (jbm.isCorrelationIDBytes())
               {
                  correlationID = jbm.getJMSCorrelationIDAsBytes();
               }
               else
               {
                  correlationID = jbm.getJMSCorrelationID();
               }

               Destination d = jbm.getJMSDestination();
               if (d != null)
               {
                  destIsQueue = d instanceof Queue;
                  if (destIsQueue)
                  {
                     dest = ((Queue)d).getQueueName();
                  }
                  else
                  {
                     dest = ((Topic)d).getTopicName();
                  }
               }

               Destination r = jbm.getJMSReplyTo();
               if (r != null)
               {
                  replyToIsQueue = r instanceof Queue;
                  if (replyToIsQueue)
                  {
                     replyTo = ((Queue)r).getQueueName();
                  }
                  else
                  {
                     replyTo = ((Topic)r).getTopicName();
                  }
               }
               jmsProperties = jbm.getJMSProperties();
            }

            ps.setInt(9, type);
            ps.setString(10, jmsType);
            ps.setObject(11, correlationID);
            ps.setString(12, destIsQueue ? "Y" : "N");
            ps.setString(13, dest);
            ps.setString(14, replyToIsQueue ? "Y" : "N");
            ps.setString(15, replyTo);
            ps.setInt(16, connectionID);
            ps.setObject(17, jmsProperties);
            ps.setInt(18, 1);

            int result = ps.executeUpdate();
            if (trace) { log.trace(JDBCUtil.statementToString(insertMessage, id) + " inserted " + result + " row(s)"); }
         }
         else
         {
            // increment the reference count
            ps = conn.prepareStatement(updateReferenceCount);
            ps.setInt(1, ++referenceCount);
            ps.setString(2, id);

            ps.executeUpdate();
            if (trace) { log.trace(JDBCUtil.statementToString(updateReferenceCount, new Integer(referenceCount), id) + " executed successfully"); }
         }
      }
      catch (Exception e)
      {
         if (trace) { log.trace(JDBCUtil.statementToString(insertMessage, id) + " failed"); }
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

   /**
    * Removes the message from the MESSAGE table.
    */
   public boolean removeMessage(String messageID) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();

         // get the reference count from the database
         ps = conn.prepareStatement(selectReferenceCount);
         ps.setString(1, messageID);

         //TODO this can be combined into one query
         int referenceCount = 0;
         ResultSet rs = ps.executeQuery();
         if (rs.next())
         {
            referenceCount = rs.getInt(1);
         }

         if (trace) { log.trace(JDBCUtil.statementToString(selectReferenceCount, messageID) + " returned " + (referenceCount == 0 ? "no rows" : Integer.toString(referenceCount))); }

         if (referenceCount == 0)
         {
            if (trace) { log.trace("no message " + messageID + " to delete in the database"); }
            return false;
         }
         else if (referenceCount == 1)
         {
            // physically delete the row in the database
            ps = conn.prepareStatement(deleteMessage);
            ps.setString(1, messageID);

            int rows = ps.executeUpdate();
            if (trace) { log.trace(JDBCUtil.statementToString(deleteMessage, messageID) + " deleted " + rows + " row(s)"); }

            return rows == 1;
         }
         else
         {
            // decrement the reference count
            ps = conn.prepareStatement(updateReferenceCount);
            ps.setInt(1, --referenceCount);
            ps.setString(2, messageID);

            ps.executeUpdate();
            if (trace) { log.trace(JDBCUtil.statementToString(updateReferenceCount, new Integer(referenceCount), messageID) + " executed successfully"); }
            return true;
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

   /**
    * Retrieves the message from the MESSAGE table and <i>increments the reference count</i> if
    * required.
    */
   public Message getMessage(Serializable messageID) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         Message m = null;
         int referenceCount = 0;
         conn = ds.getConnection();

         ps = conn.prepareStatement(selectMessage);
         ps.setString(1, (String)messageID);

         rs = ps.executeQuery();

         int count = 0;
         if (rs.next())
         {           
            m = MessageFactory.createMessage(rs.getString(1), //message id
                                      rs.getString(2).equals("Y"), //reliable
                                      rs.getLong(3), //expiration
                                      rs.getLong(4), //timestamp
                                      rs.getByte(5), //priority
                                      rs.getInt(6), //delivery count
                                      (Map)rs.getObject(7), //core headers
                                      (Serializable)rs.getObject(8), //payload
                                      rs.getByte(9), //type
                                      rs.getString(10), //jms type
                                      rs.getObject(11), //correlation id
                                      rs.getString(12).equals("Y"), //destination is queue
                                      rs.getString(13), //destination
                                      rs.getString(14).equals("Y"), //reply to is queue
                                      rs.getString(15), //reply to
                                      rs.getInt(16), // connection id
                                      (Map)rs.getObject(17)); // jms properties

            referenceCount = rs.getInt(18);
            count ++;
         }

         if (trace) { log.trace(JDBCUtil.statementToString(selectMessage, messageID) + " selected " + count + " row(s)"); }

         if (m != null)
         {
            // increment the reference count
            ps = conn.prepareStatement(updateReferenceCount);
            ps.setInt(1, ++referenceCount);
            ps.setString(2, (String)m.getMessageID());

            ps.executeUpdate();
            if (trace) { log.trace(JDBCUtil.statementToString(updateReferenceCount, new Integer(referenceCount), m.getMessageID()) + " executed successfully"); }
         }

         return m;
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

      TransactionManagerServiceMBean tms =
         (TransactionManagerServiceMBean)MBeanServerInvocationHandler.
         newProxyInstance(getServer(), tmObjectName, TransactionManagerServiceMBean.class, false);

      tm = tms.getTransactionManager();
   }

   /**
    * Managed attribute.
    */
   public ObjectName getTransactionManager()
   {
      return tmObjectName;
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

      try
      {
         conn = ds.getConnection();
         
         try
         {
            conn.createStatement().executeUpdate(createTransaction);
            if (trace) { log.trace(createTransaction + " succeeded"); }
         }
         catch (SQLException e)
         {
            log.debug(createTransaction + " failed!", e);
         }
                   
         try
         {
            conn.createStatement().executeUpdate(createMessageReference);
            if (trace) { log.trace(createMessageReference + " succeeded"); }
         }
         catch (SQLException e)
         {
            log.debug(createMessageReference + " failed!", e);
         }

         try
         {
            conn.createStatement().executeUpdate(createIdxMessageRefTx);
            if (trace) { log.trace(createIdxMessageRefTx + " succeeded"); }
         }
         catch (SQLException e)
         {
            log.debug(createIdxMessageRefTx + " failed!", e);
         }
         
         try
         {
            conn.createStatement().executeUpdate(createMessage);
            if (trace) { log.trace(createMessage + " succeeded"); }
         }
         catch (SQLException e)
         {
            log.debug(createMessage + " failed!", e);
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
            {}
         }
         tx.end();
      }
   }
   
   protected void initSqlProperties()
   {
      insertMessageRef = sqlProperties.getProperty("INSERT_MESSAGE_REF", insertMessageRef);
      deleteMessageRef = sqlProperties.getProperty("DELETE_MESSAGE_REF", deleteMessageRef);     
      updateMessageRef = sqlProperties.getProperty("UPDATE_MESSAGE_REF", updateMessageRef);
      deleteChannelMessageRefs = sqlProperties.getProperty("DELETE_CHANNEL_MESSAGE_REFS", deleteChannelMessageRefs);      
      selectChannelMessageRefs = sqlProperties.getProperty("SELECT_CHANNEL_MESSAGE_REFS", selectChannelMessageRefs);
      createTransaction = sqlProperties.getProperty("CREATE_TRANSACTION", createTransaction);
      createMessageReference = sqlProperties.getProperty("CREATE_MESSAGE_REF", createMessageReference);      
      insertTransactionXA = sqlProperties.getProperty("INSERT_TRANSACTION_XA", insertTransactionXA);
      insertTransaction = sqlProperties.getProperty("INSERT_TRANSACTION", insertTransaction);
      deleteTransaction = sqlProperties.getProperty("DELETE_TRANSACTION", deleteTransaction); 
      selectPreparedTransactions = sqlProperties.getProperty("SELECT_XA_TRANSACTIONS", selectPreparedTransactions);
      createIdxMessageRefTx = sqlProperties.getProperty("CREATE_IDX_MESSAGE_REF_TX", createIdxMessageRefTx);      
      selectTransactionId = sqlProperties.getProperty("SELECT_TX_ID", selectTransactionId);   
      prepareTransaction = sqlProperties.getProperty("PREPARE_TX", prepareTransaction);    
      commitMessageRef1 = sqlProperties.getProperty("COMMIT_MESSAGE_REF1", commitMessageRef1);
      commitMessageRef2 = sqlProperties.getProperty("COMMIT_MESSAGE_REF2", commitMessageRef2);
      rollbackMessageRef1 = sqlProperties.getProperty("ROLLBACK_MESSAGE_REF1", rollbackMessageRef1);
      rollbackMessageRef2 = sqlProperties.getProperty("ROLLBACK_MESSAGE_REF2", rollbackMessageRef2);      
      rollbackNonPreparedTx1 = sqlProperties.getProperty("ROLLBACK_TX1", rollbackNonPreparedTx1);
      rollbackNonPreparedTx2 = sqlProperties.getProperty("ROLLBACK_TX2", rollbackNonPreparedTx2);
      deleteNonPreparedTx = sqlProperties.getProperty("DELETE_TX", deleteNonPreparedTx);
      deleteMessage = sqlProperties.getProperty("DELETE_MESSAGE", deleteMessage);
      insertMessage = sqlProperties.getProperty("INSERT_MESSAGE", insertMessage);
      selectMessage = sqlProperties.getProperty("SELECT_MESSAGE", selectMessage);            
      createMessage = sqlProperties.getProperty("CREATE_MESSAGE", createMessage);
      selectReferenceCount = sqlProperties.getProperty("SELECT_REF_COUNT", selectReferenceCount);
      updateReferenceCount = sqlProperties.getProperty("UPDATE_REF_COUNT", updateReferenceCount); 
            
      createTablesOnStartup = sqlProperties.getProperty("CREATE_TABLES_ON_STARTUP", "true").equalsIgnoreCase("true");      
      storeXid = sqlProperties.getProperty("STORE_XID", "true").equalsIgnoreCase("true");

      //txIdGUID has its own variable and mutator so storing it here is confusing and error prone.
      //txIdGUID = sqlProperties.getProperty("TX_ID_IS_GUID", "false").equalsIgnoreCase("true");
      performRecovery = sqlProperties.getProperty("PERFORM_NONXA_RECOVERY", "false").equalsIgnoreCase("true");
   }
   
   protected void insertTx(Connection conn, Transaction tx) throws Exception
   {
      if (tx != null && !tx.insertedTXRecord())
      {
         addTXRecord(conn, tx);
      }
   }
   
   protected void addTXRecord(Connection conn, Transaction tx) throws Exception
   {
      if (trace) { log.trace("logging " + tx);}

      PreparedStatement ps = null;
      String statement = "UNDEFINED";
      ResultSet rs = null;
      boolean xa = false;
      int rows = -1;
      int formatID = -1;
      String txID = null;
      try
      {
         xa = storeXid && tx.getXid() != null;
         statement = xa ? insertTransactionXA : insertTransaction;

         ps = conn.prepareStatement(statement);
         
         if (txIdGUID)
         {
            txID = new GUID().toString();
            tx.setGuidTxID(txID);
            ps.setString(1, txID);
         }
         else
         {
            // the TRANSACTIONID field is created as IDENTITY so inserting null here is acceptable
            ps.setNull(1, Types.BIGINT);
         }
         
         if (xa)
         {
            Xid xid = tx.getXid();
            formatID = xid.getFormatId();
            this.setBytes(ps, 2, xid.getBranchQualifier());
            ps.setInt(3, formatID);
            this.setBytes(ps, 4, xid.getGlobalTransactionId());
         }

         rows = ps.executeUpdate();
         
         if (!txIdGUID)
         {
            long ltxID = Long.MIN_VALUE;
            try
            {
               ps.close();
               ps = conn.prepareStatement(selectTransactionId);
               rs = ps.executeQuery();
               rs.next();
               ltxID = rs.getLong(1);
               tx.setLongTxId(ltxID);
            }
            finally
            {
               if (trace) { log.trace(selectTransactionId + (ltxID == Long.MIN_VALUE ? " failed!" : " returned " + txID)); }
            }
         }
      }
      finally
      {
         if (trace)
         {
            String s = insertTransaction.equals(statement) ?
               JDBCUtil.statementToString(insertTransaction, txID) :
               JDBCUtil.statementToString(insertTransactionXA, getTxId(tx), "<byte-array>", new Integer(formatID), "<byte-array>");
            log.trace(s + (rows == -1 ? " failed!" : " inserted " + rows + " row(s)"));
         }

         try
         {
            if (rs != null)
            {
               rs.close();
            }
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
         if (tx.getGuidTxId() != null)
         {
            ps.setString(1, tx.getGuidTxId());
         }
         else
         {
            ps.setLong(1, tx.getLongTxId());
         }
         
         int rows = ps.executeUpdate();

         
         if (trace)
         {
            log.trace(JDBCUtil.statementToString(deleteTransaction, getTxId(tx)) + " removed " + rows + " row(s)"); 
           
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
   
   protected void setBytes(PreparedStatement stmt, int column, byte[] bytes)
      throws IOException, SQLException
   {
      if (bytesStoredAs == BYTES_AS_OBJECT)
      {
         stmt.setObject(column, bytes);
      }
      else if (bytesStoredAs == BYTES_AS_BLOB)
      {
         throw new NotYetImplementedException("BLOB_TYPE: BLOB_BLOB is not yet implemented.");
      }
      else if (bytesStoredAs == BYTES_AS_BYTES)
      {
         stmt.setBytes(column, bytes);
      }
      else if (bytesStoredAs == BYTES_AS_BINARY_STREAM)
      {
         ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         stmt.setBinaryStream(column, bais, bytes.length);
      }
   }
   
   protected byte[] getBytes(ResultSet res, int column, int initSize)
      throws IOException, SQLException
   {
      byte[] bytes = null;
      if (bytesStoredAs == BYTES_AS_OBJECT)
      {
         bytes = (byte[])res.getBytes(column);
      }
      else if (bytesStoredAs == BYTES_AS_BLOB)
      {
         throw new NotYetImplementedException("BLOB_TYPE: BLOB_BLOB is not yet implemented.");
      }
      else if (bytesStoredAs == BYTES_AS_BYTES)
      {
         bytes = res.getBytes(column);
      }
      else if (bytesStoredAs == BYTES_AS_BINARY_STREAM)
      {
         InputStream is = res.getBinaryStream(column);
         ByteArrayOutputStream bos = new ByteArrayOutputStream(initSize);
         int b;
         while ((b = is.read()) != -1)
         {
            bos.write(b);
         }
         bos.flush();
         bytes = bos.toByteArray();
      }
      return bytes;
   }
   
   protected Object getTxId(Transaction tx)
   {
      if (tx == null)
      {
         return null;
      }
      if (tx.getGuidTxId() != null)
      {
         return tx.getGuidTxId();
      }
      else
      {
         return new Long(tx.getLongTxId());
      }
   }
   

   protected void addReference(Serializable channelID,
                               MessageReference ref,
                               Transaction tx,
                               Connection conn)
      throws Exception
   {
      if (trace) { log.trace("adding " + ref + " to channel "  + channelID + (tx == null ? " non-transactionally" : " on transaction: " + tx));}
            
      PreparedStatement ps = null;

      int inserted = -1;
      boolean attempted = false;
      String messageID = (String)ref.getMessageID();
      String storeID = (String)ref.getStoreID();
      long ordering = ref.getOrdering();
      String state = null;

      try
      {      
         insertTx(conn, tx);

         if (tx == null)
         {
            state = "C";
         }
         else
         {
            state = "+";
         }
         
         ps = conn.prepareStatement(insertMessageRef);
         ps.setString(1, (String)channelID);
         ps.setString(2, messageID);
         ps.setString(3, storeID);
         if (tx == null)
         {
            if (this.txIdGUID)
            {
               ps.setNull(4, java.sql.Types.VARCHAR);
            }
            else
            {
               ps.setNull(4, java.sql.Types.BIGINT);
            }
         }
         else
         {
            if (this.txIdGUID)
            {
               ps.setString(4, tx.getGuidTxId());
            }
            else
            {
               ps.setLong(4, tx.getLongTxId());
            }
         }
         ps.setString(5, state);
         ps.setLong(6, ordering);

         attempted = true;
         inserted = ps.executeUpdate();
      }
      finally
      {
         if (trace && attempted)
         {
            String s = JDBCUtil.statementToString(insertMessageRef, channelID, messageID,
                                                  storeID, getTxId(tx), state, new Long(ordering));
            log.trace(s + (inserted == -1 ? " failed" : " inserted " + inserted + " row(s)"));
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
      }
   }
   
   protected void removeReference(Serializable channelID, MessageReference ref, Transaction tx, Connection conn)
      throws Exception
   {
      if (trace) { log.trace("removing " + ref + " from channel "  + channelID + (tx == null ? " non-transactionally" : " on transaction: " + tx));}
            
      PreparedStatement ps = null;
      try
      {
         insertTx(conn, tx);
   
         int updated;
   
         String messageID = (String)ref.getMessageID();
   
         if (tx == null)
         {
            //Non transacted case
            ps = conn.prepareStatement(deleteMessageRef);
            ps.setString(1, (String)channelID);
            ps.setString(2, messageID);
            
            updated = ps.executeUpdate();
   
            if (trace) { log.trace(JDBCUtil.statementToString(deleteMessageRef, channelID, messageID) + " removed/updated " + updated + " row(s)"); }
         }
         else
         {
            //Transacted case
            ps = conn.prepareStatement(updateMessageRef);
            if (tx.getGuidTxId() != null)
            {
               ps.setString(1, tx.getGuidTxId());
            }
            else
            {
               ps.setLong(1, tx.getLongTxId());
            }
            ps.setString(2, (String)channelID);
            ps.setString(3, messageID);            
   
            updated = ps.executeUpdate();
   
            if (trace) { log.trace(JDBCUtil.statementToString(updateMessageRef, getTxId(tx), channelID, messageID) + " updated " + updated + " row(s)"); }
         }   
      }
      finally
      {
         if (ps != null)
         {
            try
            {
               ps.close();
            }
            catch (Throwable t)
            {}
         }
      }
   }
   
   protected void resolveAllUncommitedTXs() throws Exception
   {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      TransactionWrapper tx = new TransactionWrapper();

      log.info(this + " resolving any uncommitted transactions");
      
      try
      {
         conn = ds.getConnection();
         
         // Delete all the messages that were added but their tx's were not commited.
         stmt = conn.prepareStatement(rollbackNonPreparedTx1);
         int rows = stmt.executeUpdate();
         stmt.close();
         stmt = null;
         if (rows != 0)
         {
            log.info("Rolled back " + rows + " message references added in uncommitted transactions");
         }

         // Restore all the messages that were removed but their tx's were not commited.
         stmt = conn.prepareStatement(rollbackNonPreparedTx2);
         rows = stmt.executeUpdate();
         stmt.close();
         stmt = null;
         if (rows != 0)
         {
            log.info("Rolled back " + rows + " message references due to be removed in uncommitted transactions");
         }

         // Clear the transaction table (leave any prepared transactions)
         stmt = conn.prepareStatement(deleteNonPreparedTx);
         rows = stmt.executeUpdate();
         stmt.close();
         stmt = null;
         if (rows != 0)
         {
            log.info("Deleted " + rows + " non-prepared transactions");
         }
         
      }
      catch (SQLException e)
      {
         tx.exceptionOccurred();
         throw new JBossJMSException(
            "Could not resolve uncommited transactions.  Message recovery may not be accurate",
            e);
      }
      finally
      {
         try
         {
            if (rs != null)
            {
               rs.close();
            }
         }
         catch (Throwable ignore)
         {
         }
         try
         {
            if (stmt != null)
            {
               stmt.close();
            }
         }
         catch (Throwable ignore)
         {
         }
         try
         {
            if (conn != null)
            {
               conn.close();           
            }
         }
         catch (Throwable ignore)
         {
         }
         tx.end();
      }

   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
   class TransactionWrapper
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

}
