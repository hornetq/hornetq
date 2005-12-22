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
package org.jboss.messaging.core.persistence;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.TransactionManager;
import javax.transaction.xa.Xid;

import org.jboss.jms.message.JBossMessage;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.StatefulReceiverDelivery;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.message.MessageSupport;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.XidImpl;
import org.jboss.util.id.GUID;

/**
 *  
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Partially derived from org.jboss.mq.pm.jdbc2.PersistenceManager which was written by:
 * 
 * @author Jayesh Parayali (jayeshpk1@yahoo.com)
 * @author Hiram Chirino (cojonudo14@hotmail.com)
 * @author Adrian Brock (adrian@jboss.org)
 * 
 * @version <tt>$Revision$</tt>
 *
 */
public class JDBCPersistenceManager implements PersistenceManager
{
   // Constants -----------------------------------------------------

   /* The default DML and DDL will work with HSQLDB */
   
   private static final Logger log = Logger.getLogger(JDBCPersistenceManager.class);
   
   protected static final int BYTES_AS_OBJECT = 0;

   protected static final int BYTES_AS_BYTES = 1;

   protected static final int BYTES_AS_BINARY_STREAM = 2;

   protected static final int BYTES_AS_BLOB = 3; 
   
   protected String insertDelivery =
      "INSERT INTO DELIVERY (CHANNELID, MESSAGEID, RECEIVERID, STOREID, TRANSACTIONID, STATE) " +
      "VALUES (?, ?, ?, ?, ?, ?)";
   
   protected String deleteDelivery =
      "DELETE FROM DELIVERY WHERE CHANNELID=? AND MESSAGEID=? AND RECEIVERID=? AND STATE='C'";
   
   protected String updateDelivery =
      "UPDATE DELIVERY SET TRANSACTIONID=?, STATE='-' " +
      "WHERE CHANNELID=? AND MESSAGEID=? AND RECEIVERID=? AND STATE='C'";
   
   protected String insertMessageRef =
      "INSERT INTO MESSAGE_REFERENCE (CHANNELID, MESSAGEID, STOREID, TRANSACTIONID, STATE, ORD) " +
      "VALUES (?, ?, ?, ?, ?, ?)";
   
   protected String deleteMessageRef =
      "DELETE FROM MESSAGE_REFERENCE WHERE CHANNELID=? AND MESSAGEID=? AND STATE='C'";
   
   protected String commitDelivery =
      "DELETE FROM DELIVERY WHERE STATE='-' AND TRANSACTIONID=?";
   
   protected String commitMessageRef =
      "UPDATE MESSAGE_REFERENCE SET STATE='C', TRANSACTIONID=? WHERE STATE='+' AND TRANSACTIONID=?";
   
   protected String rollbackDelivery =
      "UPDATE DELIVERY SET STATE='C', TRANSACTIONID=? WHERE STATE='-' AND TRANSACTIONID=?";
   
   protected String rollbackMessageRef =
      "DELETE FROM MESSAGE_REFERENCE WHERE TRANSACTIONID=? AND STATE='+'";
   
   protected String selectChannelDeliveries =
      "SELECT MESSAGEID FROM DELIVERY WHERE CHANNELID=? AND STOREID=?";
   
   protected String deleteChannelDeliveries =
      "DELETE FROM DELIVERY WHERE CHANNELID=?";
   
   protected String deleteChannelMessageRefs =
      "DELETE FROM MESSAGE_REFERENCE WHERE CHANNELID=?";
   
   protected String selectChannelMessageRefs =
      "SELECT MESSAGEID FROM MESSAGE_REFERENCE WHERE CHANNELID=? AND STOREID=? AND STATE='C' ORDER BY ORD";
   
   protected String insertMessage =
      "INSERT INTO MESSAGE (MESSAGEID, RELIABLE, EXPIRATION, TIMESTAMP, PRIORITY, DELIVERYCOUNT, " +
      "COREHEADERS, PAYLOAD, " +
      "TYPE, JMSTYPE, CORRELATIONID, DESTINATIONISQUEUE, " +
      "DESTINATION, REPLYTOISQUEUE, REPLYTO, CONNECTIONID, JMSPROPERTIES, REFERENCECOUNT) " + 
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

   protected String selectMessageReferenceCount =
      "SELECT REFERENCECOUNT FROM MESSAGE WHERE MESSAGEID = ?";

   protected String updateReferenceCount =
      "UPDATE MESSAGE SET REFERENCECOUNT=? WHERE MESSAGEID=?";

   protected String createTransaction = 
      "CREATE TABLE TRANSACTION " +
      "(TRANSACTIONID BIGINT GENERATED BY DEFAULT AS IDENTITY, BRANCH_QUAL OBJECT, " +
      "FORMAT_ID INTEGER, GLOBAL_TXID OBJECT, STATE CHAR(1), PRIMARY KEY (TRANSACTIONID) )";
   
   protected String createDelivery =
      "CREATE TABLE DELIVERY " +
      "(CHANNELID VARCHAR(256), MESSAGEID VARCHAR(256), RECEIVERID VARCHAR(256)," +
      "STOREID VARCHAR(256), TRANSACTIONID BIGINT, STATE CHAR(1), " +
      "PRIMARY KEY(CHANNELID, MESSAGEID, RECEIVERID))";
   
   protected String createMessageReference =
      "CREATE TABLE MESSAGE_REFERENCE (CHANNELID VARCHAR(256), MESSAGEID VARCHAR(256), " +
      "STOREID VARCHAR(256), TRANSACTIONID BIGINT, STATE CHAR(1), ORD BIGINT, PRIMARY KEY(STOREID, CHANNELID, MESSAGEID))";
   
   protected String createMessage =
      "CREATE TABLE MESSAGE (" +
      "MESSAGEID VARCHAR(256), " +
      "RELIABLE CHAR(1), " +
      "EXPIRATION BIGINT, " +
      "TIMESTAMP BIGINT, " +
      "PRIORITY TINYINT, " + 
      "DELIVERYCOUNT INTEGER, " +
      "COREHEADERS OBJECT, " +
      "PAYLOAD OBJECT, " +
      "TYPE INTEGER, " +
      "JMSTYPE VARCHAR(255), " +
      "CORRELATIONID OBJECT, " +
      "DESTINATIONISQUEUE CHAR(1), " +
      "DESTINATION VARCHAR(255), " +
      "REPLYTOISQUEUE CHAR(1), " +
      "REPLYTO VARCHAR(255), " +
      "CONNECTIONID VARCHAR(255), " +
      "JMSPROPERTIES OBJECT, " +
      "REFERENCECOUNT TINYINT, " +
      "PRIMARY KEY (MESSAGEID))";

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
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected DataSource ds;

   protected TransactionManager mgr;
   
   protected boolean storeXid;
   
   protected int bytesStoredAs;
   
   protected Properties sqlProperties;
   
   protected boolean createTablesOnStartup;
   
   protected boolean txIdGUID;


   // Constructors --------------------------------------------------

   public JDBCPersistenceManager() throws Exception
   {
      bytesStoredAs = BYTES_AS_BYTES;
      
      sqlProperties = new Properties();
                  
   }

   // PersistenceManager implementation -----------------------------
   
   /*
    * A new message has arrived but we can't handle it.
    * We add the message reference in a JDBC transaction (which may in turn be inside a JMS transaction)
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
    * A message is being delivered - we add a delivery in a JDBC transaction
    */
   public void deliver(Serializable channelID, Delivery d) throws Exception
   {
      TransactionWrapper wrap = new TransactionWrapper();
      Connection conn = ds.getConnection();
      try
      {
         addDelivery(channelID, d, conn);
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
    * A message is being redelivered.
    * We remove the message reference and add deliveries (in JMS there will be only one delivery)
    * in the same JDBC transaction
    */
   public void redeliver(Serializable channelID, Set deliveries) throws Exception
   {   
      TransactionWrapper wrap = new TransactionWrapper();
      Connection conn = ds.getConnection();
      try
      {
         //Remove the reference
         removeReference(channelID, ((Delivery)deliveries.iterator().next()).getReference(), conn);
         
         //Add the deliveries
         Iterator iter = deliveries.iterator();
         while (iter.hasNext())
         {
            Delivery del = (Delivery)iter.next();
            addDelivery(channelID, del, conn);
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
    * A message is being cancelled.
    * We remove the delivery and add a message reference in the same JDBC transaction
    */
   public void cancel(Serializable channelID, Delivery d) throws Exception
   {
      TransactionWrapper wrap = new TransactionWrapper();
      Connection conn = ds.getConnection();
      try
      {
         //Remove the delivery
         removeDelivery(channelID, d, null, conn);
         
         //Add the message reference
         addReference(channelID, d.getReference(), null, conn);
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
    * We remove the delivery in a JDBC transaction (which may in turn be inside a JMS transaction)
    */
   public void acknowledge(Serializable channelID, Delivery d, Transaction tx) throws Exception
   {
      TransactionWrapper wrap = new TransactionWrapper();
      Connection conn = ds.getConnection();
      try
      {
         //Remove the delivery
         removeDelivery(channelID, d, tx, conn);
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
      
      if (log.isTraceEnabled()) { log.trace("Writing prepared flag for: " + tx); }
      
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

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(prepareTransaction, getTxId(tx)) + " updated " + rows + " row(s)"); }
                    
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
    * We needs to remove any deliveries marked as "-".
    */
   public void commitTx(Transaction tx) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("updating database for committing transaction " + tx); }
      
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();

         ps = conn.prepareStatement(commitDelivery);
         if (tx.getGuidTxId() != null)
         {
            ps.setString(1, tx.getGuidTxId());
         }
         else
         {
            ps.setLong(1, tx.getLongTxId());
         }
         
         int rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(commitDelivery, getTxId(tx)) + " removed " + rows + " row(s)"); }
                  
         ps.close();
         ps = conn.prepareStatement(commitMessageRef);
         ps.setNull(1, Types.VARCHAR);
         
         if (tx.getGuidTxId() != null)
         {
            ps.setString(2, tx.getGuidTxId());
         }
         else
         {
            ps.setLong(2, tx.getLongTxId());
         }
         
         rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(commitMessageRef, null, getTxId(tx)) + " updated " + rows + " row(s)"); }

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
    * We needs to remove any message refs marked as 'A'
    * and update any deliveries marked as 'D' to 'A'
    */
   public void rollbackTx(Transaction tx) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Rolling back transaction:" + tx); }
      
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();

         ps = conn.prepareStatement(rollbackMessageRef);
         
         if (tx.getGuidTxId() != null)
         {
            ps.setString(1, tx.getGuidTxId());
         }
         else
         {
            ps.setLong(1, tx.getLongTxId());
         }
         
         int rows = ps.executeUpdate();
         
         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(rollbackMessageRef, getTxId(tx)) + " removed " + rows + " row(s)"); }
         
         ps.close();
         
         ps = conn.prepareStatement(rollbackDelivery);
         ps.setNull(1, Types.VARCHAR);
         
         if (tx.getGuidTxId() != null)
         {
            ps.setString(2, tx.getGuidTxId());
         }
         else
         {
            ps.setLong(2, tx.getLongTxId());
         }
         
         rows = ps.executeUpdate();
         
         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(rollbackDelivery, null, getTxId(tx)) + " updated " + rows + " row(s)"); }
                  
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
   
 
   public List deliveries(Serializable storeID, Serializable channelID) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("listing deliveries for channel: " + channelID + " and store " + storeID); }

      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();
               
         List result = new ArrayList();

         // the deliveries that are currently being acknowledged in a transaction still count until
         // transaction commits
         ps = conn.prepareStatement(selectChannelDeliveries);
         ps.setString(1, (String)channelID);
         ps.setString(2, (String)storeID);
         
         rs = ps.executeQuery();

         int count = 0;
         while (rs.next())
         {
            String id = rs.getString(1);            
            result.add(id);
            count++;
         }

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(selectChannelDeliveries, channelID) + " selected " + count + " row(s)"); }
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

   public void removeAllMessageData(Serializable channelID) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Removing all message data for channel: " + channelID); }

      Connection conn = null;
      PreparedStatement ps1 = null;
      PreparedStatement ps2 = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();         
   
         ps1 = conn.prepareStatement(deleteChannelDeliveries);
         ps2 = conn.prepareStatement(deleteChannelMessageRefs);
         
         ps1.setString(1, (String)channelID);
         ps2.setString(1, (String)channelID);
         
         ps1.executeUpdate();
         ps2.executeUpdate();
      }
      catch (Exception e)
      {
         wrap.exceptionOccurred();
         throw e;
      }
      finally
      {         

         if (ps1 != null)
         {
            try
            {
               ps1.close();
            }
            catch (Throwable e)
            {}
         }
         if (ps2 != null)
         {
            try
            {
               ps2.close();
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
      if (log.isTraceEnabled()) { log.trace("Getting message references for channel " + channelID + " and store " + storeID); }
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
         
         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(selectChannelMessageRefs, channelID) + " selected " + result.size() + " row(s)"); }

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
         ps = conn.prepareStatement(selectMessageReferenceCount);
         ps.setString(1, id);

         int referenceCount = 0;
         ResultSet rs = ps.executeQuery();
         if (rs.next())
         {
            referenceCount = rs.getInt(1);
         }

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(selectMessageReferenceCount, id) + " returned " + (referenceCount == 0 ? "no rows" : Integer.toString(referenceCount))); }

         if (referenceCount == 0)
         {
            // physically insert the row in the database

            //First set the fields from org.jboss.messaging.core.Routable
            ps = conn.prepareStatement(insertMessage);
            ps.setString(1, id);
            ps.setString(2, m.isReliable() ? "Y" : "N");
            ps.setLong(3, m.getExpiration());
            ps.setLong(4, m.getTimestamp());
            ps.setInt(5, m.getPriority());
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
            String connectionID = null;

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
            ps.setString(16, connectionID);
            ps.setObject(17, jmsProperties);
            ps.setInt(18, 1);

            int result = ps.executeUpdate();
            if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(insertMessage, id) + " inserted " + result + " row(s)"); }
         }
         else
         {
            // increment the reference count
            ps = conn.prepareStatement(updateReferenceCount);
            ps.setInt(1, ++referenceCount);
            ps.setString(2, id);

            ps.executeUpdate();
            if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(updateReferenceCount, new Integer(referenceCount), id) + " executed successfully"); }
         }
      }
      catch (Exception e)
      {
         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(insertMessage, id) + " failed"); }
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
         ps = conn.prepareStatement(selectMessageReferenceCount);
         ps.setString(1, messageID);

         //TODO this can be combined into one query
         int referenceCount = 0;
         ResultSet rs = ps.executeQuery();
         if (rs.next())
         {
            referenceCount = rs.getInt(1);
         }

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(selectMessageReferenceCount, messageID) + " returned " + (referenceCount == 0 ? "no rows" : Integer.toString(referenceCount))); }

         if (referenceCount == 0)
         {
            if (log.isTraceEnabled()) { log.trace("no message " + messageID + " to delete in the database"); }
            return false;
         }
         else if (referenceCount == 1)
         {
            // physically delete the row in the database
            ps = conn.prepareStatement(deleteMessage);
            ps.setString(1, messageID);

            int rows = ps.executeUpdate();
            if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(deleteMessage, messageID) + " deleted " + rows + " row(s)"); }

            return rows == 1;
         }
         else
         {
            // decrement the reference count
            ps = conn.prepareStatement(updateReferenceCount);
            ps.setInt(1, --referenceCount);
            ps.setString(2, messageID);

            ps.executeUpdate();
            if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(updateReferenceCount, new Integer(referenceCount), messageID) + " executed successfully"); }
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
   public Message retrieveMessage(Serializable messageID)
         throws Exception
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
                                      rs.getInt(5), //priority
                                      rs.getInt(6), //delivery count
                                      (Map)rs.getObject(7), //core headers
                                      (Serializable)rs.getObject(8), //payload
                                      rs.getInt(9), //type
                                      rs.getString(10), //jms type
                                      rs.getObject(11), //correlation id
                                      rs.getString(12).equals("Y"), //destination is queue
                                      rs.getString(13), //destination
                                      rs.getString(14).equals("Y"), //reply to is queue
                                      rs.getString(15), //reply to
                                      rs.getString(16), // connection id
                                      (Map)rs.getObject(17)); // jms properties
            
            referenceCount = rs.getInt(18);
            count ++;                                    
         }

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(selectMessage, messageID) + " selected " + count + " row(s)"); }

         if (m != null)
         {
            // increment the reference count
            ps = conn.prepareStatement(updateReferenceCount);
            ps.setInt(1, ++referenceCount);
            ps.setString(2, (String)m.getMessageID());

            ps.executeUpdate();
            if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(updateReferenceCount, new Integer(referenceCount), m.getMessageID()) + " executed successfully"); }
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

         ps = conn.prepareStatement(selectMessageReferenceCount);
         ps.setString(1, (String)messageID);

         rs = ps.executeQuery();

         int count = 0;
         if (rs.next())
         {
            count = rs.getInt(1);
         }

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(selectMessageReferenceCount, messageID) + " returned " + (count == 0 ? "no rows" : Integer.toString(count))); }

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
   

   // Public --------------------------------------------------------
   
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

   public void start() throws Exception
   {
      log.debug("starting JDBCPersistenceManager");

      initSqlProperties();
      
      InitialContext ic = new InitialContext();
      
      ds = (DataSource)ic.lookup("java:/DefaultDS");
      
      mgr = (TransactionManager)ic.lookup("java:/TransactionManager");
      
      ic.close();

      if (createTablesOnStartup)
      {
         createSchema();
      }
   }
      

   public void stop() throws Exception
   {
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
            if (log.isTraceEnabled()) { log.trace("Creating TRANSACTION table"); }
           
            conn.createStatement().executeUpdate(createTransaction);
            
         }
         catch (SQLException e)
         {
            log.debug("Failed to create transaction table: " + (storeXid ? createTransaction : createTransaction), e);
         }
                  
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating DELIVERY table"); }            
            conn.createStatement().executeUpdate(createDelivery);
         }
         catch (SQLException e)
         {
            log.debug("Failed to create delivery table: " + createDelivery, e);
         }
         
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating MESSAGE_REFERENCE table"); }            
            conn.createStatement().executeUpdate(createMessageReference);
         }
         catch (SQLException e)
         {
            log.debug("Failed to create message reference table: " + createMessageReference, e);
         }

         if (log.isTraceEnabled()) { log.trace("Creating MESSAGE table"); }
         try
         {
            conn.createStatement().executeUpdate(createMessage);         
         }
         catch (SQLException e)
         {
            log.debug("Failed to create message table: " + createMessage, e);
         }
         
         if (log.isTraceEnabled()) { log.trace("Creating indexes"); }
         
         try
         {
            conn.createStatement().executeUpdate(createIdxMessageRefTx);         
         }
         catch (SQLException e)
         {
            log.debug("Failed to create index: " + createIdxMessageRefTx, e);
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
      insertDelivery = sqlProperties.getProperty("INSERT_DELIVERY", insertDelivery);
      deleteDelivery = sqlProperties.getProperty("DELETE_DELIVERY", deleteDelivery);
      updateDelivery = sqlProperties.getProperty("UPDATE_DELIVERY", updateDelivery);
      insertMessageRef = sqlProperties.getProperty("INSERT_MESSAGE_REF", insertMessageRef);
      deleteMessageRef = sqlProperties.getProperty("DELETE_MESSAGE_REF", deleteMessageRef);
      commitDelivery = sqlProperties.getProperty("COMMIT_DELIVERY", commitDelivery);      
      commitMessageRef = sqlProperties.getProperty("COMMIT_MESSAGE_REF", commitMessageRef);      
      rollbackDelivery = sqlProperties.getProperty("ROLLBACK_DELIVERY", rollbackDelivery);      
      rollbackMessageRef = sqlProperties.getProperty("ROLLBACK_MESSAGE_REF", rollbackMessageRef);      
      selectChannelDeliveries = sqlProperties.getProperty("SELECT_DELIVERIES", selectChannelDeliveries);      
      deleteChannelDeliveries = sqlProperties.getProperty("DELETE_CHANNEL_DELIVERIES", deleteChannelDeliveries);      
      deleteChannelMessageRefs = sqlProperties.getProperty("DELETE_CHANNEL_MESSAGE_REFS", deleteChannelMessageRefs);      
      selectChannelMessageRefs = sqlProperties.getProperty("SELECT_CHANNEL_MESSAGE_REFS", selectChannelMessageRefs);
      deleteMessage = sqlProperties.getProperty("DELETE_MESSAGE", deleteMessage);
      insertMessage = sqlProperties.getProperty("INSERT_MESSAGE", insertMessage);
      selectMessage = sqlProperties.getProperty("SELECT_MESSAGE", selectMessage);
      commitDelivery = sqlProperties.getProperty("COMMIT_DELIVERY", commitDelivery);            
      createTransaction = sqlProperties.getProperty("CREATE_TRANSACTION", createTransaction); 
      createDelivery = sqlProperties.getProperty("CREATE_DELIVERY", createDelivery);      
      createMessageReference = sqlProperties.getProperty("CREATE_MESSAGE_REF", createMessageReference);      
      createMessage = sqlProperties.getProperty("CREATE_MESSAGE", createMessage);
      insertTransactionXA = sqlProperties.getProperty("INSERT_TRANSACTION_XA", insertTransactionXA);
      insertTransaction = sqlProperties.getProperty("INSERT_TRANSACTION", insertTransaction);
      deleteTransaction = sqlProperties.getProperty("DELETE_TRANSACTION", deleteTransaction); 
      selectPreparedTransactions = sqlProperties.getProperty("SELECT_XA_TRANSACTIONS", selectPreparedTransactions);
      createIdxMessageRefTx = sqlProperties.getProperty("CREATE_IDX_MESSAGE_REF_TX", createIdxMessageRefTx);      
      selectTransactionId = sqlProperties.getProperty("SELECT_TX_ID", selectTransactionId);   
      prepareTransaction = sqlProperties.getProperty("PREPARE_TX", prepareTransaction);
      
      createTablesOnStartup = sqlProperties.getProperty("CREATE_TABLES_ON_STARTUP", "true").equalsIgnoreCase("true");      
      storeXid = sqlProperties.getProperty("STORE_XID", "true").equalsIgnoreCase("true");
      txIdGUID = sqlProperties.getProperty("TX_ID_IS_GUID", "false").equalsIgnoreCase("true");
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
      PreparedStatement ps = null;
      ResultSet rs = null;
      try
      {
         boolean xa = storeXid && tx.getXid() != null;
         
         ps = conn.prepareStatement(xa ? insertTransactionXA : insertTransaction);
         
         if (txIdGUID)
         {
            String txID = new GUID().toString();
            tx.setGuidTxID(txID);
            ps.setString(1, txID);
         }
         else
         {
            ps.setNull(1, Types.BIGINT);
         }
         
         if (xa)
         {
            Xid xid = tx.getXid();
            this.setBytes(ps, 2, xid.getBranchQualifier());
            ps.setInt(3, xid.getFormatId());
            this.setBytes(ps, 4, xid.getGlobalTransactionId());
         }

         int rows = ps.executeUpdate();
         
         if (!txIdGUID)
         {
            ps.close();
            ps = conn.prepareStatement(selectTransactionId);
            rs = ps.executeQuery();
            rs.next();
            long txID = rs.getLong(1);
            tx.setLongTxId(txID);               
         }

         if (log.isTraceEnabled())
         { 
            log.trace(JDBCUtil.statementToString(xa ? insertTransactionXA : insertTransaction, getTxId(tx)) + " inserted " + rows + " row(s)"); 
         }
      }
      finally
      {
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

         
         if (log.isTraceEnabled())
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
         //TODO
         throw new RuntimeException("BLOB_TYPE: BLOB_BLOB is not yet implemented.");         
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
         //TODO
         throw new RuntimeException("BLOB_TYPE: BLOB_BLOB is not yet implemented.");         
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
   
   protected void addDelivery(Serializable channelID, Delivery d, Connection conn) throws SQLException
   {
      if (log.isTraceEnabled()) { log.trace("Adding delivery " + d + " to channel: " + channelID); }
            
      PreparedStatement ps = null;
      MessageReference ref = null;
      String messageID = null;
      String receiverID = "";
      String storeID = null;

      try
      {
         ref = d.getReference();
         messageID = (String)ref.getMessageID();
         if (d instanceof StatefulReceiverDelivery)
         {
            receiverID = (String)((StatefulReceiverDelivery)d).getReceiverID();
         }
         storeID = (String)ref.getStoreID();
         ps = conn.prepareStatement(insertDelivery);

         ps.setString(1, (String)channelID);
         ps.setString(2, messageID);
         ps.setString(3, receiverID);
         ps.setString(4, storeID);
         ps.setNull(5, java.sql.Types.VARCHAR);
         ps.setString(6, "C");
         int rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(insertDelivery, channelID, messageID, receiverID, storeID, null, "C") + " inserted " + rows + " row(s)"); }
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
   
   protected void removeDelivery(Serializable channelID, Delivery d, Transaction tx, Connection conn)
      throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Removing delivery " + d + " from channel: "  + channelID + (tx == null ? " non-transactionally" : " on transaction: " + tx));}
            
      PreparedStatement ps = null;
      try
      {
         insertTx(conn, tx);

         int updated;

         String messageID = (String)d.getReference().getMessageID();
         String receiverID = "";

         if (d instanceof StatefulReceiverDelivery)
         {
            receiverID = (String)((StatefulReceiverDelivery)d).getReceiverID();
         }

         if (tx == null)
         {
            //Non transacted case
            ps = conn.prepareStatement(deleteDelivery);
            ps.setString(1, (String)channelID);
            ps.setString(2, messageID);
            ps.setString(3, receiverID);

            updated = ps.executeUpdate();

            if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(deleteDelivery, channelID, messageID, receiverID) + " removed/updated " + updated + " row(s)"); }
         }
         else
         {
            //Transacted case
            ps = conn.prepareStatement(updateDelivery);
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
            ps.setString(4, receiverID);

            updated = ps.executeUpdate();

            if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(updateDelivery, getTxId(tx), channelID, messageID, receiverID) + " updated " + updated + " row(s)"); }
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

   protected void addReference(Serializable channelID, MessageReference ref, Transaction tx, Connection conn)
      throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Adding reference " + ref + " to channel: "  + channelID + (tx == null ? " non-transactionally" : " on transaction: " + tx));}
            
      PreparedStatement ps = null;
      try
      {      
         insertTx(conn, tx);

         String state;
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
         ps.setString(2, (String)ref.getMessageID());
         ps.setString(3, (String)ref.getStoreID());
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
         ps.setString(5, (String)state);
         ps.setLong(6, ref.getOrdering());

         int inserted = ps.executeUpdate();
         
         if (log.isTraceEnabled())
         {
            log.trace(JDBCUtil.statementToString(insertMessageRef, channelID, ref.getMessageID(), ref.getStoreID(),
                  getTxId(tx), state, new Long(ref.getOrdering())) + " inserted " + inserted + " row(s)");
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
            catch (Throwable e)
            {}
         }  
      }
   }
   

   protected void removeReference(Serializable channelID, MessageReference ref, Connection conn) throws SQLException
   {
      if (log.isTraceEnabled()) { log.trace("Removing reference " + ref + " from channel: "  + channelID);}
      
      PreparedStatement ps = null;
      try
      {
         ps = conn.prepareStatement(deleteMessageRef);
         ps.setString(1, (String)channelID);
         ps.setString(2, (String)ref.getMessageID());
         int rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(deleteMessageRef, channelID, ref.getMessageID()) + " deleted " + rows + " row(s)"); }
 
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
      }
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
   class TransactionWrapper
   {
      private javax.transaction.Transaction oldTx;

      private TransactionWrapper() throws Exception
      {
         oldTx = mgr.suspend();

         mgr.begin();
      }

      private void end() throws Exception
      {
         try
         {
            if (Status.STATUS_MARKED_ROLLBACK == mgr.getStatus())
            {
               mgr.rollback();
            }
            else
            {
               mgr.commit();
            }
         }
         finally
         {
            if (oldTx != null)
            {
               mgr.resume(oldTx);
            }
         }
      }

      private void exceptionOccurred() throws Exception
      {
         mgr.setRollbackOnly();
      }
   }

}
