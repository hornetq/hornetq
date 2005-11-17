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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.jms.Destination;
import javax.jms.JMSException;
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
import org.jboss.messaging.core.message.MessageSupport;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.XidImpl;

/**
 *  
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Partially derived from org.jboss.mq.pm.jdbc2.PersistenceManager by:
 * 
 * @author Jayesh Parayali (jayeshpk1@yahoo.com)
 * @author Hiram Chirino (cojonudo14@hotmail.com)
 * @author Adrian Brock (adrian@jboss.org)
 * 
 * @version <tt>$Revision$</tt>
 *
 * $Id$
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
      "INSERT INTO DELIVERY (CHANNELID, MESSAGEID, STOREID, TRANSACTIONID, STATE) " +
      "VALUES (?, ?, ?, ?, ?)";
   
   protected String deleteDelivery =
      "DELETE FROM DELIVERY WHERE CHANNELID=? AND MESSAGEID=? AND STATE='C'";
   
   protected String updateDelivery =
      "UPDATE DELIVERY SET TRANSACTIONID=?, STATE='-' " +
      "WHERE CHANNELID=? AND MESSAGEID=? AND STATE='C'";
   
   protected String insertMessageRef =
      "INSERT INTO MESSAGE_REFERENCE (CHANNELID, MESSAGEID, STOREID, TRANSACTIONID, STATE) " +
      "VALUES (?, ?, ?, ?, ?)";
   
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
      "SELECT MESSAGEID FROM MESSAGE_REFERENCE WHERE CHANNELID=? AND STOREID=? AND STATE='C'";
   
   protected String insertMessage =
      "INSERT INTO MESSAGE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

   protected String deleteMessage =
      "DELETE FROM MESSAGE WHERE MESSAGEID=?";

   protected String selectMessage =
      "SELECT " +
      "MESSAGEID, " +
      "RELIABLE, " +
      "EXPIRATION, " +
      "TIMESTAMP, " +
      "COREHEADERS, " +
      "PAYLOAD, " +
      "TYPE, " +
      "JMSTYPE, " +
      "PRIORITY, " +
      "CORRELATIONID, " +
      "DESTINATIONISQUEUE, " +
      "DESTINATION, " +
      "REPLYTOISQUEUE, " +
      "REPLYTO, " +
      "JMSPROPERTIES, " +
      "REFERENCECOUNT " +
      "FROM MESSAGE WHERE MESSAGEID = ?";

   protected String selectMessageReferenceCount =
      "SELECT REFERENCECOUNT FROM MESSAGE WHERE MESSAGEID = ?";

   protected String updateReferenceCount =
      "UPDATE MESSAGE SET REFERENCECOUNT=? WHERE MESSAGEID=?";

   protected String createTransaction =
      "CREATE TABLE TRANSACTION ( TRANSACTIONID INTEGER, PRIMARY KEY (TRANSACTIONID) )";
   
   protected String createTransactionXA = 
      "CREATE TABLE TRANSACTION ( TRANSACTIONID INTEGER, BRANCH_QUAL OBJECT, FORMAT_ID INTEGER, " +
      "GLOBAL_TXID OBJECT, PRIMARY KEY (TRANSACTIONID) )";
   
   protected String createDelivery =
      "CREATE TABLE DELIVERY (CHANNELID VARCHAR(256), MESSAGEID VARCHAR(256), " +
      "STOREID VARCHAR(256), TRANSACTIONID INTEGER, STATE CHAR(1), PRIMARY KEY(CHANNELID, MESSAGEID))";
   
   protected String createMessageReference =
      "CREATE TABLE MESSAGE_REFERENCE (CHANNELID VARCHAR(256), MESSAGEID VARCHAR(256), " +
      "STOREID VARCHAR(256), TRANSACTIONID INTEGER, STATE CHAR(1), PRIMARY KEY(CHANNELID, MESSAGEID))";
   
   protected String createMessage =
      "CREATE TABLE MESSAGE (" +
      "MESSAGEID VARCHAR(256), " +
      "RELIABLE CHAR(1), " +
      "EXPIRATION BIGINT, " +
      "TIMESTAMP BIGINT, " +
      "COREHEADERS OBJECT, " +
      "PAYLOAD OBJECT, " +
      "TYPE INTEGER, " +
      "JMSTYPE VARCHAR(256), " +
      "PRIORITY TINYINT, " +
      "CORRELATIONID OBJECT, " +
      "DESTINATIONISQUEUE CHAR(1), " +
      "DESTINATION VARCHAR(256), " +
      "REPLYTOISQUEUE CHAR(1), " +
      "REPLYTO VARCHAR(256), " +
      "JMSPROPERTIES OBJECT, " +
      "REFERENCECOUNT INTEGER, " +
      "PRIMARY KEY (MESSAGEID))";

   protected String insertTransactionXA =
      "INSERT INTO TRANSACTION (TRANSACTIONID, BRANCH_QUAL, FORMAT_ID, GLOBAL_TXID) " +
      "VALUES(?, ?, ?, ?)";

   protected String insertTransaction =
      "INSERT INTO TRANSACTION (TRANSACTIONID) VALUES (?)";
   
   protected String deleteTransaction =
      "DELETE FROM TRANSACTION WHERE TRANSACTIONID = ?";
   
   protected String selectXATransactions =
      "SELECT TRANSACTIONID, BRANCH_QUAL, FORMAT_ID, GLOBAL_TXID FROM TRANSACTION " +
      "WHERE FORMAT_ID <> NULL";
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected DataSource ds;

   protected TransactionManager mgr;
   
   protected boolean storeXid;
   
   protected int bytesStoredAs;
   
   protected Properties sqlProperties;
   
   protected boolean createTablesOnStartup;


   // Constructors --------------------------------------------------

   public JDBCPersistenceManager() throws Exception
   {
      bytesStoredAs = BYTES_AS_BYTES;
      
      sqlProperties = new Properties();
            
      start();
   }

   // PersistenceManager implementation -----------------------------

   /*
    * Addition of a delivery is never done in a jms transaction
    */
   public void addDelivery(Serializable channelID, Delivery d) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Adding delivery " + d + " to channel: "  + channelID);}

      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         MessageReference ref = d.getReference();
         conn = ds.getConnection();         
         ps = conn.prepareStatement(insertDelivery);
         ps.setString(1, (String)channelID);
         ps.setString(2, (String)ref.getMessageID());
         ps.setString(3, (String)ref.getStoreID());
         ps.setNull(4, java.sql.Types.VARCHAR);
         ps.setString(5, "C");
         int rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(insertDelivery, channelID, ref.getMessageID(), ref.getStoreID(), null, "C") + " inserted " + rows + " row(s)"); }
      }
      catch (SQLException e)
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
            catch (Throwable t)
            {}
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable t)
            {}
         }
         wrap.end();
      }
   }

   /*
    * Removal of a delivery can be done in a jms transaction
    */
   public boolean removeDelivery(Serializable channelID, Delivery d, Transaction tx) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Removing delivery " + d + " from channel: "  + channelID + (tx == null ? " non-transactionally" : " on transaction: " + tx));}
            
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();
      try
      {
         conn = ds.getConnection();
         
         insertTx(conn, tx);
         
         int updated;
         if (tx == null)
         {
            //Non transacted case
            ps = conn.prepareStatement(deleteDelivery);
            ps.setString(1, (String)channelID);
            ps.setString(2, (String)d.getReference().getMessageID());
            
            updated = ps.executeUpdate();

            if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(deleteDelivery, channelID, d.getReference().getMessageID()) + " removed/updated " + updated + " row(s)"); }
         }
         else
         {
            //Transacted case
            ps = conn.prepareStatement(updateDelivery);
            ps.setString(1, String.valueOf(tx.getID()));
            ps.setString(2, (String)channelID);
            ps.setString(3, (String)d.getReference().getMessageID());

            updated = ps.executeUpdate();

            if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(updateDelivery, String.valueOf(tx.getID()), channelID, d.getReference().getMessageID()) + " updated " + updated + " row(s)"); }
         }

         return updated == 1;
      }
      catch (SQLException e)
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
            catch (Throwable t)
            {}
         }
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Throwable t)
            {}
         }
         wrap.end();
      }
   }


   /*
    * Message references can be added transactionally
    */
   public void addReference(Serializable channelID, MessageReference ref, Transaction tx) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Adding " + ref + " for channel "  + channelID + (tx == null ? " non-transactionally" : " in transaction: " + tx));}

      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();
                  
         insertTx(conn, tx);

         String state;
         String txID = null;

         if (tx == null)
         {
            state = "C";
         }
         else
         {
            txID = String.valueOf(tx.getID());
            state = "+";
         }
         
         ps = conn.prepareStatement(insertMessageRef);
         ps.setString(1, (String)channelID);
         ps.setString(2, (String)ref.getMessageID());
         ps.setString(3, (String)ref.getStoreID());
         if (txID == null)
         {
            ps.setNull(4, java.sql.Types.VARCHAR);
         }
         else
         {
            ps.setString(4, (String)txID);
         }
         ps.setString(5, (String)state);

         int inserted = ps.executeUpdate();
         
         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(insertMessageRef, channelID, ref.getMessageID(), ref.getStoreID(), txID, state) + " inserted " + inserted + " row(s)"); }
      }
      catch (SQLException e)
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
    * Message reference removal never happens in a jms transaction
    */
   public boolean removeReference(Serializable channelID, MessageReference ref) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Removing " + ref + " from channel " + channelID); }
      
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(deleteMessageRef);
         ps.setString(1, (String)channelID);
         ps.setString(2, (String)ref.getMessageID());
         int rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(deleteMessageRef, channelID, ref.getMessageID()) + " deleted " + rows + " row(s)"); }
  
         return rows == 1;
      }
      catch (SQLException e)
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
         ps.setString(1, String.valueOf(tx.getID()));
         
         int rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(commitDelivery, String.valueOf(tx.getID())) + " removed " + rows + " row(s)"); }
                  
         ps.close();
         ps = conn.prepareStatement(commitMessageRef);
         ps.setNull(1, Types.VARCHAR);
         ps.setString(2, String.valueOf(tx.getID()));
         
         rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(commitMessageRef, null, String.valueOf(tx.getID())) + " updated " + rows + " row(s)"); }

         removeTXRecord(conn, tx);
      }
      catch (SQLException e)
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
      if (log.isTraceEnabled()) { log.trace("Rolling back transaction:" + tx.getID()); }
      
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();

         ps = conn.prepareStatement(rollbackMessageRef);
         ps.setString(1, String.valueOf(tx.getID()));
         
         int rows = ps.executeUpdate();
         
         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(rollbackMessageRef, String.valueOf(tx.getID())) + " removed " + rows + " row(s)"); }
         
         ps.close();
         
         ps = conn.prepareStatement(rollbackDelivery);
         ps.setNull(1, Types.VARCHAR);
         ps.setString(2, String.valueOf(tx.getID()));
         
         rows = ps.executeUpdate();
         
         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(rollbackDelivery, null, String.valueOf(tx.getID())) + " updated " + rows + " row(s)"); }
                  
         removeTXRecord(conn, tx);
      }
      catch (SQLException e)
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
      catch (SQLException e)
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
      catch (SQLException e)
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
      catch (SQLException e)
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

            ps = conn.prepareStatement(insertMessage);
            ps.setString(1, id);
            ps.setString(2, m.isReliable() ? "Y" : "N");
            ps.setLong(3, m.getExpiration());
            ps.setLong(4, m.getTimestamp());
            ps.setObject(5, ((MessageSupport)m).getHeaders());
            //stat.setObject(6, m.getPayload());


            /*
            * FIXME - Tidy this up
            *
            * I have changed this for now to serialize the whole message rather than just the payload.
            * This is because the message contains hidden protected fields e.g. bodyWriteOnly which
            * are part of it's state.
            * This can probably be done in a more elegant way
            * Just saving the payload and a few fields doesn't persist these fields
            * so when they are read back, the state is wrong.
            * This has been causing a lot of previouly passing TCK tests to fail.
            *
            */

            ps.setObject(6, m);

            int type = -1;
            String jmsType = null;
            int priority = -1;
            Object correlationID = null;
            boolean destIsQueue = false;
            String dest = null;
            boolean replyToIsQueue = false;
            String replyTo = null;
            Map jmsProperties = null;

            if (m instanceof JBossMessage)
            {
               JBossMessage jbm = (JBossMessage)m;
               type = jbm.getType();
               jmsType = jbm.getJMSType();
               priority = jbm.getJMSPriority();

               // TODO TODO_CORRID
               try
               {
                  correlationID = jbm.getJMSCorrelationID();
               }
               catch(JMSException e)
               {
                  // is this exception really supposed to be thrown?
                  correlationID = jbm.getJMSCorrelationIDAsBytes();
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

            ps.setInt(7, type);
            ps.setString(8, jmsType);
            ps.setInt(9, priority);
            ps.setObject(10, correlationID);
            ps.setString(11, destIsQueue ? "Y" : "N");
            ps.setString(12, dest);
            ps.setString(13, replyToIsQueue ? "Y" : "N");
            ps.setString(14, replyTo);
            ps.setObject(15, jmsProperties);
            ps.setInt(16, 1);

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
      catch (SQLException e)
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
      catch (SQLException e)
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
            m = (Message)rs.getObject("PAYLOAD");
            referenceCount = rs.getInt("REFERENCECOUNT");
            count ++;
            /*
            m = Factory.createMessage(rs.getString("MESSAGEID"),
                                      rs.getBoolean("RELIABLE"),
                                      rs.getLong("EXPIRATION"),
                                      rs.getLong("TIMESTAMP"),
                                      (Map)rs.getObject("COREHEADERS"),
                                      (Serializable)rs.getObject("PAYLOAD"),
                                      rs.getInt("TYPE"),
                                      rs.getString("JMSTYPE"),
                                      rs.getInt("PRIORITY"),
                                      rs.getObject("CORRELATIONID"),
                                      rs.getBoolean("DESTINATIONISQUEUE"),
                                      rs.getString("DESTINATION"),
                                      rs.getBoolean("REPLYTOISQUEUE"),
                                      rs.getString("REPLYTO"),
                                      (Map)rs.getObject("JMSPROPERTIES"));
              */                        
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
      catch (SQLException e)
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
      catch (SQLException e)
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
         rs = st.executeQuery(selectXATransactions);
         
         while (rs.next())
         {            
            //int transactionId = rs.getInt(1);
            byte[] branchQual = this.getBytes(rs, 2, 0);
            int formatId = rs.getInt(3);
            byte[] globalTxId = this.getBytes(rs, 3, 0);
            Xid xid = new XidImpl(branchQual, formatId, globalTxId);
            
            transactions.add(xid);
         }
         
         return transactions;
      
      }
      catch (SQLException e)
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

   public void start() throws Exception
   {
      initSqlProperties();
      
      InitialContext ic = new InitialContext();
      mgr = (TransactionManager)ic.lookup("java:/TransactionManager");

      ds = (DataSource)ic.lookup("java:/DefaultDS");
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
            if (storeXid)
            {
               conn.createStatement().executeUpdate(createTransactionXA);
            }
            else
            {
               conn.createStatement().executeUpdate(createTransaction);
            }
         }
         catch (SQLException e) {
         }
                  
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating DELIVERY table"); }            
            conn.createStatement().executeUpdate(createDelivery);
         }
         catch (SQLException e) {}
         
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating MESSAGE_REFERENCE table"); }            
            conn.createStatement().executeUpdate(createMessageReference);
         }
         catch (SQLException e) {}

         if (log.isTraceEnabled()) { log.trace("Creating MESSAGE table"); }
         try
         {
            conn.createStatement().executeUpdate(createMessage);         
         }
         catch (SQLException e) {}
         
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
      selectXATransactions = sqlProperties.getProperty("SELECT_XA_TRANSACTIONS", selectXATransactions);
      createTablesOnStartup = sqlProperties.getProperty("CREATE_TABLES_ON_STARTUP", "true").equalsIgnoreCase("true");
      storeXid = sqlProperties.getProperty("STORE_XID", "true").equalsIgnoreCase("true");
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
      try
      {
         String sql = null;
         
         boolean xa = storeXid && tx.getXid() != null;
         
         if (xa)
         {
            sql = "INSERT INTO TRANSACTION (TRANSACTIONID, BRANCH_QUAL, FORMAT_ID, GLOBAL_TXID) values(?, ?, ?, ?)";
         }
         else
         {
            sql = "INSERT INTO TRANSACTION (TRANSACTIONID) values(?)";
         }
         
         ps = conn.prepareStatement(sql);
         
         ps.setString(1, String.valueOf(tx.getID()));
         if (xa)
         {
            Xid xid = tx.getXid();
            this.setBytes(ps, 2, xid.getBranchQualifier());
            ps.setInt(3, xid.getFormatId());
            this.setBytes(ps, 4, xid.getGlobalTransactionId());
         }

         int rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, String.valueOf(tx.getID())) + " inserted " + rows + " row(s)"); }
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

   protected void removeTXRecord(Connection conn, Transaction tx) throws SQLException
   {
      PreparedStatement ps = null;
      try
      {
         String sql = "DELETE FROM TRANSACTION WHERE TRANSACTIONID = ?";

         ps = conn.prepareStatement(sql);
         ps.setString(1, String.valueOf(tx.getID()));
         int rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, String.valueOf(tx.getID())) + " removed " + rows + " row(s)"); }
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
