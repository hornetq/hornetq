/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.persistence;

import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.message.StorageIdentifier;
import org.jboss.messaging.core.message.MessageSupport;
import org.jboss.logging.Logger;
import org.jboss.jms.message.JBossMessage;

import javax.sql.DataSource;
import javax.naming.InitialContext;
import javax.transaction.TransactionManager;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.JMSException;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class HSQLDBPersistenceManager implements PersistenceManager
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(HSQLDBPersistenceManager.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private DataSource ds;
   private TransactionManager tm;

   // Constructors --------------------------------------------------

   public HSQLDBPersistenceManager() throws Exception
   {
      start();
   }

   // PersistenceManager implementation -----------------------------

   public void add(Serializable channelID, Delivery d) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("Current transaction: " + currentTransactionToString()); }
      
      if (log.isTraceEnabled()) { log.trace("Adding delivery " + d + " to channel: "  + channelID);}

      Routable r = d.getRoutable();

      if (!r.isReference())
      {
         throw new IllegalStateException("Delivery must contain a message reference");
      }

      MessageReference ref = (MessageReference)r;

      Connection conn = ds.getConnection();
      String sql =
            "INSERT INTO DELIVERIES VALUES ('" +
            channelID + "', '" + ref.getMessageID() + "', '" + ref.getStoreID() + "')";

      conn.createStatement().executeUpdate(sql);
      if (log.isTraceEnabled()) { log.trace(sql); }
      conn.close();
   }

   public boolean remove(Serializable channelID, Delivery d) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("Current transaction: " + currentTransactionToString()); }
      
      if (log.isTraceEnabled()) { log.trace("Removing delivery " + d + " from channel: "  + channelID);}

      Serializable messageID = d.getRoutable().getMessageID();

      Connection conn = ds.getConnection();

      String sql = "SELECT MESSAGEID FROM DELIVERIES WHERE " +
            "CHANNELID = '" + channelID + "' AND " +
            "MESSAGEID = '" + messageID+ "'";

      ResultSet rs = conn.createStatement().executeQuery(sql);
      if (log.isTraceEnabled()) { log.trace(sql); }

      if(!rs.next())
      {
         if (log.isTraceEnabled()) { log.trace("no delivery in database"); }
         conn.close();
         return false;
      }

      sql = "DELETE FROM DELIVERIES WHERE " +
            "CHANNELID = '" + channelID + "' AND " +
            "MESSAGEID = '" + d.getRoutable().getMessageID() + "'";

      conn.createStatement().executeUpdate(sql);
      if (log.isTraceEnabled()) { log.trace(sql); }
      conn.close();

      return true;
   }

   public List deliveries(Serializable channelID) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("Current transaction: " + currentTransactionToString()); }
      
      if (log.isTraceEnabled()) { log.trace("listing deliveries for channel: " + channelID); }

      List result = new ArrayList();

      Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGEID, STOREID FROM DELIVERIES " +
                   "WHERE CHANNELID = '" + channelID + "'";
      ResultSet rs = conn.createStatement().executeQuery(sql);
      if (log.isTraceEnabled()) { log.trace(sql); }

      int count = 0;
      while (rs.next())
      {
         String id = rs.getString("MESSAGEID");
         String storeID = rs.getString("STOREID");
         result.add(new StorageIdentifier(id, storeID));
         count++;
      }
      
      if (log.isTraceEnabled()) { log.trace("There are " + count + " deliveries"); }

      conn.close();
      return result;
   }


   public void add(Serializable channelID, MessageReference ref) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("Current transaction: " + currentTransactionToString()); }
      
      if (log.isTraceEnabled()) { log.trace("Adding message ref " + ref + " to channel: "  + channelID);}

      Connection conn = ds.getConnection();
      String sql =
            "INSERT INTO MESSAGE_REFERENCES VALUES ( '" +
            channelID + "', '" + ref.getMessageID() +  "', '" + ref.getStoreID() + "')";

      conn.createStatement().executeUpdate(sql);
      if (log.isTraceEnabled()) { log.trace(sql); }
      conn.close();
   }

   public void add(Serializable channelID, Serializable txID, MessageReference ref)
         throws SystemException
   {
      try
      {
         if (log.isTraceEnabled()) { log.trace("Current transaction: " + currentTransactionToString()); }
         
         if (log.isTraceEnabled()) { log.trace("Adding transactional message ref " + ref + " to channel: "  + channelID);}

         Connection conn = ds.getConnection();
         String sql =
               "INSERT INTO TRANSACTIONAL_MESSAGE_REFERENCES VALUES ( '" +
               channelID + "', '" + txID + "', '" +
               ref.getMessageID() + "', '" + ref.getStoreID() +"')";

         conn.createStatement().executeUpdate(sql);
         if (log.isTraceEnabled()) { log.trace(sql); }
         conn.close();


      }
      catch(Throwable t)
      {
         log.error("", t);
         tm.setRollbackOnly();
      }
   }

   public void enableTransactedMessages(Serializable channelID, Serializable txID)
         throws SystemException
   {
      try
      {
         if (log.isTraceEnabled()) { log.trace("Current transaction: " + currentTransactionToString()); }
         
         if (log.isTraceEnabled()) { log.trace("Enabling transacted messages for " + channelID); }

         Connection conn = ds.getConnection();
         Statement stat = conn.createStatement();

         String sql = "SELECT MESSAGEID, STOREID FROM TRANSACTIONAL_MESSAGE_REFERENCES WHERE " +
                      "CHANNELID = '" + channelID + "' AND " +
                      "TXID = '" + txID + "'";

         ResultSet rs = stat.executeQuery(sql);
         if (log.isTraceEnabled()) { log.trace(sql); }
         while (rs.next())
         {
            sql = "INSERT INTO MESSAGE_REFERENCES VALUES ('" +
                  channelID + "', '" +
                  rs.getString("MESSAGEID") + "', '" +
                  rs.getString("STOREID") + "')";

            stat.executeUpdate(sql);
            if (log.isTraceEnabled()) { log.trace(sql); }
         }

         sql = "DELETE FROM TRANSACTIONAL_MESSAGE_REFERENCES WHERE " +
               "CHANNELID = '" + channelID + "' AND " +
               "TXID = '" + txID + "'";

         stat.executeUpdate(sql);
         if (log.isTraceEnabled()) { log.trace(sql); }

         conn.close();
      }
      catch(Throwable t)
      {
         log.error("Failed to enable transacted undelivered", t);
         tm.setRollbackOnly();
      }
   }

   public void dropTransactedMessages(Serializable channelID, Serializable txID) throws SystemException
   {
      try
      {
         if (log.isTraceEnabled()) { log.trace("Current transaction: " + currentTransactionToString()); }
         
         if (log.isTraceEnabled()) { log.trace("Dropping transacted messages for channel " + channelID); }

         Connection conn = ds.getConnection();

         String sql = "DELETE FROM TRANSACTIONAL_MESSAGE_REFERENCES WHERE " +
                      "CHANNELID = '" + channelID + "' AND " +
                      "TXID = '" + txID + "'";

         conn.createStatement().executeUpdate(sql);
         if (log.isTraceEnabled()) { log.trace(sql); }
         conn.close();
      }
      catch(Throwable t)
      {
         log.error("Failed to drop transacted undelivered", t);
         tm.setRollbackOnly();
      }
   }


   public void removeAllMessageData(Serializable channelID) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Removing all message data for channel: " + channelID); }

      Connection conn = ds.getConnection();


      final String sql1 = "DELETE FROM DELIVERIES WHERE " +
         "CHANNELID = '" + channelID + "'";
      
      final String sql2 = "DELETE FROM MESSAGE_REFERENCES WHERE " +
         "CHANNELID = '" + channelID + "'";

  
      conn.createStatement().executeUpdate(sql1);
      if (log.isTraceEnabled()) { log.trace(sql1); }
      
      conn.createStatement().executeUpdate(sql2);
      if (log.isTraceEnabled()) { log.trace(sql2); }
      
  
      conn.close();

   }
   
   public boolean remove(Serializable channelID, MessageReference ref) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("Current transaction: " + currentTransactionToString()); }
      
      if (log.isTraceEnabled()) { log.trace("Removing message ref " + ref + " from channel " + channelID); }
      
      if (log.isTraceEnabled()) { log.trace("Removing message ref for message id " + ref.getMessageID()); }

      Connection conn = ds.getConnection();

      Serializable messageID = ref.getMessageID();

      String sql =
            "SELECT MESSAGEID FROM MESSAGE_REFERENCES WHERE " +
            "CHANNELID = '" + channelID + "' AND " +
            "MESSAGEID = '" + messageID + "'";

      ResultSet rs = conn.createStatement().executeQuery(sql);
      if (log.isTraceEnabled()) { log.trace(sql); }

      if(!rs.next())
      {
         if (log.isTraceEnabled()) { log.trace("no message reference " + ref + " in database"); }
         conn.close();
         return false;
      }

      sql = "DELETE FROM MESSAGE_REFERENCES WHERE " +
            "CHANNELID = '" + channelID + "' AND " +
            "MESSAGEID = '" + messageID + "'";

      conn.createStatement().executeUpdate(sql);
      if (log.isTraceEnabled()) { log.trace(sql); }
      conn.close();

      return true;
   }

   public List messages(Serializable channelID) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("Current transaction: " + currentTransactionToString()); }
      
      if (log.isTraceEnabled()) { log.trace("Getting message references for channel " + channelID); }

      List result = new ArrayList();
      Connection conn = ds.getConnection();

      String sql = "SELECT MESSAGEID, STOREID FROM MESSAGE_REFERENCES " +
                   "WHERE CHANNELID = '" + channelID + "'";
      ResultSet rs = conn.createStatement().executeQuery(sql);
      if (log.isTraceEnabled()) { log.trace(sql); }
      while (rs.next())
      {
         String id = rs.getString("MESSAGEID");
         String storeID = rs.getString("STOREID");
         result.add(new StorageIdentifier(id, storeID));
      }
      if (log.isTraceEnabled()) { log.trace("got " + result.size() + " message refs"); }
      conn.close();
      return result;
   }

   public void store(Message m) throws Throwable
   {
      Connection conn = ds.getConnection();

      String sql = "INSERT INTO MESSAGES VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";

      PreparedStatement stat = conn.prepareStatement(sql);
      stat.setString(1, (String)m.getMessageID());
      stat.setBoolean(2, m.isReliable());
      stat.setLong(3, m.getExpiration());
      stat.setLong(4, m.getTimestamp());
      stat.setObject(5, ((MessageSupport)m).getHeaders());
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
      
      stat.setObject(6, m);

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

      stat.setInt(7, type);
      stat.setString(8, jmsType);
      stat.setInt(9, priority);
      stat.setObject(10, correlationID);
      stat.setBoolean(11, destIsQueue);
      stat.setString(12, dest);
      stat.setBoolean(13, replyToIsQueue);
      stat.setString(14, replyTo);
      stat.setObject(15, jmsProperties);
      stat.executeUpdate();
      if (log.isTraceEnabled()) { log.trace(stat); }
      conn.close();
   }


   public Message retrieve(Serializable messageID) throws Throwable
   {

      Message m = null;
      Connection conn = ds.getConnection();

      String sql = "SELECT " +
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
                   "JMSPROPERTIES " +
                   "FROM MESSAGES WHERE MESSAGEID = '" + messageID + "'";

      ResultSet rs = conn.createStatement().executeQuery(sql);
      if (log.isTraceEnabled()) { log.trace(sql); }

      if (rs.next())
      {
         m = (Message)rs.getObject("PAYLOAD");
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
      conn.close();
      return m;
   }



   // Public --------------------------------------------------------

   public void start() throws Exception
   {
      InitialContext ic = new InitialContext();
      tm = (TransactionManager)ic.lookup("java:/TransactionManager");
      ds = (DataSource)ic.lookup("java:/DefaultDS");
      ic.close();

      Connection conn = ds.getConnection();
      String sql = null;

      try
      {
         sql = "SELECT MESSAGEID FROM DELIVERIES WHERE MESSAGEID='nosuchmessage'";
         try
         {
            conn.createStatement().executeQuery(sql);
         }
         catch(SQLException e)
         {
            log.debug("Table DELIVERIES not found: " + e.getMessage() +". Creating table.");
            sql = "CREATE TABLE DELIVERIES (CHANNELID VARCHAR, MESSAGEID VARCHAR, STOREID VARCHAR)";
            conn.createStatement().executeUpdate(sql);
         }

         sql = "SELECT MESSAGEID FROM MESSAGE_REFERENCES WHERE MESSAGEID='nosuchmessage'";
         try
         {
            conn.createStatement().executeQuery(sql);
         }
         catch(SQLException e)
         {
            log.debug("Table MESSAGE_REFERENCES not found: " + e.getMessage() +". Creating table.");
            sql = "CREATE TABLE MESSAGE_REFERENCES (CHANNELID VARCHAR, MESSAGEID VARCHAR, STOREID VARCHAR)";
            conn.createStatement().executeUpdate(sql);
         }

         sql = "SELECT MESSAGEID FROM TRANSACTIONAL_MESSAGE_REFERENCES WHERE MESSAGEID='nosuchmessage'";
         try
         {
            conn.createStatement().executeQuery(sql);
         }
         catch(SQLException e)
         {
            log.debug("Table TRANSACTIONAL_MESSAGE_REFERENCES not found: " + e.getMessage() +". Creating table.");
            sql = "CREATE TABLE TRANSACTIONAL_MESSAGE_REFERENCES (CHANNELID VARCHAR, TXID VARCHAR, MESSAGEID VARCHAR, STOREID VARCHAR)";
            conn.createStatement().executeUpdate(sql);
         }

         sql = "SELECT MESSAGEID FROM MESSAGES WHERE MESSAGEID='nosuchmessage'";
         try
         {
            conn.createStatement().executeQuery(sql);
         }
         catch(SQLException e)
         {
            log.debug("Table MESSAGES not found: " + e.getMessage() +". Creating table.");
            sql = "CREATE TABLE MESSAGES (" +
                  "MESSAGEID VARCHAR, " +
                  "RELIABLE BOOLEAN, " +
                  "EXPIRATION BIGINT, " +
                  "TIMESTAMP BIGINT, " +
                  "COREHEADERS OBJECT, " +
                  "PAYLOAD OBJECT, " +
                  "TYPE INT, " +
                  "JMSTYPE VARCHAR, " +
                  "PRIORITY INT, " +
                  "CORRELATIONID OBJECT, " +
                  "DESTINATIONISQUEUE BOOLEAN, " +
                  "DESTINATION VARCHAR, " +
                  "REPLYTOISQUEUE BOOLEAN, " +
                  "REPLYTO VARCHAR, " +
                  "JMSPROPERTIES OBJECT) ";
            conn.createStatement().executeUpdate(sql);
         }

         conn.close();
      }
      catch (SQLException e)
      {
         log.warn("Caught SQLException in starting persistence manager", e);
      }
   }

   public void stop() throws Exception
   {
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private String currentTransactionToString()
   {
      try
      {
         Transaction tx = tm.getTransaction();
         if (tx == null)
         {
            return "NO TRANSACTION";
         }
         return tx.toString();
      }
      catch(Exception e)
      {
         return e.toString();
      }
   }

   // Inner classes -------------------------------------------------
}
