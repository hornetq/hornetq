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
import org.jboss.messaging.core.message.Factory;
import org.jboss.logging.Logger;

import javax.sql.DataSource;
import javax.naming.InitialContext;
import javax.transaction.TransactionManager;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
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

      List result = new ArrayList();

      Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGEID, STOREID FROM DELIVERIES " +
                   "WHERE CHANNELID = '" + channelID + "'";
      ResultSet rs = conn.createStatement().executeQuery(sql);
      if (log.isTraceEnabled()) { log.trace(sql); }

      while (rs.next())
      {
         String id = rs.getString("MESSAGEID");
         String storeID = rs.getString("STOREID");
         result.add(new StorageIdentifier(id, storeID));
      }

      conn.close();
      return result;
   }


   public void add(Serializable channelID, MessageReference ref) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("Current transaction: " + currentTransactionToString()); }

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


   public boolean remove(Serializable channelID, MessageReference ref) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("Current transaction: " + currentTransactionToString()); }

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
      conn.close();
      return result;
   }

   public void store(Message m) throws Throwable
   {
      Connection conn = ds.getConnection();
      String sql = "INSERT INTO MESSAGES VALUES ( ?, ?, ?, ?, ?)";
      PreparedStatement stat = conn.prepareStatement(sql);
      stat.setString(1, (String)m.getMessageID());
      stat.setBoolean(2, m.isReliable());
      stat.setLong(3, m.getExpiration());
      stat.setLong(4, m.getTimestamp());
      stat.setObject(5, m.getPayload());
      stat.executeUpdate();
      if (log.isTraceEnabled()) { log.trace(stat); }
      conn.close();
   }

   public Message retrieve(Serializable messageID) throws Throwable
   {

      Message m = null;
      Connection conn = ds.getConnection();

      String sql = "SELECT MESSAGEID, RELIABLE, EXPIRATION, TIMESTAMP, BODY FROM MESSAGES WHERE " +
                   "MESSAGEID = '" + messageID + "'";
      ResultSet rs = conn.createStatement().executeQuery(sql);
      if (log.isTraceEnabled()) { log.trace(sql); }
      if (rs.next())
      {
         String id = rs.getString("MESSAGEID");
         boolean reliable = rs.getBoolean("RELIABLE");
         long expiration = rs.getLong("EXPIRATION");
         long timestamp = rs.getLong("TIMESTAMP");
         Serializable body = (Serializable) rs.getObject("BODY");
         m = Factory.createMessage(id, reliable, expiration, timestamp, body);
      }
      conn.close();
      return m;
   }



   // Public --------------------------------------------------------

   public void start() throws Exception
   {
      //FIXME - Sort this out so it doesn't assume the tables are already there!!!
      try
      {
         InitialContext ic = new InitialContext();
         tm = (TransactionManager)ic.lookup("java:/TransactionManager");
         ds = (DataSource)ic.lookup("java:/DefaultDS");
         ic.close();
   
         Connection conn = ds.getConnection();
         String sql = null;
   
         sql = "CREATE TABLE DELIVERIES (CHANNELID VARCHAR, MESSAGEID VARCHAR, STOREID VARCHAR)";
         conn.createStatement().executeUpdate(sql);
         sql = "CREATE TABLE MESSAGE_REFERENCES (CHANNELID VARCHAR, MESSAGEID VARCHAR, STOREID VARCHAR)";
         conn.createStatement().executeUpdate(sql);
         sql = "CREATE TABLE TRANSACTIONAL_MESSAGE_REFERENCES (CHANNELID VARCHAR, TXID VARCHAR, MESSAGEID VARCHAR, STOREID VARCHAR)";
         conn.createStatement().executeUpdate(sql);
         sql = "CREATE TABLE MESSAGES (MESSAGEID VARCHAR, RELIABLE BOOLEAN, EXPIRATION BIGINT, TIMESTAMP BIGINT, BODY OBJECT)";
         conn.createStatement().executeUpdate(sql);
   
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
