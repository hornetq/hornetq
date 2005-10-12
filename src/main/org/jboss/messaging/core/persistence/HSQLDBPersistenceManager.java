/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.persistence;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.jboss.jms.message.JBossMessage;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.message.MessageSupport;
import org.jboss.messaging.core.message.StorageIdentifier;
import org.jboss.messaging.core.tx.Transaction;

/**
 *  
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
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

   protected DataSource ds;

   // Constructors --------------------------------------------------

   public HSQLDBPersistenceManager(String dbURL) throws Exception
   {
      this.dbURL = dbURL;
      start();
   }

   // PersistenceManager implementation -----------------------------

   /*
    * Addition of a delivery is never done in a jms transaction
    */
   public void add(Serializable channelID, Delivery d) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Adding delivery " + d + " to channel: "  + channelID);}

      Connection conn = null;
      PreparedStatement ps = null;
      //TransactionManagerStrategy tms = new TransactionManagerStrategy();
      
      //tms.startTX();
      try
      {
         MessageReference ref = d.getReference();
         conn = ds.getConnection();         
         String sql = "INSERT INTO DELIVERY (CHANNELID, MESSAGEID, STOREID, TRANSACTIONID, STATE) VALUES (?, ?, ?, ?, ?)";
         ps = conn.prepareStatement(sql);
         ps.setString(1, (String)channelID);
         ps.setString(2, (String)ref.getMessageID());
         ps.setString(3, (String)ref.getStoreID());
         ps.setNull(4, java.sql.Types.VARCHAR);
         ps.setString(5, "C");
         int rows = ps.executeUpdate();

         if (log.isTraceEnabled())
         {
            log.trace(sql);
            log.trace("Inserted " + rows + " rows");
         }
         conn.commit();
      }
      catch (SQLException e)
      {
         try
         {
            conn.rollback();
         }
         catch (Throwable t)
         {}
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
        
      }
   }

   /*
    * Removal of a delivery can be done in a jms transaction
    */
   public boolean remove(Serializable channelID, Delivery d, Transaction tx) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Removing delivery " + d + " from channel: "  + channelID);}
            

      Connection conn = null;
      PreparedStatement ps = null;
      try
      {
         conn = ds.getConnection();
         
         insertTx(conn, tx);
         
         int updated;
         String sql;
         
         if (tx == null)
         {
            //Non transacted case
            sql = "DELETE FROM DELIVERY WHERE CHANNELID=? AND MESSAGEID=? AND STATE='C'";
      
            ps = conn.prepareStatement(sql);
            ps.setString(1, (String)channelID);
            ps.setString(2, (String)d.getReference().getMessageID());
            
            updated = ps.executeUpdate();
         }
         else
         {
            //Transacted case
            sql = "UPDATE DELIVERY SET TRANSACTIONID=?, STATE='-' WHERE CHANNELID=? AND MESSAGEID=? AND STATE='C'";
            ps = conn.prepareStatement(sql);
            ps.setString(1, String.valueOf(tx.getID()));
            ps.setString(2, (String)d.getReference().getStoreID());
            ps.setString(3, (String)d.getReference().getMessageID());
            
            updated = ps.executeUpdate();            
         }
         
         if (log.isTraceEnabled())
         {
            log.trace(sql);
            log.trace("Removed/updated " + updated + " rows");
         }
                  
         conn.commit();        
   
         return updated == 1;
      }
      catch (SQLException e)
      {
         try
         {
            conn.rollback();
         }
         catch (Throwable t)
         {}
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
      }
   }


   /*
    * Message references can be added transactionally
    */
   public void add(Serializable channelID, MessageReference ref, Transaction tx) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Adding message ref " + ref + " to channel: "  + channelID);}

      Connection conn = null;
      PreparedStatement ps = null;      

      try
      {
         conn = ds.getConnection();
                  
         insertTx(conn, tx);
         
         String sql =
               "INSERT INTO MESSAGE_REFERENCE (CHANNELID, MESSAGEID, STOREID, TRANSACTIONID, STATE) VALUES (?, ?, ?, ?, ?)";
         ps = conn.prepareStatement(sql);
         ps.setString(1, (String)channelID);
         ps.setString(2, (String)ref.getMessageID());
         ps.setString(3, (String)ref.getStoreID());
         if (tx == null)
         {
            if (log.isTraceEnabled()) { log.trace("No tx so adding with state C"); }
            ps.setNull(4, java.sql.Types.VARCHAR);
            ps.setString(5, "C");
         }
         else
         {
            { log.trace("tx so adding with state +"); }
            ps.setString(4, String.valueOf(tx.getID()));
            ps.setString(5, "+");
         }
         ps.executeUpdate();
         if (log.isTraceEnabled()) { log.trace(sql); }
         conn.commit();
         
      }
      catch (SQLException e)
      {
         try
         {
            conn.rollback();
         }
         catch (Throwable t)
         {}
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
      }
   }
   
   /*
    * Message reference removal never happens in a jms transaction
    */
   public boolean remove(Serializable channelID, MessageReference ref) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Removing message ref " + ref + " from channel " + channelID); }
      
      Connection conn = null;
      PreparedStatement ps = null;  

      try
      {
         conn = ds.getConnection();
         
         String sql = "DELETE FROM MESSAGE_REFERENCE WHERE CHANNELID=? AND MESSAGEID=? AND STATE='C'";
         ps = conn.prepareStatement(sql);
         ps.setString(1, (String)channelID);
         ps.setString(2, (String)ref.getMessageID());
         int rows = ps.executeUpdate();
         if (log.isTraceEnabled()) { log.trace(sql); }
         conn.commit();
         return rows == 1;         
      }
      catch (SQLException e)
      {
         try
         {
            conn.rollback();
         }
         catch (Throwable t)
         {}
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
      }
   }
   
   /*
    * The jms transaction is committing.
    * We needs to remove any deliveries marked as "D".
    */
   public void commitTx(Transaction tx) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Committing transaction:" + tx.getID()); }
      
      Connection conn = null;
      PreparedStatement ps = null;

      try
      {
         conn = ds.getConnection();
         
      
         String sql = "DELETE FROM DELIVERY WHERE STATE='-' AND TRANSACTIONID=?";
   
         ps = conn.prepareStatement(sql);
         ps.setString(1, String.valueOf(tx.getID()));
         
         int rows = ps.executeUpdate();
         
         if (log.isTraceEnabled()) { log.trace("Removed " + rows + " rows from DELIVERY"); }
         
         sql = "UPDATE MESSAGE_REFERENCE SET STATE='C', TRANSACTIONID=? WHERE STATE='+' AND TRANSACTIONID=?";
         ps.close();
         ps = conn.prepareStatement(sql);
         ps.setNull(1, Types.VARCHAR);
         ps.setString(2, String.valueOf(tx.getID()));
         
         rows = ps.executeUpdate();
         
         removeTXRecord(conn, tx);
         
         if (log.isTraceEnabled()) { log.trace("Updated " + rows + " rows in MESSAGE_REFERENCES"); }
         conn.commit();
                 
      }
      catch (SQLException e)
      {
         try
         {
            conn.rollback();
         }
         catch (Throwable t)
         {}
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

      try
      {
         conn = ds.getConnection();
         
      
         String sql = "DELETE FROM MESSAGE_REFERENCE WHERE TRANSACTIONID=? AND STATE='+'";
   
         ps = conn.prepareStatement(sql);
         ps.setString(1, String.valueOf(tx.getID()));
         
         int rows = ps.executeUpdate();
         
         if (log.isTraceEnabled()) { log.trace("Removed " + rows + " rows from MESSAGE_REFERENCE"); }
         
         sql = "UPDATE DELIVERY SET STATE='C', TRANSACTIONID=? WHERE STATE='-' AND TRANSACTIONID=?";
         ps.close();
         
         ps = conn.prepareStatement(sql);
         ps.setNull(1, Types.VARCHAR);
         ps.setString(2, String.valueOf(tx.getID()));
         
         rows = ps.executeUpdate();
         
         if (log.isTraceEnabled()) { log.trace("Updated " + rows + " rows from DELIVERY"); }
                  
         removeTXRecord(conn, tx);
         conn.commit();
                 
      }
      catch (SQLException e)
      {
         try
         {
            conn.rollback();
         }
         catch (Throwable t)
         {}
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
      }
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
         ps = conn.prepareStatement("INSERT INTO TRANSACTION (TRANSACTIONID) values(?)");
         ps.setString(1, String.valueOf(tx.getID()));
         ps.executeUpdate();
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
         ps = conn.prepareStatement("DELETE FROM TRANSACTION WHERE TRANSACTIONID = ?");
         ps.setString(1, String.valueOf(tx.getID()));
         ps.executeUpdate();
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

   public List deliveries(Serializable channelID) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("listing deliveries for channel: " + channelID); }

      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;

      try
      {
         conn = ds.getConnection();
               
         List result = new ArrayList();
      
         String sql = "SELECT MESSAGEID, STOREID FROM DELIVERY WHERE CHANNELID=? AND STATE='C'";
   
         ps = conn.prepareStatement(sql);
         ps.setString(1, (String)channelID);
         
         rs = ps.executeQuery();
         if (log.isTraceEnabled()) { log.trace(sql); }
   
         int count = 0;
         while (rs.next())
         {
            String id = rs.getString(1);
            String storeID = rs.getString(2);
            result.add(new StorageIdentifier(id, storeID));
            count++;
         }
         
         if (log.isTraceEnabled()) { log.trace("There are " + count + " deliveries"); }
   
         conn.commit();
         
         return result;
      }
      catch (SQLException e)
      {
         try
         {
            conn.rollback();
         }
         catch (Throwable t)
         {}
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
      }
   }

   public void removeAllMessageData(Serializable channelID) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Removing all message data for channel: " + channelID); }

      Connection conn = null;
      PreparedStatement ps1 = null;
      PreparedStatement ps2 = null;

      try
      {
         conn = ds.getConnection();         
   
         final String sql1 = "DELETE FROM DELIVERY WHERE CHANNELID=?";
         
         final String sql2 = "DELETE FROM MESSAGE_REFERENCE WHERE CHANNELID=?";
   
         ps1 = conn.prepareStatement(sql1);
         ps2 = conn.prepareStatement(sql2);
         
         ps1.setString(1, (String)channelID);
         ps2.setString(1, (String)channelID);
         
         ps1.executeUpdate();
         ps2.executeUpdate();
         
         conn.commit();
      }
      catch (SQLException e)
      {
         try
         {
            conn.rollback();
         }
         catch (Throwable t)
         {}
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
      }
    
   }
   
   

   public List messages(Serializable channelID) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Getting message references for channel " + channelID); }
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      
      try
      {
         List result = new ArrayList();
         conn = ds.getConnection();         
   
         String sql = "SELECT MESSAGEID, STOREID FROM MESSAGE_REFERENCE WHERE CHANNELID=? AND STATE='C'";
         
         ps = conn.prepareStatement(sql);
         ps.setString(1, (String)channelID);
         
         rs = ps.executeQuery();
         
         if (log.isTraceEnabled()) { log.trace(sql); }
         
         while (rs.next())
         {
            String id = rs.getString(1);
            String storeID = rs.getString(2);
            result.add(new StorageIdentifier(id, storeID));
         }
         
         if (log.isTraceEnabled()) { log.trace("got " + result.size() + " message refs"); }
         
         conn.commit();
         
         return result;
      }
      catch (SQLException e)
      {
         try
         {
            conn.rollback();
         }
         catch (Throwable t)
         {}
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
      }
   }
   
   public void remove(String messageID) throws Exception
   {
      Connection conn = null;
      PreparedStatement stat = null;

      try
      {
         conn = ds.getConnection();
         
   
         String sql = "DELETE FROM MESSAGE WHERE MESSAGEID=?";
   
         stat = conn.prepareStatement(sql);
         stat.setString(1, messageID);
         
         stat.executeUpdate();
         
         conn.commit();
      }
      catch (SQLException e)
      {
         try
         {
            conn.rollback();
         }
         catch (Throwable t)
         {}
      }
      finally
      {
         if (stat != null)
         {
            try
            {
               stat.close();
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

     }

   }

   public void store(Message m) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps = null;

      try
      {
         conn = ds.getConnection();         
   
         String sql = "INSERT INTO MESSAGE VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";
   
         ps = conn.prepareStatement(sql);
         ps.setString(1, (String)m.getMessageID());
         ps.setBoolean(2, m.isReliable());
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
         ps.setBoolean(11, destIsQueue);
         ps.setString(12, dest);
         ps.setBoolean(13, replyToIsQueue);
         ps.setString(14, replyTo);
         ps.setObject(15, jmsProperties);
         ps.executeUpdate();
         if (log.isTraceEnabled()) { log.trace(ps); }
         
         conn.commit();
         
      }
      catch (SQLException e)
      {
         try
         {
            conn.rollback();
         }
         catch (Throwable t)
         {}
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

      }
      
   }


   public Message retrieve(Serializable messageID) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try
      {
         Message m = null;
         conn = ds.getConnection();
         
   
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
                      "FROM MESSAGE WHERE MESSAGEID = ?";
         
         ps = conn.prepareStatement(sql);
         ps.setString(1, (String)messageID);
         
         rs = ps.executeQuery();
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
         
         conn.commit();
         
         return m;
      }
      catch (SQLException e)
      {
         try
         {
            conn.rollback();
         }
         catch (Throwable t)
         {}
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
      }
    
   }

   // Public --------------------------------------------------------

   public void start() throws Exception
   {
 
      InitialContext ic = new InitialContext();      

      ds = new BasicDataSource();
      ic.close();

      Connection conn = null;
      
      String sql = null;

      try
      {
         conn = ds.getConnection();
         
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating TRANSACTION table"); }
            sql = "CREATE TABLE TRANSACTION ( TRANSACTIONID INTEGER, PRIMARY KEY (TRANSACTIONID) )";
            conn.createStatement().executeUpdate(sql);
         }
         catch (SQLException e) {
         }
                  
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating DELIVERY table"); }
            sql = "CREATE TABLE DELIVERY (CHANNELID VARCHAR, MESSAGEID VARCHAR, STOREID VARCHAR, TRANSACTIONID VARCHAR, STATE CHAR, PRIMARY KEY(CHANNELID, MESSAGEID))";
            conn.createStatement().executeUpdate(sql);
         }
         catch (SQLException e) {}
         
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating MESSAGE_REFERENCE table"); }
            sql = "CREATE TABLE MESSAGE_REFERENCE (CHANNELID VARCHAR, MESSAGEID VARCHAR, STOREID VARCHAR, TRANSACTIONID VARCHAR, STATE CHAR, PRIMARY KEY(CHANNELID, MESSAGEID))";
            conn.createStatement().executeUpdate(sql);
         }
         catch (SQLException e) {}

         if (log.isTraceEnabled()) { log.trace("Creating MESSAGE table"); }
         sql = "CREATE TABLE MESSAGE (" +
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
            "JMSPROPERTIES OBJECT, " +
            "PRIMARY KEY (MESSAGEID))";
         try
         {
            conn.createStatement().executeUpdate(sql);         
         }
         catch (SQLException e) {}
         
         //TODO INDEXES
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
      }
   }

   public void stop() throws Exception
   {
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
  
   protected String dbURL;
   
   /*
    * TODO
    * FIXME
    * I have implemented the BasicDataSource class as a temporary measure since I cannot
    * work out how to use a JBoss pooled data source class without using JTA and without
    * having autocommit=true
    * We want a really simple non JCA data-source with auto-commit = false.
    */
   class BasicDataSource implements javax.sql.DataSource
   {
      
      BasicDataSource()
      {
         try
         {
            Class.forName("org.hsqldb.jdbcDriver");
         }
         catch (Exception e)
         {
            log.error("Failed to load driver", e);
         }

      }

      public Connection getConnection() throws SQLException
      {
         Connection conn = DriverManager.getConnection(dbURL, "sa", "");
         conn.setAutoCommit(false);
         return conn;
      }

      public Connection getConnection(String username, String password) throws SQLException
      {
         return getConnection();
      }

      public int getLoginTimeout() throws SQLException
      {
         // FIXME getLoginTimeout
         return 0;
      }

      public PrintWriter getLogWriter() throws SQLException
      {
         // FIXME getLogWriter
         return null;
      }

      public void setLoginTimeout(int seconds) throws SQLException
      {
         // FIXME setLoginTimeout
         
      }

      public void setLogWriter(PrintWriter out) throws SQLException
      {
         // FIXME setLogWriter
         
      }
      
   }
   
}
