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

import java.io.Serializable;
import java.sql.Connection;
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
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.jboss.jms.message.JBossMessage;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.message.MessageSupport;
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

   protected TransactionManager mgr;

   // Constructors --------------------------------------------------

   public HSQLDBPersistenceManager() throws Exception
   {

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
      TransactionWrapper wrap = new TransactionWrapper();

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

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, channelID, ref.getMessageID(), ref.getStoreID(), null, "C") + " inserted " + rows + " row(s)"); }
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
   public boolean remove(Serializable channelID, Delivery d, Transaction tx) throws Exception
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
         String sql;
         
         if (tx == null)
         {
            //Non transacted case
            sql = "DELETE FROM DELIVERY WHERE CHANNELID=? AND MESSAGEID=? AND STATE='C'";
      
            ps = conn.prepareStatement(sql);
            ps.setString(1, (String)channelID);
            ps.setString(2, (String)d.getReference().getMessageID());
            
            updated = ps.executeUpdate();

            if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, channelID, d.getReference().getMessageID()) + " removed/updated " + updated + " row(s)"); }
         }
         else
         {
            //Transacted case
            sql = "UPDATE DELIVERY SET TRANSACTIONID=?, STATE='-' WHERE CHANNELID=? AND MESSAGEID=? AND STATE='C'";
            ps = conn.prepareStatement(sql);
            ps.setString(1, String.valueOf(tx.getID()));
            ps.setString(2, (String)channelID);
            ps.setString(3, (String)d.getReference().getMessageID());

            updated = ps.executeUpdate();

            if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, String.valueOf(tx.getID()), channelID, d.getReference().getMessageID()) + " updated " + updated + " row(s)"); }
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
   public void add(Serializable channelID, MessageReference ref, Transaction tx) throws Exception
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

         String sql =
               "INSERT INTO MESSAGE_REFERENCE (CHANNELID, MESSAGEID, STOREID, TRANSACTIONID, STATE) VALUES (?, ?, ?, ?, ?)";
         if (log.isTraceEnabled())
         {
            log.trace(sql);
            log.trace("CHANNELID=" + channelID);
            log.trace("MESSAGEID=" + ref.getMessageID());
            log.trace("STOREID=" + ref.getStoreID());
            
         }
         ps = conn.prepareStatement(sql);
         ps.setString(1, (String)channelID);
         ps.setString(2, (String)ref.getMessageID());
         ps.setString(3, (String)ref.getStoreID());
         if (txID == null)
         {
            ps.setNull(4, java.sql.Types.VARCHAR);
            if (log.isTraceEnabled()) log.trace("TRANSACTIONID=NULL");
         }
         else
         {
            ps.setString(4, (String)txID);
            if (log.isTraceEnabled()) log.trace("TRANSACTIONID=" + txID);
         }
         ps.setString(5, (String)state);
         if (log.isTraceEnabled()) log.trace(("STATE=" + state));

         int inserted = ps.executeUpdate();
         
         if (log.isTraceEnabled()) log.trace("inserted " + inserted + " rows");

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, channelID, ref.getMessageID(), ref.getStoreID(), txID, state) + " inserted " + inserted + " row(s)"); }
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
   public boolean remove(Serializable channelID, MessageReference ref) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Removing " + ref + " from channel " + channelID); }
      
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();
         
         String sql = "DELETE FROM MESSAGE_REFERENCE WHERE CHANNELID=? AND MESSAGEID=? AND STATE='C'";
         
         if (log.isTraceEnabled())
         {
            log.trace(sql);
            log.trace("CHANNELID=" + channelID);
            log.trace("MESSAGEID=" + ref.getMessageID());            
         }
         
         ps = conn.prepareStatement(sql);
         ps.setString(1, (String)channelID);
         ps.setString(2, (String)ref.getMessageID());
         int rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, channelID, ref.getMessageID()) + " deleted " + rows + " row(s)"); }

         if (log.isTraceEnabled()) { log.trace("Deleted " + rows + " rows"); }
         
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

         String sql = "DELETE FROM DELIVERY WHERE STATE='-' AND TRANSACTIONID=?";
   
         ps = conn.prepareStatement(sql);
         ps.setString(1, String.valueOf(tx.getID()));
         
         int rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, String.valueOf(tx.getID())) + " removed " + rows + " row(s)"); }
         
         sql = "UPDATE MESSAGE_REFERENCE SET STATE='C', TRANSACTIONID=? WHERE STATE='+' AND TRANSACTIONID=?";
         ps.close();
         ps = conn.prepareStatement(sql);
         ps.setNull(1, Types.VARCHAR);
         ps.setString(2, String.valueOf(tx.getID()));
         
         rows = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, null, String.valueOf(tx.getID())) + " updated " + rows + " row(s)"); }

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

         String sql = "DELETE FROM MESSAGE_REFERENCE WHERE TRANSACTIONID=? AND STATE='+'";
   
         ps = conn.prepareStatement(sql);
         ps.setString(1, String.valueOf(tx.getID()));
         
         int rows = ps.executeUpdate();
         
         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, String.valueOf(tx.getID())) + " removed " + rows + " row(s)"); }
         
         sql = "UPDATE DELIVERY SET STATE='C', TRANSACTIONID=? WHERE STATE='-' AND TRANSACTIONID=?";
         ps.close();
         
         ps = conn.prepareStatement(sql);
         ps.setNull(1, Types.VARCHAR);
         ps.setString(2, String.valueOf(tx.getID()));
         
         rows = ps.executeUpdate();
         
         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, null, String.valueOf(tx.getID())) + " updated " + rows + " row(s)"); }
                  
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
         String sql = "INSERT INTO TRANSACTION (TRANSACTIONID) values(?)";
         ps = conn.prepareStatement(sql);
         ps.setString(1, String.valueOf(tx.getID()));

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
         String sql = "SELECT MESSAGEID FROM DELIVERY " +
                      "WHERE CHANNELID=? AND STOREID=? AND STATE='C'";
   
         ps = conn.prepareStatement(sql);
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

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, channelID) + " selected " + count + " row(s)"); }
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
   
         final String sql1 = "DELETE FROM DELIVERY WHERE CHANNELID=?";
         
         final String sql2 = "DELETE FROM MESSAGE_REFERENCE WHERE CHANNELID=?";
   
         ps1 = conn.prepareStatement(sql1);
         ps2 = conn.prepareStatement(sql2);
         
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
   
   

   public List messages(Serializable storeID, Serializable channelID) throws Exception
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
   
         String sql = "SELECT MESSAGEID FROM MESSAGE_REFERENCE WHERE CHANNELID=? AND STOREID=? AND STATE='C'";
         
         ps = conn.prepareStatement(sql);
         ps.setString(1, (String)channelID);
         ps.setString(2, (String)storeID);
         
         rs = ps.executeQuery();

         while (rs.next())
         {
            String id = rs.getString(1);            
            result.add(id);
         }
         
         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, channelID) + " selected " + result.size() + " row(s)"); }

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
   
   public void remove(String messageID) throws Exception
   {
      Connection conn = null;
      PreparedStatement stat = null;
      TransactionWrapper wrap = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();

         String sql = "DELETE FROM MESSAGE WHERE MESSAGEID=?";
   
         stat = conn.prepareStatement(sql);
         stat.setString(1, messageID);
         
         stat.executeUpdate();
      }
      catch (SQLException e)
      {
         wrap.exceptionOccurred();
         throw e;
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
         wrap.end();
     }

   }

   public void store(Message m) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps = null;
      TransactionWrapper wrap = new TransactionWrapper();

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
         int result = ps.executeUpdate();

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql) + " inserted " + result + " row(s)"); }
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


   public Message retrieve(Serializable messageID) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();

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

         int count = 0;
         if (rs.next())
         {
            m = (Message)rs.getObject("PAYLOAD");
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

         if (log.isTraceEnabled()) { log.trace(JDBCUtil.statementToString(sql, messageID) + " selected " + count + " row(s)"); }

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
   

   // Public --------------------------------------------------------

   public void start() throws Exception
   {
      InitialContext ic = new InitialContext();
      mgr = (TransactionManager)ic.lookup("java:/TransactionManager");

      ds = (DataSource)ic.lookup("java:/DefaultDS");
      ic.close();

      Connection conn = null;
      String sql = null;
      TransactionWrapper tx = new TransactionWrapper();

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
         tx.end();
      }
   }

   public void stop() throws Exception
   {
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
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
