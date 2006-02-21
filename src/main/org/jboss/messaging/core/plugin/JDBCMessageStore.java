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
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.TransactionManager;
import javax.management.ObjectName;
import javax.management.MBeanServerInvocationHandler;
import javax.naming.InitialContext;

import org.jboss.jms.message.JBossMessage;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.persistence.JDBCUtil;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.message.MessageSupport;
import org.jboss.messaging.util.Util;
import org.jboss.tm.TransactionManagerServiceMBean;

/**
 * Important! The default DML and DDL is only designed to work with HSQLDB.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JDBCMessageStore extends PersistentMessageStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JDBCMessageStore.class);
   
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

   protected String dataSourceJNDIName;
   protected DataSource ds;
   protected ObjectName tmObjectName;
   protected TransactionManager tm;

   protected Properties sqlProperties;
   protected boolean createTablesOnStartup;
   
   private boolean trace = log.isTraceEnabled();

   // Constructors --------------------------------------------------

   public JDBCMessageStore(String storeID) throws Exception
   {
      this(storeID, null, null);
   }

   /**
    * Only used for testing. In a real deployment, the data source and the transaction manager are
    * injected as dependencies.
    */
   public JDBCMessageStore(String storeID, DataSource ds, TransactionManager tm) throws Exception
   {
      super(storeID);
      this.ds = ds;
      this.tm = tm;
      sqlProperties = new Properties();
   }

   // ServiceMBeanSupport overrides ---------------------------------

   // TODO abstract this out, the same thing is used by the TransactionLog
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

      log.debug(this + " started");
   }

   protected void stopService() throws Exception
   {
      log.debug(this + " stopped");
   }

   // PersistentMessageStore overrides ------------------------------

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

   // TODO abstract this out, the same thing is used by the TransactionLog
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

   // TODO abstract this out, the same thing is used by the TransactionLog
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

   // TODO abstract this out, the same thing is used by the TransactionLog
   public void setSqlProperties(Properties props)
   {
      sqlProperties = new Properties(props);
   }

   public String toString()
   {
      return "JDBCMessageStore[" + Util.guidToString(getStoreID()) + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // TODO abstract this out, the same thing is used by the TransactionLog
   protected void createSchema() throws Exception
   {
      Connection conn = null;
      TransactionWrapper tx = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();
         
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

   // TODO abstract this out, the same thing is used by the TransactionLog
   protected void initSqlProperties()
   {
      deleteMessage = sqlProperties.getProperty("DELETE_MESSAGE", deleteMessage);
      insertMessage = sqlProperties.getProperty("INSERT_MESSAGE", insertMessage);
      selectMessage = sqlProperties.getProperty("SELECT_MESSAGE", selectMessage);            
      createMessage = sqlProperties.getProperty("CREATE_MESSAGE", createMessage);
      selectReferenceCount = sqlProperties.getProperty("SELECT_REF_COUNT", selectReferenceCount);
      updateReferenceCount = sqlProperties.getProperty("UPDATE_REF_COUNT", updateReferenceCount);      
      createTablesOnStartup = sqlProperties.getProperty("CREATE_TABLES_ON_STARTUP", "true").equalsIgnoreCase("true");
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   // TODO abstract this out, the same thing is used by the TransactionLog
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
