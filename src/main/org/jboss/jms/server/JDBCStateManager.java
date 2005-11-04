/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.jms.JMSException;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.local.DurableSubscription;
import org.jboss.tm.TransactionManagerService;

/**
 * StateManager that maintains state in a database
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Partially derived from org.jboss.mq.sm.jdbc.JDBCStateManager by:
 * @author Adrian Brock (Adrian@jboss.org)
 * @author Ivelin Ivanov (ivelin@jboss.org)
 *
 */
public class JDBCStateManager extends InMemoryStateManager implements JDBCStateManagerMBean
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(JDBCStateManager.class);
   
   private static final String CREATE_USER_TABLE =
      "CREATE TABLE JMS_USER (USERID VARCHAR(32) NOT NULL, PASSWD VARCHAR(32) NOT NULL, CLIENTID VARCHAR(128),"
         + " PRIMARY KEY(USERID))";

   private static final String CREATE_ROLE_TABLE = "CREATE TABLE JMS_ROLE (ROLEID VARCHAR(32) NOT NULL, USERID VARCHAR(32) NOT NULL,"
         + " PRIMARY KEY(USERID, ROLEID))";

   private static final String CREATE_SUBSCRIPTION_TABLE = "CREATE TABLE JMS_SUBSCRIPTION (CLIENTID VARCHAR(128) NOT NULL, NAME VARCHAR(128) NOT NULL,"
         + " TOPIC VARCHAR(255) NOT NULL, SELECTOR VARCHAR(255)," + " PRIMARY KEY(CLIENTID, NAME))";
   
   private static final String GET_SUBSCRIPTION = 
      "SELECT NAME, TOPIC, SELECTOR FROM JMS_SUBSCRIPTION WHERE CLIENTID=? AND NAME=?";
   
   private static final String INSERT_SUBSCRIPTION = 
      "INSERT INTO JMS_SUBSCRIPTION (CLIENTID, NAME, TOPIC, SELECTOR) VALUES (?, ?, ?, ?)";
   
   private static final String DELETE_SUBSCRIPTION = 
      "DELETE FROM JMS_SUBSCRIPTION WHERE CLIENTID=? AND NAME=?";
   
   private static final String GET_PRECONFCLIENTID = 
      "SELECT CLIENTID FROM JMS_USER WHERE USERID=?";
   
   private static final String GET_SUBSCRIPTIONS = 
      "SELECT CLIENTID, NAME, SELECTOR FROM JMS_SUBSCRIPTION WHERE TOPIC=?";
   
      
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
      
   protected DataSource ds;

   protected TransactionManager mgr;
   
   protected Properties sqlProperties;

   protected ObjectName connectionManagerName;

   
   
   // Constructors --------------------------------------------------
  
   public JDBCStateManager(ServerPeer serverPeer)
   {
      super(serverPeer);
      
      sqlProperties = new Properties();
   
   }
   
   // MBean operations ----------------------------------------------
   
   
   public ObjectName getConnectionManager()
   {
      return connectionManagerName;
   }

   public void setConnectionManager(ObjectName connectionManagerName)
   {
      this.connectionManagerName = connectionManagerName;
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
         log.error("Caught IOException", shouldnothappen);
      }
   }
   


   // Public --------------------------------------------------------
   
   public void start() throws Exception
   {

      InitialContext ctx = new InitialContext();
      try
      {
         ds = (DataSource) ctx.lookup("java:/DefaultDS");
         mgr = (TransactionManager) ctx.lookup(TransactionManagerService.JNDI_NAME);
      }
      finally
      {
         ctx.close();
      }
      initDB();
   }
   
   
   

   public DurableSubscription getDurableSubscription(String clientID, String subscriptionName)
      throws JMSException
   {
      //Look in memory first
      DurableSubscription sub = super.getDurableSubscription(clientID, subscriptionName);
      
      if (sub != null)
      {
         return sub;
      }
      
      try
      {
      
         Connection conn = null;
         PreparedStatement ps  = null;
         ResultSet rs = null;
         TransactionWrapper wrap = new TransactionWrapper();
   
         try
         {
            conn = ds.getConnection();
            
            ps = conn.prepareStatement(sqlProperties.getProperty("GET_SUBSCRIPTION", GET_SUBSCRIPTION));
            
            ps.setString(1, clientID);
            
            ps.setString(2, subscriptionName);
            
            rs = ps.executeQuery();
            
            if (rs.next())
            {
               String name = rs.getString(1);
               String topicName = rs.getString(2);
               String selector = rs.getString(3);
               
               //Create in memory
               sub = super.createDurableSubscription(topicName, clientID, name, selector);
               
               //load it's state
               sub.load();
            }
            
            return sub;
         }
         finally
         {
            if (rs != null)
            {
               rs.close();
            }
            if (ps != null)
            {
               ps.close();
            }
            if (conn != null)
            {
               conn.close();
            }
            wrap.end();
         }
      }
      catch (Exception e)
      {
         final String msg = "Failed to get subscription";
         log.error(msg, e);
         JMSException e2 = new JMSException(msg);
         e2.setLinkedException(e);
         throw e2;
      }
              
   }
   
   public DurableSubscription createDurableSubscription(String topicName, String clientID, String subscriptionName,
         String selector) throws JMSException
   {
      try
      {
      
         Connection conn = null;
         PreparedStatement ps  = null;         
         TransactionWrapper wrap = new TransactionWrapper();
   
         try
         {
            conn = ds.getConnection();
            
            ps = conn.prepareStatement(sqlProperties.getProperty("INSERT_SUBSCRIPTION", INSERT_SUBSCRIPTION));
            
            ps.setString(1, clientID);
            
            ps.setString(2, subscriptionName);
            
            ps.setString(3, topicName);
            
            ps.setString(4, selector);
            
            ps.executeUpdate();
            
            //Create in memory too
            DurableSubscription sub = super.createDurableSubscription(topicName, clientID, subscriptionName, selector);
            
            return sub;
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
               ps.close();
            }
            if (conn != null)
            {
               conn.close();
            }
            wrap.end();
         }
      }
      catch (Exception e)
      {
         final String msg = "Failed to insert subscription";
         log.error(msg, e);
         JMSException e2 = new JMSException(msg);
         e2.setLinkedException(e);
         throw e2;
      }
   }
   
   public boolean removeDurableSubscription(String clientID, String subscriptionName)
      throws JMSException
   {
      try
      {
      
         Connection conn = null;
         PreparedStatement ps  = null;         
         TransactionWrapper wrap = new TransactionWrapper();
   
         try
         {
            conn = ds.getConnection();
            
            ps = conn.prepareStatement(sqlProperties.getProperty("DELETE_SUBSCRIPTION", DELETE_SUBSCRIPTION));
            
            ps.setString(1, clientID);
            
            ps.setString(2, subscriptionName);                        
            
            int rows = ps.executeUpdate();
            
            if (rows == 1)
            {
               boolean removed = super.removeDurableSubscription(clientID, subscriptionName);
               return removed;
            }
            else
            {
               return false;
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
               ps.close();
            }
            if (conn != null)
            {
               conn.close();
            }
            wrap.end();
         }
      }
      catch (Exception e)
      {
         final String msg = "Failed to delete subscription";
         log.error(msg, e);
         JMSException e2 = new JMSException(msg);
         e2.setLinkedException(e);
         throw e2;
      }
   }
   
   public String getPreConfiguredClientID(String username) throws JMSException
   {
      try
      {      
         Connection conn = null;
         PreparedStatement ps  = null;     
         ResultSet rs = null;
         TransactionWrapper wrap = new TransactionWrapper();
   
         try
         {
            conn = ds.getConnection();
            
            ps = conn.prepareStatement(sqlProperties.getProperty("GET_PRECONFCLIENTID", GET_PRECONFCLIENTID));
            
            ps.setString(1, username);
            
            rs = ps.executeQuery();
            
            String clientID = null;
            
            if (rs.next())
            {
               clientID = rs.getString(1);
            }            
            
            return clientID;
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
               rs.close();
            }
            if (ps != null)
            {
               ps.close();
            }
            if (conn != null)
            {
               conn.close();
            }
            wrap.end();
         }
      }
      catch (Exception e)
      {
         final String msg = "Failed to get client id";
         log.error(msg, e);
         JMSException e2 = new JMSException(msg);
         e2.setLinkedException(e);
         throw e2;
      }
   }
   
   public Set loadDurableSubscriptionsForTopic(String topicName) throws JMSException
   {
      try
      {      
         Connection conn = null;
         PreparedStatement ps  = null;     
         ResultSet rs = null;
         TransactionWrapper wrap = new TransactionWrapper();
   
         try
         {
            conn = ds.getConnection();
            
            ps = conn.prepareStatement(sqlProperties.getProperty("GET_SUBSCRIPTIONS", GET_SUBSCRIPTIONS));
            
            ps.setString(1, topicName);
            
            rs = ps.executeQuery();
            
            Set subs = new HashSet();
            
            while (rs.next())
            {
               String clientID = rs.getString(1);
               String subName = rs.getString(2);
               String selector = rs.getString(3);
               
               DurableSubscription sub = super.getDurableSubscription(clientID, subName);
               if (sub == null)
               {
                  sub = super.createDurableSubscription(topicName, clientID, subName, selector);
               }
               subs.add(sub);
            }
            return subs;
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
               rs.close();
            }
            if (ps != null)
            {
               ps.close();
            }
            if (conn != null)
            {
               conn.close();
            }
            wrap.end();
         }
      }
      catch (Exception e)
      {
         final String msg = "Failed to get client id";
         log.error(msg, e);
         JMSException e2 = new JMSException(msg);
         e2.setLinkedException(e);
         throw e2;
      }
   }
   

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void initDB() throws Exception
   {      
//      if (!"true".equalsIgnoreCase(sqlProperties.getProperty("CREATE_TABLES_ON_STARTUP")))
//      {
//         return;
//      }
      
      Connection conn = null;      
      TransactionWrapper tx = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();
         
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating JMS_USERS table"); }            
            conn.createStatement().executeUpdate(sqlProperties.getProperty("CREATE_USER_TABLE", CREATE_USER_TABLE));
         }
         catch (SQLException e) 
         {}
                  
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating JMS_ROLES table"); }
            conn.createStatement().executeUpdate(sqlProperties.getProperty("CREATE_ROLE_TABLE", CREATE_ROLE_TABLE));
         }
         catch (SQLException e) 
         {}
         
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating JMS_SUBSCRIPTIONS table"); }
            conn.createStatement().executeUpdate(sqlProperties.getProperty("CREATE_SUBSCRIPTION_TABLE", CREATE_SUBSCRIPTION_TABLE));
         }
         catch (SQLException e) 
         {}
         
         //TODO indexes

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
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
   /*
    * TODO This inner class is duplicated from HSQLPersistenceManager - need to avoid
    * code duplication
    */
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
