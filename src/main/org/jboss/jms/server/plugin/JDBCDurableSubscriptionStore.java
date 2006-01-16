/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.plugin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.jms.JMSException;
import javax.management.ObjectName;
import javax.management.MBeanServerInvocationHandler;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.local.DurableSubscription;
import org.jboss.messaging.core.persistence.JDBCUtil;
import org.jboss.tm.TransactionManagerServiceMBean;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ivelin@jboss.org">Ivelin Ivanov</a>
 *
 * $Id$
 */
public class JDBCDurableSubscriptionStore extends DurableSubscriptionStoreSupport
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(JDBCDurableSubscriptionStore.class);

   private String createUserTable =
      "CREATE TABLE JMS_USER (USERID VARCHAR(32) NOT NULL, PASSWD VARCHAR(32) NOT NULL, CLIENTID VARCHAR(128),"
         + " PRIMARY KEY(USERID))";

   private String createRoleTable = "CREATE TABLE JMS_ROLE (ROLEID VARCHAR(32) NOT NULL, USERID VARCHAR(32) NOT NULL,"
         + " PRIMARY KEY(USERID, ROLEID))";

   private String createSubscriptionTable = "CREATE TABLE JMS_SUBSCRIPTION (CLIENTID VARCHAR(128) NOT NULL, NAME VARCHAR(128) NOT NULL,"
         + " TOPIC VARCHAR(255) NOT NULL, SELECTOR VARCHAR(255)," + " PRIMARY KEY(CLIENTID, NAME))";
   
   private String selectSubscription = 
      "SELECT NAME, TOPIC, SELECTOR FROM JMS_SUBSCRIPTION WHERE CLIENTID=? AND NAME=?";
   
   private String insertSubscription = 
      "INSERT INTO JMS_SUBSCRIPTION (CLIENTID, NAME, TOPIC, SELECTOR) VALUES (?, ?, ?, ?)";
   
   private String deleteSubscription = 
      "DELETE FROM JMS_SUBSCRIPTION WHERE CLIENTID=? AND NAME=?";
   
   private String selectPreConfClientId = 
      "SELECT CLIENTID FROM JMS_USER WHERE USERID=?";
   
   private String selectSubscriptionsForTopic = 
      "SELECT CLIENTID, NAME, SELECTOR FROM JMS_SUBSCRIPTION WHERE TOPIC=?";
   

      
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
      
   protected String dataSourceJNDIName;
   protected DataSource ds;
   protected ObjectName tmObjectName;
   protected TransactionManager tm;

   protected boolean createTablesOnStartup;
   protected Properties sqlProperties;
   protected List populateTables;

   // Constructors --------------------------------------------------
  
   public JDBCDurableSubscriptionStore()
   {
      super();
      
      sqlProperties = new Properties();
      populateTables = new ArrayList();
   }

   // ServiceMBeanSupport overrides ---------------------------------

   protected void startService() throws Exception
   {
      // these are supposed to be set by dependencies, this is just a safeguard
      if (ds == null)
      {
         throw new Exception("No DataSource found. This service dependencies must  " +
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

   // DurableSubscriptionStoreDelegate implementation ---------------

   public Object getInstance()
   {
      return this;
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

            ps = conn.prepareStatement(selectSubscription);

            ps.setString(1, clientID);
            ps.setString(2, subscriptionName);

            boolean exists = false;

            try
            {
               rs = ps.executeQuery();
               exists = rs.next();
            }
            finally
            {
               if (log.isTraceEnabled())
               {
                  String s = JDBCUtil.statementToString(selectSubscription, clientID, subscriptionName);
                  log.trace(s + (rs == null ? " failed!" : (exists ? " returned rows" : " did NOT return rows")));
               }
            }

            if (exists)
            {
               String name = rs.getString(1);
               String topicName = rs.getString(2);
               String selector = rs.getString(3);

               // create in memory
               sub = super.createDurableSubscription(topicName, clientID, name, selector);

               // load its state
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

   public DurableSubscription createDurableSubscription(String topicName, String clientID,
                                                        String subscriptionName, String selector)
         throws JMSException
   {
      try
      {

         Connection conn = null;
         PreparedStatement ps  = null;
         TransactionWrapper wrap = new TransactionWrapper();
         boolean failed = false;

         try
         {
            conn = ds.getConnection();

            ps = conn.prepareStatement(insertSubscription);

            ps.setString(1, clientID);
            ps.setString(2, subscriptionName);
            ps.setString(3, topicName);
            ps.setString(4, selector);

            ps.executeUpdate();

            // create it in memory too
            DurableSubscription sub =
               super.createDurableSubscription(topicName, clientID, subscriptionName, selector);

            return sub;
         }
         catch (SQLException e)
         {
            failed = true;
            wrap.exceptionOccurred();
            throw e;
         }
         finally
         {
            if (log.isTraceEnabled())
            {
               String s = JDBCUtil.statementToString(insertSubscription, clientID, subscriptionName, topicName, selector);
               log.trace(s + (failed ? " failed!" : " executed successfully"));
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

            ps = conn.prepareStatement(deleteSubscription);

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
         final String msg = "Failed to remove subscription";
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

            ps = conn.prepareStatement(selectPreConfClientId);

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

            ps = conn.prepareStatement(selectSubscriptionsForTopic);

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

   // MBean operations ----------------------------------------------

//   public ObjectName getConnectionManager()
//   {
//      return connectionManagerName;
//   }
//
//   public void setConnectionManager(ObjectName connectionManagerName)
//   {
//      this.connectionManagerName = connectionManagerName;
//   }
//
//
//   public String getSqlProperties()
//   {
//      try
//      {
//         ByteArrayOutputStream boa = new ByteArrayOutputStream();
//         sqlProperties.store(boa, "");
//         return new String(boa.toByteArray());
//      }
//      catch (IOException shouldnothappen)
//      {
//         return "";
//      }
//   }
//
//   public void setSqlProperties(String value)
//   {
//      try
//      {
//
//         ByteArrayInputStream is = new ByteArrayInputStream(value.getBytes());
//         sqlProperties = new Properties();
//         sqlProperties.load(is);
//
//      }
//      catch (IOException shouldnothappen)
//      {
//         log.error("Caught IOException", shouldnothappen);
//      }
//   }
   
   // Public --------------------------------------------------------

   /**
    * Managed attribute.
    */
   public void setDataSource(String dataSourceJNDIName) throws Exception
   {
      this.dataSourceJNDIName = dataSourceJNDIName;

      InitialContext ic = new InitialContext();
      ds = (DataSource)ic.lookup(dataSourceJNDIName);
      ic.close();
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

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void initSqlProperties()
   {
      createUserTable = sqlProperties.getProperty("CREATE_USER_TABLE", createUserTable);
      createRoleTable = sqlProperties.getProperty("CREATE_ROLE_TABLE", createRoleTable);
      createSubscriptionTable = sqlProperties.getProperty("CREATE_SUBSCRIPTION_TABLE", createSubscriptionTable);
      selectSubscription = sqlProperties.getProperty("SELECT_SUBSCRIPTION", selectSubscription);
      insertSubscription = sqlProperties.getProperty("INSERT_SUBSCRIPTION", insertSubscription);
      deleteSubscription = sqlProperties.getProperty("DELETE_SUBSCRIPTION", deleteSubscription);
      selectPreConfClientId = sqlProperties.getProperty("SELECT_PRECONF_CLIENTID", selectPreConfClientId);
      selectSubscriptionsForTopic = sqlProperties.getProperty("SELECT_SUBSCRIPTIONS_FOR_TOPIC", selectSubscriptionsForTopic);
      createTablesOnStartup = sqlProperties.getProperty("CREATE_TABLES_ON_STARTUP", "true").equalsIgnoreCase("true");
      
      for (Iterator i = sqlProperties.entrySet().iterator(); i.hasNext();)
      {
         Map.Entry entry = (Map.Entry) i.next();
         String key = (String) entry.getKey();
         if (key.startsWith("POPULATE.TABLES."))
            populateTables.add(entry.getValue());
      }
      
   }
   
   protected void createSchema() throws Exception
   {      
      Connection conn = null;      
      TransactionWrapper tx = new TransactionWrapper();

      try
      {
         conn = ds.getConnection();
         
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating JMS_USERS table"); }            
            conn.createStatement().executeUpdate(createUserTable);
         }
         catch (SQLException e) 
         {
            log.debug("Failed to create users table: " + createUserTable, e);
         }
                  
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating JMS_ROLES table"); }
            conn.createStatement().executeUpdate(createRoleTable);
         }
         catch (SQLException e) 
         {
            log.debug("Failed to create roles table: " + createRoleTable, e);
         }
         
         try
         {
            if (log.isTraceEnabled()) { log.trace("Creating JMS_SUBSCRIPTIONS table"); }
            conn.createStatement().
                  executeUpdate(createSubscriptionTable);
         }
         catch (SQLException e) 
         {
            log.debug("Failed to create subscriptions table: " + createSubscriptionTable, e);
         }
         
         
         //Insert user-role data
         Iterator iter = populateTables.iterator();
         String nextQry = null;
         while (iter.hasNext())
         {
            Statement st = null;
            try
            {
               nextQry = (String) iter.next();               
               st = conn.createStatement();              
               st.executeUpdate(nextQry);
            }
            catch (SQLException ignored)
            {
               log.debug("Error populating tables: " + nextQry, ignored);
            }
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
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
   /*
    * TODO This inner class is duplicated from HSQLPersistenceManager - need to avoid code duplication
    */
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
