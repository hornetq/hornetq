/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.plugin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.jms.JMSException;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.server.plugin.contract.ChannelMapper;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.core.local.CoreDurableSubscription;
import org.jboss.messaging.core.local.CoreSubscription;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.local.Topic;
import org.jboss.messaging.core.persistence.JDBCUtil;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.system.ServiceMBeanSupport;
import org.jboss.tm.TransactionManagerServiceMBean;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * JDBC Implementation of ChannelMapper
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ivelin@jboss.org">Ivelin Ivanov</a>
 *
 * JDBCChannelMapper.java,v 1.1 2006/03/01 09:15:09 timfox Exp
 */
public class JDBCChannelMapper extends ServiceMBeanSupport implements ChannelMapper
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(JDBCChannelMapper.class);
   
   protected static final char TYPE_QUEUE = 'Q';
   
   protected static final char TYPE_TOPIC = 'T';
   
   protected static final char TYPE_DURABLE_SUB = 'D';
   
   //========================================
   
   //FIXME - user-role table management should be handled by a different service
   //It doesn't belong here
   
   private String createUserTable =
      "CREATE TABLE JMS_USER (USERID VARCHAR(32) NOT NULL, PASSWD VARCHAR(32) NOT NULL, CLIENTID VARCHAR(128),"
      + " PRIMARY KEY(USERID))";
   
   private String createRoleTable = "CREATE TABLE JMS_ROLE (ROLEID VARCHAR(32) NOT NULL, USERID VARCHAR(32) NOT NULL,"
      + " PRIMARY KEY(USERID, ROLEID))";
   
   private String selectPreConfClientId = 
      "SELECT CLIENTID FROM JMS_USER WHERE USERID=?";
   
   
   // =============================================================================
   
   private String createMappingTable = 
      "CREATE TABLE CHANNEL_MAPPING (ID BIGINT, TYPE CHAR(1), " +
      "JMS_DEST_NAME VARCHAR(1024), JMS_SUB_NAME VARCHAR(1024), " +
      "CLIENT_ID VARCHAR(128), SELECTOR VARCHAR(1024), NO_LOCAL CHAR(1), PRIMARY KEY(ID))";
   
   private String insertMapping = 
      "INSERT INTO CHANNEL_MAPPING (ID, TYPE, JMS_DEST_NAME, JMS_SUB_NAME, CLIENT_ID, SELECTOR, NO_LOCAL) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?)";
   
   private String deleteMapping =
      "DELETE FROM CHANNEL_MAPPING WHERE ID = ?";
   
   private String selectIdForDestination = 
      "SELECT ID FROM CHANNEL_MAPPING WHERE TYPE=? AND JMS_DEST_NAME=?";
   
   private String selectDurableSub = 
      "SELECT JMS_DEST_NAME, ID, SELECTOR, NO_LOCAL FROM CHANNEL_MAPPING WHERE CLIENT_ID=? AND JMS_SUB_NAME=?";
   
   private String selectSubscriptionsForTopic = 
      "SELECT ID, CLIENT_ID, JMS_SUB_NAME, SELECTOR, NO_LOCAL FROM CHANNEL_MAPPING WHERE TYPE='D' AND JMS_DEST_NAME=?";
   
      
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Map<clientID - Map<subscriptionName - CoreDurableSubscription>>
   protected Map subscriptions;
   
   // Map<name - Queue>
   protected Map queues;
   
   // Map<name - Topic>
   protected Map topics;
   
   // Map<id - JBossDestination>
   protected Map idMap;
   
   protected String dataSourceJNDIName;
   protected DataSource ds;
   protected ObjectName tmObjectName;
   protected TransactionManager tm;
   
   protected IdManager channelIDManager;
   
   protected boolean createTablesOnStartup = true;
   protected Properties sqlProperties;
   protected List populateTables;
   
   // Constructors --------------------------------------------------
   
   public JDBCChannelMapper()
   {
      subscriptions = new ConcurrentReaderHashMap();
      
      queues = new ConcurrentReaderHashMap();
      
      topics = new ConcurrentReaderHashMap();
      
      idMap = new ConcurrentReaderHashMap();
      
      sqlProperties = new Properties();
      
      populateTables = new ArrayList();
   }
   
   /*
    * Only to be used by tests to create an instance
    */
   public JDBCChannelMapper(DataSource ds, TransactionManager tm)
   {
      this();
      
      this.ds = ds;
      
      this.tm = tm;
   }
   
   //Injection
   
   //TODO
   //Since channel mapper requires knowledge of the persistence manager anyway so it can get ids for new channels
   //there seems to me no reason why channel mapper should handle it's own persistence
   //IMHO all persistence should be handled by the pm
   public void setPersistenceManager(PersistenceManager pm) throws Exception
   {      
      this.channelIDManager = new IdManager("CHANNEL_ID", 10, pm);
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
         throw new IllegalStateException("No DataSource found. This service dependencies must " +
         "have not been enforced correctly!");
      }
      if (tm == null)
      {
         throw new IllegalStateException("No TransactionManager found. This service dependencies must " +
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
   
   // DurableSubscriptionStore implementation ---------------
   
   public Object getInstance()
   {
      return this;
   }
   
   public CoreDestination getCoreDestination(JBossDestination jbDest)
   {
      return getCoreDestinationInternal(jbDest.isQueue(), jbDest.getName());
   }
   
   public JBossDestination getJBossDestination(long coreDestinationId)
   {
      return (JBossDestination)idMap.get(new Long(coreDestinationId));
   }
    
   public void deployCoreDestination(boolean isQueue, 
                                     String destName,
                                     MessageStore ms, 
                                     PersistenceManager pm,
                                     int fullSize, 
                                     int pageSize, 
                                     int downCacheSize) throws JMSException
   {
      try
      {         
         if (log.isTraceEnabled()) { log.trace("creating core destination for " + destName); }
         
         CoreDestination cd = getCoreDestinationInternal(isQueue, destName);
         if (cd != null)
         {
            throw new JMSException("Destination " + destName + " already deployed");
         }
         
         // Might already be in db
         long id;
         Long l = getIdForDestination(isQueue, destName);
         if (l == null)
         {
            //Not in db - insert a new mapping row
            id = this.getNextId();
            
            insertMappingRow(id, isQueue ? TYPE_QUEUE : TYPE_TOPIC,
                             destName, null, null, null, null);
         }
         else
         {
            id = l.longValue();
         }
         
         // TODO I am using LocalQueues for the time being, switch to distributed Queues
         if (isQueue)
         {
            cd = new Queue(id, ms, pm, true, fullSize, pageSize, downCacheSize);
            
            try
            {
               // we load the queue with any state it might have in the db
               ((Queue)cd).load();
            }
            catch (Exception e)
            {
               log.error("Failed to load queue state", e);
               JMSException e2 = new JMSException("Failed to load queue state");
               e2.setLinkedException(e);
               throw e2;
            }
            
            queues.put(destName, cd);
         }
         else
         {
            // TODO I am using LocalTopics for the time being, switch to distributed Topics
            cd = new Topic(id, fullSize, pageSize, downCacheSize);
            
            topics.put(destName, cd);
            
            // TODO: The following piece of code may be better placed either in the Topic itself or in
            //       the DurableSubscriptionStore - I'm not sure it really belongs here
            
            // Load any durable subscriptions for the Topic
            List durableSubs = loadDurableSubscriptionsForTopic(destName, ms, pm);
            
            Iterator iter = durableSubs.iterator();
            while (iter.hasNext())
            {
               CoreDurableSubscription sub = (CoreDurableSubscription)iter.next();
               //load the state of the dub
               try
               {
                  sub.load();
               }
               catch (Exception e)
               {
                  log.error("Failed to load queue state", e);
                  JMSException e2 = new JMSException("Failed to load durable subscription state");
                  e2.setLinkedException(e);
                  throw e2;
               }
               //and subscribe it to the Topic
               sub.subscribe();
            }
         }
         
         // Put in id map too
         
         JBossDestination jbd ;
         
         if (isQueue)
         {
            jbd = new JBossQueue(destName);
         }
         else
         {
            jbd =  new JBossTopic(destName);
         }
         
         idMap.put(new Long(id), jbd);

         log.debug("core destination " + cd + " (fullSize=" + fullSize + ", pageSize=" +
                   pageSize  + ", downCacheSize=" + downCacheSize + ") deployed");
      }
      catch (Exception e)
      {
         e.printStackTrace();
         throw new JBossJMSException("Failed to deploy core destination", e);        
      }
   }
   
   public CoreDestination undeployCoreDestination(boolean isQueue, String destName)
   {
      Map m = isQueue ? queues : topics;
      
      CoreDestination dest = (CoreDestination)m.remove(destName);
      
      if (dest != null)
      {
      
         idMap.remove(new Long(dest.getId()));
      }
      
      return dest;
   }
   
   
   public CoreDurableSubscription getDurableSubscription(String clientID,
                                                         String subscriptionName,                                                         
                                                         MessageStore ms,
                                                         PersistenceManager pm)
      throws JMSException
   {
      // Look in memory first
      CoreDurableSubscription sub = getDurableSubscription(clientID, subscriptionName);
      
      if (sub != null)
      {
         return sub;
      }
      
      //Now look in the db
      try
      {         
         Connection conn = null;
         PreparedStatement ps  = null;
         ResultSet rs = null;
         TransactionWrapper wrap = new TransactionWrapper();
         
         try
         {
            conn = ds.getConnection();
            
            ps = conn.prepareStatement(selectDurableSub);
            
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
                  String s = JDBCUtil.statementToString(selectDurableSub, clientID, subscriptionName);
                  log.trace(s + (rs == null ? " failed!" : (exists ? " returned rows" : " did NOT return rows")));
               }
            }
            
            if (exists)
            {
               String topicName = rs.getString(1);
               long id = rs.getLong(2);
               String selector = rs.getString(3);
               boolean noLocal = rs.getString(4).equals("Y");
               
               Map subs = (Map)subscriptions.get(clientID);
               if (subs == null)
               {
                  subs = new ConcurrentReaderHashMap();
                  subscriptions.put(clientID, subs);
               }
               
               // create in memory
               sub = createDurableSubscriptionInternal(id, topicName, clientID, subscriptionName, selector,
                                                       noLocal, ms, pm);
               
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
     
   public CoreDurableSubscription createDurableSubscription(String topicName,
                                                            String clientID,
                                                            String subscriptionName, 
                                                            String selector, 
                                                            boolean noLocal,                                                            
                                                            MessageStore ms,
                                                            PersistenceManager pm) throws JMSException
   {
      try
      {
         //First insert a row in the db
         long id = this.getNextId();
         
         insertMappingRow(id, TYPE_DURABLE_SUB, topicName, subscriptionName, clientID,
                          selector, new Boolean(noLocal));
         
         return createDurableSubscriptionInternal(id, topicName, clientID, subscriptionName, selector,
                                                  noLocal, ms, pm);
      }
      catch (Exception e)
      {
         e.printStackTrace();
         throw new JBossJMSException("Failed to create durable subscription", e);        
      }
     
   }
   
   public CoreSubscription createSubscription(String topicName, String selector, boolean noLocal,
         MessageStore ms, PersistenceManager pm) throws JMSException
   {
      try
      {
         long id = this.getNextId();
         
         Topic topic = (Topic)getCoreDestinationInternal(false, topicName);
         
         if (topic == null)
         {
            throw new javax.jms.IllegalStateException("Topic " + topicName + " is not loaded");
         }
                
         return new CoreSubscription(id, topic, selector, noLocal, ms, pm, topic.getFullSize(),
                                     topic.getPageSize(), topic.getDownCacheSize());
      }
      catch (Exception e)
      {
         throw new JBossJMSException("Failed to create durable subscription", e);        
      }
   }
   
   public boolean removeDurableSubscription(String clientID, String subscriptionName)
      throws JMSException
   {
      try
      {
         if (log.isTraceEnabled()) { log.trace("removing durable subscription " + subscriptionName); }
         
         if (clientID == null)
         {
            throw new JMSException("Client ID must be set for connection!");
         }
   
         Map subs = (Map)subscriptions.get(clientID);
   
         if (subs == null)
         {
            return false;
         }
   
         if (log.isTraceEnabled()) { log.trace("removing durable subscription " + subscriptionName); }
   
         CoreDurableSubscription removed = (CoreDurableSubscription)subs.remove(subscriptionName);
   
         if (subs.size() == 0)
         {
            subscriptions.remove(clientID);
         }
         
         if (removed == null)
         {
            return false;
         }
         
         //Now remove from db
         deleteMappingRow(removed.getChannelID());
         
         return true;
      }
      catch (Exception e)
      {
         throw new JBossJMSException("Failed to remove durable subscription", e);        
      }
   }
   
   //FIXME - This doesn't belong here
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
   
  
   // MBean operations ----------------------------------------------
   
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
   
   /**
    * @return a Set<String>. It may return an empty Set, but never null.
    */
   public Set getSubscriptions(String clientID)
   {
      Map m = (Map)subscriptions.get(clientID);
      if (m == null)
      {
         return Collections.EMPTY_SET;
      }
      // concurrent keyset is not serializable
      Set result = new HashSet();
      result.addAll(m.keySet());
      return result;
   }
   
   public String toString()
   {
      return "JDBCChannelMapper[" + Integer.toHexString(hashCode()) + "]";
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected List loadDurableSubscriptionsForTopic(String topicName,                                               
                                                  MessageStore ms,
                                                  PersistenceManager pm) throws JMSException
   {      
      try
      {
         
         List result = new ArrayList();
         
         try
         {
            Connection conn = null;
            PreparedStatement ps  = null;
            ResultSet rs = null;
            TransactionWrapper wrap = new TransactionWrapper();
            int rowCount = -1;
            
            try
            {
               conn = ds.getConnection();
      
               ps = conn.prepareStatement(selectSubscriptionsForTopic);
               
               ps.setString(1, topicName);
               
               rs = ps.executeQuery();
               
               rowCount = 0;
               
               while (rs.next())
               {
                  rowCount ++;
                  long id = rs.getLong(1);
                  String clientId = rs.getString(2);
                  String subName = rs.getString(3);
                  String selector = rs.getString(4);
                  boolean noLocal = rs.getString(5).equals("Y");
                                    
                  CoreDurableSubscription sub = getDurableSubscription(clientId, subName);
                  
                  if (sub == null)
                  {
                     sub = createDurableSubscriptionInternal(id, topicName,
                                                            clientId,
                                                            subName,
                                                            selector,
                                                            noLocal,
                                                            ms, pm);
                     result.add(sub);
                  }                  
               }
            }
            catch (SQLException e)
            {
               wrap.exceptionOccurred();
               throw e;
            }
            finally
            {
               if (log.isTraceEnabled())
               {
                  String s = JDBCUtil.statementToString(selectSubscriptionsForTopic, topicName);
                  log.trace(s + " returned " + rowCount + " rows");
               }            
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
            final String msg = "Failed to get durable subscription";
            log.error(msg, e);
            JMSException e2 = new JMSException(msg);
            e2.setLinkedException(e);
            throw e2;
         }
                 
         return result;
      }
      catch (Exception e)
      {
         throw new JBossJMSException("Failed to load durable subscriptions for topic", e);        
      }
   }

   

   protected void insertMappingRow(long id, char type, String jmsDestName, String jmsSubName, String clientID,
                                   String selector, Boolean noLocal) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps  = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      boolean failed = true;
      
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(insertMapping);
         
         ps.setLong(1, id);
         
         ps.setString(2, String.valueOf(type)); 
         
         ps.setString(3, jmsDestName);
         
         ps.setString(4, jmsSubName);
         
         ps.setString(5, clientID);
         
         ps.setString(6, selector);
         
         if (noLocal == null)
         {
            ps.setNull(7, Types.VARCHAR);
         }
         else
         {
            ps.setString(7, noLocal.booleanValue() ? "Y" : "N");
         }
                  
         int rows = ps.executeUpdate();
         
         failed = rows != 1;
          
         ps.close();
         
         ps = null;        
      }
      finally
      {
         if (log.isTraceEnabled())
         {
            String s = JDBCUtil.
            statementToString(insertMapping, null, String.valueOf(type), jmsDestName, jmsSubName,
                  clientID, selector, noLocal);
            log.trace(s + (failed ? " failed!" : " executed successfully"));
         }
         
         if (rs != null)
         {
            try
            {
               rs.close();
            }
            catch (Throwable t)
            {}
         }
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
   
   protected Long getIdForDestination(boolean isQueue, String destName) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps  = null;
      ResultSet rs = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(selectIdForDestination);
         
         ps.setString(1, String.valueOf(isQueue ? TYPE_QUEUE : TYPE_TOPIC));
         
         ps.setString(2, destName);
        
         rs = ps.executeQuery();  
         
         if (rs.next())
         {
            return new Long(rs.getLong(1));
         }
         else
         {
            return null;
         }         
      }
      finally
      {
         if (rs != null)
         {
            try
            {
               rs.close();
            }
            catch (Throwable t)
            {}
         }
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
   
   
   
   
   protected boolean deleteMappingRow(long id) throws Exception
   {
      Connection conn = null;
      PreparedStatement ps  = null;
      TransactionWrapper wrap = new TransactionWrapper();
      
      boolean failed = true;
      
      try
      {
         conn = ds.getConnection();
         
         ps = conn.prepareStatement(deleteMapping);
         
         ps.setLong(1, id);
        
         failed = ps.executeUpdate() != -1;      
         
         return !failed;
      }
      finally
      {
         if (log.isTraceEnabled())
         {
            String s = JDBCUtil.statementToString(deleteMapping, new Long(id));
            log.trace(s + (failed ? " failed!" : " executed successfully"));
         }
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
   
   protected CoreDurableSubscription getDurableSubscription(String clientID,
                                                            String subscriptionName) throws JMSException
   {
      Map subs = (Map)subscriptions.get(clientID);
      return subs == null ? null : (CoreDurableSubscription)subs.get(subscriptionName);
   }
   
   
   protected CoreDurableSubscription createDurableSubscriptionInternal(long id, String topicName,
                                                                       String clientID,
                                                                       String subscriptionName, 
                                                                       String selector, 
                                                                       boolean noLocal,                                                            
                                                                       MessageStore ms,
                                                                       PersistenceManager pm) throws Exception
   {
      Map subs = (Map)subscriptions.get(clientID);
      if (subs == null)
      {
         subs = new ConcurrentReaderHashMap();
         subscriptions.put(clientID, subs);
      }
      
      Topic topic = (Topic)getCoreDestinationInternal(false, topicName);
      
      if (topic == null)
      {
         throw new javax.jms.IllegalStateException("Topic " + topicName + " is not loaded");
      }
      
      CoreDurableSubscription subscription =
         new CoreDurableSubscription(id, clientID, subscriptionName, topic, selector, noLocal, ms, pm, 
               topic.getFullSize(), topic.getPageSize(), topic.getDownCacheSize());
      
      subs.put(subscriptionName, subscription);
      
      return subscription;
   }   
   
   protected CoreDestination getCoreDestinationInternal(boolean isQueue, String destName)
   {
      Map m = isQueue ? queues : topics;
      
      return (CoreDestination)m.get(destName);
   }
   
   protected void initSqlProperties()
   {
      createUserTable = sqlProperties.getProperty("CREATE_USER_TABLE", createUserTable);
      createRoleTable = sqlProperties.getProperty("CREATE_ROLE_TABLE", createRoleTable);
      selectPreConfClientId = sqlProperties.getProperty("SELECT_PRECONF_CLIENTID", selectPreConfClientId);      
      createMappingTable = sqlProperties.getProperty("CREATE_MAPPING_TABLE", createMappingTable);
      insertMapping = sqlProperties.getProperty("INSERT_MAPPING", insertMapping);
      deleteMapping = sqlProperties.getProperty("DELETE_MAPPING", deleteMapping);
      selectIdForDestination = sqlProperties.getProperty("SELECT_ID_FOR_DESTINATION", selectIdForDestination);
      selectDurableSub = sqlProperties.getProperty("SELECT_DURABLE_SUB", selectDurableSub);
      selectSubscriptionsForTopic = sqlProperties.getProperty("SELECT_SUBSCRIPTIONS_FOR_TOPIC", selectSubscriptionsForTopic);
            
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
            executeUpdate(createMappingTable);
         }
         catch (SQLException e) 
         {
            log.debug("Failed to create subscriptions table: " + createMappingTable, e);
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
   
   protected long getNextId() throws Exception
   {
      long id = this.channelIDManager.getId();
      
      return id;
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
