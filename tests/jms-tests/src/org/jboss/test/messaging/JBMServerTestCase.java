/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.test.messaging;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.ConnectionManager;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.microcontainer.JBMBootstrapServer;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.container.DatabaseClearer;
import org.jboss.test.messaging.tools.container.Server;
import org.jboss.tm.TransactionManagerLocator;

/**
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.org">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JBMServerTestCase extends JBMBaseTestCase
{
   // Constants -----------------------------------------------------

   public final static int MAX_TIMEOUT = 1000 * 10 /* seconds */;

   public final static int MIN_TIMEOUT = 1000 * 1 /* seconds */;

   protected static List<Server> servers = new ArrayList<Server>();

   protected static Topic topic1;

   protected static Topic topic2;

   protected static Topic topic3;

   protected static Queue queue1;

   protected static Queue queue2;

   protected static Queue queue3;

   protected static Queue queue4;
   private static DatabaseClearer databaseClearer;


   public JBMServerTestCase()
   {
      super();    //To change body of overridden methods use File | Settings | File Templates.
   }

   public JBMServerTestCase(String string)
   {
      super(string);    //To change body of overridden methods use File | Settings | File Templates.
   }

   protected void setUp() throws Exception
   {
      super.setUp();
      
      String banner =
         "####################################################### Start " +
         (isRemote() ? "REMOTE" : "IN-VM") + " test: " + getName();

      log.info(banner);

    
      if (getClearDatabase())
      {
         //clearDatabase();
      }
      try
      {
         //create any new server we need
         for (int i = servers.size(); i < getServerCount(); i++)
         {
            servers.add(ServerManagement.create(i));
         }
         //kill off any servers we dont need anymore
         for (int i = getServerCount(); i < servers.size();)
         {
            try
            {

               servers.get(i).stop();
            }
            catch (Exception e)
            {
               //ignore, as it meay be remote and stopped anyway
            }
            servers.remove(i);
         }
         //start the servers if needed
         for (int i = 0; i < servers.size(); i++)
         {
            boolean started = false;
            try
            {
               started = servers.get(i).isStarted();
            }
            catch (Exception e)
            {
               //ignore, incase its a remote server
            }
            if (!started)
            {
               if(i > getServerCount())
               {
                  servers.get(i).stop();
               }
               else
               {
//                  try
//                  {
                     servers.get(i).start(getContainerConfig(), getConfiguration(), getClearDatabase() && i == 0);
//                  }
//                  catch (Exception e)
//                  {
//                     //if we are remote we will need recreating if we get here
//                     servers.set(i, ServerManagement.create(i));
//                     servers.get(i).start(getContainerConfig(), getConfiguration(), getClearDatabase() && i == 0);
//                  }
               }
               //deploy the objects for this test
               deployAdministeredObjects(i);
            }
         }
         lookUp();
      }
      catch (Exception e)
      {
         //if we get here we need to clean up for the next test
         e.printStackTrace();
         for (int i = 0; i < getServerCount(); i++)
         {
            servers.get(i).stop();
         }
         throw e;
      }
      //empty the queues
      checkEmpty(queue1);
      checkEmpty(queue2);
      checkEmpty(queue3);
      checkEmpty(queue4);

      // Check no subscriptions left lying around

      checkNoSubscriptions(topic1);
      checkNoSubscriptions(topic2);
      checkNoSubscriptions(topic3);

      if (isRemote())
      {
         // log the test start in the remote log, this will make hunting through logs so much easier
         ServerManagement.log(ServerManagement.INFO, banner);
      }           
   }

   protected boolean isRemote()
   {
      return ServerManagement.isRemote();
   }
   
   public void stop() throws Exception
   {
      for (int i = 0; i < getServerCount(); i++)
      {
         servers.get(i).stop();
      }
   }

   public void start() throws Exception
   {
      System.setProperty("java.naming.factory.initial", getContextFactory());
      for (int i = 0; i < getServerCount(); i++)
      {
         servers.get(i).start(getContainerConfig(), getConfiguration(), i != 0);
      }
      //deployAdministeredObjects();
   }
   
   public void startNoDelete() throws Exception
   {
      System.setProperty("java.naming.factory.initial", getContextFactory());
      for (int i = 0; i < getServerCount(); i++)
      {
         servers.get(i).start(getContainerConfig(), getConfiguration(), false);
      }
      //deployAdministeredObjects();
   }

   public void stopServerPeer() throws Exception
   {
      for (int i = 0; i < getServerCount(); i++)
      {
         servers.get(i).stopServerPeer();
      }
   }

   public void startServerPeer() throws Exception
   {
      System.setProperty("java.naming.factory.initial", getContextFactory());
      for (int i = 0; i < getServerCount(); i++)
      {
         servers.get(i).startServerPeer(i, null, null, null, false);
      }
      //deployAdministeredObjects();
   }


   protected boolean getClearDatabase()
   {
      return true;
   }

   protected HashMap<String, Object> getConfiguration()
   {
      return new HashMap<String, Object>();
   }

   protected void deployAndLookupAdministeredObjects() throws Exception
   {
      createTopic("Topic1");
      createTopic("Topic2");
      createTopic("Topic3");
      createQueue("Queue1");
      createQueue("Queue2");
      createQueue("Queue3");
      createQueue("Queue4");

      lookUp();
   }

   protected void deployAdministeredObjects(int i) throws Exception
   {
      createTopic("Topic1", i);
      createTopic("Topic2", i);
      createTopic("Topic3", i);
      createQueue("Queue1", i);
      createQueue("Queue2", i);
      createQueue("Queue3", i);
      createQueue("Queue4", i);
   }

   private void lookUp()
           throws Exception
   {
      InitialContext ic = getInitialContext();
      topic1 = (Topic) ic.lookup("/topic/Topic1");
      topic2 = (Topic) ic.lookup("/topic/Topic2");
      topic3 = (Topic) ic.lookup("/topic/Topic3");
      queue1 = (Queue) ic.lookup("/queue/Queue1");
      queue2 = (Queue) ic.lookup("/queue/Queue2");
      queue3 = (Queue) ic.lookup("/queue/Queue3");
      queue4 = (Queue) ic.lookup("/queue/Queue4");
   }

   protected void undeployAdministeredObjects() throws Exception
   {
      removeAllMessages("Topic1", false);
      removeAllMessages("Topic2", false);
      removeAllMessages("Topic3", false);
      removeAllMessages("Queue1", true);
      removeAllMessages("Queue2", true);
      removeAllMessages("Queue3", true);
      removeAllMessages("Queue4", true);

      destroyTopic("Topic1");
      destroyTopic("Topic2");
      destroyTopic("Topic3");
      destroyQueue("Queue1");
      destroyQueue("Queue2");
      destroyQueue("Queue3");
      destroyQueue("Queue4");
   }


   public String[] getContainerConfig()
   {
         return new String[]{ "invm-beans.xml", "jbm-beans.xml"};
   }

   protected MessagingServer getJmsServer() throws Exception
   {
      return servers.get(0).getMessagingServer();
   }

   protected JMSServerManager getJmsServerManager() throws Exception
   {
      return servers.get(0).getJMSServerManager();
   }
   /*protected void tearDown() throws Exception
   {
      super.tearDown();
      //undeployAdministeredObjects();
      for (int i = 0; i < getServerCount(); i++)
      {
         servers.get(i).stopServerPeer();
      }
   }*/

   public int getServerCount()
   {
      return 1;
   }

   public InitialContext getInitialContext() throws Exception
   {
      return getInitialContext(0);
   }

   public JBossConnectionFactory getConnectionFactory() throws Exception
   {
      return (JBossConnectionFactory) getInitialContext().lookup("/ConnectionFactory");
   }


   public InitialContext getInitialContext(int serverid) throws Exception
   {
      return new InitialContext(ServerManagement.getJNDIEnvironment(serverid));
   }

   public void configureSecurityForDestination(String destName, boolean isQueue, HashSet<Role> roles) throws Exception
   {
      servers.get(0).configureSecurityForDestination(destName, isQueue, roles);
   }
   
   public void createQueue(String name) throws Exception
   {
      for (int i = 0; i < getServerCount(); i++)
      {
         servers.get(i).createQueue(name, null);
      }
   }
   
   public void createTopic(String name) throws Exception
   {
      for (int i = 0; i < getServerCount(); i++)
      {
         servers.get(i).createTopic(name, null);
      }
   }
   
   public void destroyQueue(String name) throws Exception
   {
      for (int i = 0; i < getServerCount(); i++)
      {
         servers.get(i).destroyQueue(name, null);
      }
   }
   
   public void destroyTopic(String name) throws Exception
   {
      for (int i = 0; i < getServerCount(); i++)
      {
         servers.get(i).destroyTopic(name, null);
      }
   }
   
   
   public void createQueue(String name, int i) throws Exception
   {
      log.info("********Creating queue " + name);
      servers.get(i).createQueue(name, null);      
   }
   
   public void createTopic(String name, int i) throws Exception
   {
      servers.get(i).createTopic(name, null);      
   }
   
   public void destroyQueue(String name, int i) throws Exception
   {
      servers.get(i).destroyQueue(name, null);      
   }

//   public void deployQueue(String name) throws Exception
//   {
//      for (int i = 0; i < getServerCount(); i++)
//      {
//         servers.get(i).deployQueue(name, null, i != 0);
//      }
//   }
//
//   public void deployQueue(String name, String jndiName) throws Exception
//   {
//      for (int i = 0; i < getServerCount(); i++)
//      {
//         servers.get(i).deployQueue(name, jndiName, i != 0);
//      }
//   }
//
//   public void deployQueue(String name, int server) throws Exception
//   {
//      servers.get(server).deployQueue(name, null, true);
//
//   }
//
//   public void deployQueue(String name, int fullSize, int pageSize, int downCacheSize) throws Exception
//   {
//      for (int i = 0; i < getServerCount(); i++)
//      {
//         servers.get(i).deployQueue(name, null, fullSize, pageSize, downCacheSize, i != 0);
//      }
//   }
//
//   public void deployQueue(String name, String jndiName, int fullSize, int pageSize,
//                           int downCacheSize, int serverIndex, boolean clustered) throws Exception
//   {
//      servers.get(serverIndex).deployQueue(name, jndiName, fullSize, pageSize, downCacheSize, clustered);
//
//   }
//
//   public void deployTopic(String name) throws Exception
//   {
//      for (int i = 0; i < getServerCount(); i++)
//      {
//         servers.get(i).deployTopic(name, null, i != 0);
//      }
//   }
//
//   public void deployTopic(String name, String jndiName) throws Exception
//   {
//      for (int i = 0; i < getServerCount(); i++)
//      {
//         servers.get(i).deployTopic(name, jndiName, i != 0);
//      }
//   }
//
//   public void deployTopic(String name, int server) throws Exception
//   {
//      servers.get(server).deployTopic(name, null, server != 0);
//
//   }
//
//   public void deployTopic(String name, int fullSize, int pageSize, int downCacheSize) throws Exception
//   {
//      for (int i = 0; i < getServerCount(); i++)
//      {
//         servers.get(i).deployTopic(name, null, fullSize, pageSize, downCacheSize, i != 0);
//      }
//   }
//
//   public void deployTopic(String name, String jndiName, int fullSize, int pageSize,
//                           int downCacheSize, int serverIndex, boolean clustered) throws Exception
//   {
//      servers.get(serverIndex).deployTopic(name, jndiName, fullSize, pageSize, downCacheSize, clustered);
//
//   }
//
//   public void undeployQueue(String name) throws Exception
//   {
//      for (int i = 0; i < getServerCount(); i++)
//      {
//         try
//         {
//            servers.get(i).undeployDestination(true, name);
//         }
//         catch (Exception e)
//         {
//            log.info("did not undeploy " + name);
//         }
//      }
//   }
//
//   public void undeployQueue(String name, int server) throws Exception
//   {
//      servers.get(server).undeployDestination(true, name);
//
//   }
//
//   public void undeployTopic(String name) throws Exception
//   {
//      for (int i = 0; i < getServerCount(); i++)
//      {
//         try
//         {
//            servers.get(i).undeployDestination(false, name);
//         }
//         catch (Exception e)
//         {
//            log.info("did not undeploy " + name);
//         }
//      }
//   }
//
//   public void undeployTopic(String name, int server) throws Exception
//   {
//      servers.get(server).undeployDestination(false, name);
//
//   }
//
//   public static boolean destroyTopic(String name) throws Exception
//   {
//      return servers.get(0).undeployDestinationProgrammatically(false, name);
//   }
//
//   public static boolean destroyQueue(String name) throws Exception
//   {
//      return servers.get(0).undeployDestinationProgrammatically(true, name);
//   }
//

   public boolean checkNoMessageData()
   {
      return false;
   }

   public boolean checkEmpty(Queue queue) throws Exception
   {
      Integer messageCount = servers.get(0).getMessageCountForQueue(queue.getQueueName());
      if (messageCount > 0)
      {
         removeAllMessages(queue.getQueueName(), true);
      }
      return true;
   }

   public boolean checkEmpty(Queue queue, int i)
   {
      return true;
   }

   public boolean checkEmpty(Topic topic)
   {
      return true;
   }

   public boolean checkEmpty(Topic topic, int i)
   {
      return true;
   }

   protected void removeAllMessages(String destName, boolean isQueue) throws Exception
   {
      for (int i = 0; i < getServerCount(); i++)
      {
         try
         {
            removeAllMessages(destName, isQueue, i);
         }
         catch (Exception e)
         {
            log.info("did not clear messages for " + destName);
         }
      }
   }

   protected void removeAllMessages(String destName, boolean isQueue, int server) throws Exception
   {
      if (isQueue)
      {
         servers.get(server).removeAllMessagesForQueue(destName);
      }
      else
      {
         servers.get(server).removeAllMessagesForTopic(destName);
      }
   }

   public void dropTables() throws Exception
   {
      dropAllTables();
   }

   private void dropAllTables() throws Exception
   {
      log.info("DROPPING ALL TABLES FROM DATABASE!");

      InitialContext ctx = new InitialContext();

      // We need to execute each drop in its own transaction otherwise postgresql will not execute
      // further commands after one fails

      TransactionManager mgr = TransactionManagerLocator.locateTransactionManager();
      DataSource ds = (DataSource) ctx.lookup("java:/DefaultDS");

      javax.transaction.Transaction txOld = mgr.suspend();

      executeStatement(mgr, ds, "DROP TABLE JBM_POSTOFFICE");

      executeStatement(mgr, ds, "DROP TABLE JBM_MSG_REF");

      executeStatement(mgr, ds, "DROP TABLE JBM_MSG");

      executeStatement(mgr, ds, "DROP TABLE JBM_TX");

      executeStatement(mgr, ds, "DROP TABLE JBM_COUNTER");

      executeStatement(mgr, ds, "DROP TABLE JBM_USER");

      executeStatement(mgr, ds, "DROP TABLE JBM_ROLE");

      executeStatement(mgr, ds, "DROP TABLE JBM_DUAL");

      if (txOld != null)
      {
         mgr.resume(txOld);
      }

      log.debug("done with dropping tables");
   }

   private void executeStatement(TransactionManager mgr, DataSource ds, String statement) throws Exception
   {
      Connection conn = null;
      boolean exception = false;

      try
      {
         try
         {
            mgr.begin();

            conn = ds.getConnection();

            log.debug("executing " + statement);

            PreparedStatement ps = conn.prepareStatement(statement);

            ps.executeUpdate();

            log.debug(statement + " executed");

            ps.close();
         }
         catch (SQLException e)
         {
            // Ignore
            log.debug("Failed to execute statement", e);
            exception = true;
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }

         if (exception)
         {
            mgr.rollback();
         }
         else
         {
            mgr.commit();
         }
      }


   }

   protected int getNoSubscriptions(Topic topic)
           throws Exception
   {
      return getNoSubscriptions(topic, 0);
   }

   protected int getNoSubscriptions(Topic topic, int server)
           throws Exception
   {
      ObjectName destObjectName = new ObjectName("jboss.messaging.destination:service=Topic,name=" + topic.getTopicName());

      Integer messageCount = (Integer) ServerManagement.getAttribute(server, destObjectName, "AllSubscriptionsCount");
      return messageCount.intValue();
   }

   protected boolean assertRemainingMessages(int expected) throws Exception
   {
      Integer messageCount = servers.get(0).getMessageCountForQueue("Queue1");

      assertEquals(expected, messageCount.intValue());
      return expected == messageCount.intValue();
   }

   protected static void assertActiveConnectionsOnTheServer(int expectedSize)
   throws Exception
   {
      assertEquals(expectedSize, servers.get(0).getMessagingServer().getServerManagement().getConnectionCount());
   }

   public static void deployConnectionFactory(String clientId, String objectName,
                                              List<String> jndiBindings)
           throws Exception
   {
      servers.get(0).deployConnectionFactory(clientId, objectName, jndiBindings);
   }

   public static void deployConnectionFactory(String objectName,
                                              List<String> jndiBindings,
                                              int prefetchSize)
           throws Exception
   {
      servers.get(0).deployConnectionFactory(objectName, jndiBindings, prefetchSize);
   }

   public static void deployConnectionFactory(String objectName,
                                              List<String> jndiBindings)
           throws Exception
   {
      servers.get(0).deployConnectionFactory(objectName, jndiBindings);
   }

   public static void deployConnectionFactory(int server,
                                              String objectName,
                                              List<String> jndiBindings,
                                              int prefetchSize)
           throws Exception
   {
      servers.get(server).deployConnectionFactory(objectName, jndiBindings, prefetchSize);
   }

   public static void deployConnectionFactory(int server,
                                              String objectName,
                                              List<String> jndiBindings)
           throws Exception
   {
      servers.get(server).deployConnectionFactory(objectName, jndiBindings);
   }

   public void deployConnectionFactory(String clientId,
                                       String objectName,
                                       List<String> jndiBindings,
                                       int prefetchSize,
                                       int defaultTempQueueFullSize,
                                       int defaultTempQueuePageSize,
                                       int defaultTempQueueDownCacheSize,
                                       boolean supportsFailover,
                                       boolean supportsLoadBalancing,              
                                       int dupsOkBatchSize,
                                       boolean blockOnAcknowledge) throws Exception
   {
      servers.get(0).deployConnectionFactory(clientId, objectName, jndiBindings, prefetchSize, defaultTempQueueFullSize,
              defaultTempQueuePageSize, defaultTempQueueDownCacheSize, supportsFailover, supportsLoadBalancing, dupsOkBatchSize, blockOnAcknowledge);
   }

   public static void deployConnectionFactory(String objectName,
                                              List<String> jndiBindings,
                                              int prefetchSize,
                                              int defaultTempQueueFullSize,
                                              int defaultTempQueuePageSize,
                                              int defaultTempQueueDownCacheSize)
           throws Exception
   {
      servers.get(0).deployConnectionFactory(objectName,
              jndiBindings,
              prefetchSize,
              defaultTempQueueFullSize,
              defaultTempQueuePageSize,
              defaultTempQueueDownCacheSize);
   }

   public static void undeployConnectionFactory(String objectName) throws Exception
   {
      servers.get(0).undeployConnectionFactory(objectName);
   }

   public static void undeployConnectionFactory(int server, String objectName) throws Exception
   {
      servers.get(server).undeployConnectionFactory(objectName);
   }

   protected List listAllSubscriptionsForTopic(String s) throws Exception
   {
      return servers.get(0).listAllSubscriptionsForTopic(s);
   }

   protected Integer getMessageCountForQueue(String s) throws Exception
   {
      return servers.get(0).getMessageCountForQueue(s);
   }

   protected Integer getMessageCountForQueue(String s, int server) throws Exception
   {
      return servers.get(server).getMessageCountForQueue(s);
   }

   protected Set<Role> getSecurityConfig() throws Exception
   {
      return servers.get(0).getSecurityConfig();
   }

   protected void setSecurityConfig(Set<Role> defConfig) throws Exception
   {
      servers.get(0).setSecurityConfig(defConfig);
   }

   protected void setSecurityConfigOnManager(boolean b, String s, HashSet<Role> lockedConf) throws Exception
   {
      servers.get(0).configureSecurityForDestination(s, b, lockedConf);
   }

   protected void setRedeliveryDelayOnDestination(String dest, boolean isQueue, long delay) throws Exception
   {
      servers.get(0).setRedeliveryDelayOnDestination(dest, isQueue, delay);
   }

   protected void kill(int i) throws Exception
   {
      log.info("Attempting to kill server " + i);

      if (i == 0)
      {
         //Cannot kill server 0 if there are any other servers since it has the rmi registry in it
         for (int j = 1; j < servers.size(); j++)
         {
            if (servers.get(j) != null)
            {
               throw new IllegalStateException("Cannot kill server 0, since server[" + j + "] still exists");
            }
         }
      }

      if (i >= servers.size())
      {
         log.info("server " + i + " has not been created or has already been killed, so it cannot be killed");
      }
      else
      {
         Server server = servers.get(i);
         log.info("invoking kill() on server " + i);
         try
         {
            server.kill();
         }
         catch (Throwable t)
         {
            // This is likely to throw an exception since the server dies before the response is received
         }

         log.info("Waiting for server to die");

         try
         {
            while (true)
            {
               server.ping();
               log.debug("server " + i + " still alive ...");
               Thread.sleep(100);
            }
         }
         catch (Throwable e)
         {
            //Ok
         }

         Thread.sleep(300);

         log.info("server " + i + " killed and dead");
      }
   }   
}
