/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jms.tests.tools.container;

import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRY_INTERVAL;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.ObjectNameBuilder;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.integration.bootstrap.HornetQBootstrapServer;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.jms.HornetQTopic;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.management.JMSQueueControl;
import org.hornetq.jms.server.management.TopicControl;
import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;
import org.jboss.kernel.plugins.config.property.PropertyKernelConfig;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.1</tt>
 *          <p/>
 *          LocalTestServer.java,v 1.1 2006/02/21 08:25:32 timfox Exp
 */
public class LocalTestServer implements Server, Runnable
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(LocalTestServer.class);

   private boolean started = false;

   private HashMap<String, List<String>> allBindings = new HashMap<String, List<String>>();

   // Static ---------------------------------------------------------------------------------------

   public static void setEnvironmentServerIndex(int serverIndex)
   {
      System.setProperty(Constants.SERVER_INDEX_PROPERTY_NAME, Integer.toString(serverIndex));
   }

   public static void clearEnvironmentServerIndex()
   {
      System.getProperty(Constants.SERVER_INDEX_PROPERTY_NAME, null);
   }

   // Attributes -----------------------------------------------------------------------------------

   private int serverIndex;

   HornetQBootstrapServer bootstrap;

   // Constructors ---------------------------------------------------------------------------------

   public LocalTestServer()
   {
      super();

      this.serverIndex = 0;
   }

   // Server implementation ------------------------------------------------------------------------

   public int getServerID()
   {
      return serverIndex;
   }

   public synchronized void start(String[] containerConfig, HashMap<String, Object> configuration, boolean clearJournal) throws Exception
   {
      if (isStarted())
      {
         return;
      }

      if (clearJournal)
      {
         // Delete the Journal environment

         File dir = new File("data");

         boolean deleted = deleteDirectory(dir);

         log.info("Deleted dir: " + dir.getAbsolutePath() + " deleted: " + deleted);
      }

      PropertyKernelConfig propertyKernelConfig = new PropertyKernelConfig(System.getProperties());
      bootstrap = new HornetQBootstrapServer(propertyKernelConfig, containerConfig);
      System.setProperty(Constants.SERVER_INDEX_PROPERTY_NAME, "" + getServerID());
      bootstrap.run();
      started = true;

   }

   private static boolean deleteDirectory(File directory)
   {
      if (directory.isDirectory())
      {
         String[] files = directory.list();

         for (int j = 0; j < files.length; j++)
         {
            if (!deleteDirectory(new File(directory, files[j])))
            {
               return false;
            }
         }
      }

      return directory.delete();
   }

   public synchronized boolean stop() throws Exception
   {
      bootstrap.shutDown();
      started = false;
      unbindAll();
      return true;
   }

   public void ping() throws Exception
   {
      if (!isStarted())
      {
         throw new RuntimeException("ok");
      }
   }

   public synchronized void kill() throws Exception
   {
      stop();
   }

   public synchronized boolean isStarted() throws Exception
   {
      return started;
   }

   public synchronized void startServerPeer() throws Exception
   {
      System.setProperty(Constants.SERVER_INDEX_PROPERTY_NAME, "" + getServerID());
      getHornetQServer().start();
   }

   public synchronized void stopServerPeer() throws Exception
   {
      System.setProperty(Constants.SERVER_INDEX_PROPERTY_NAME, "" + getServerID());
      getHornetQServer().stop();
      // also unbind everything
      unbindAll();
   }

   private void unbindAll() throws Exception
   {
      Collection<List<String>> bindings = allBindings.values();
      for (List<String> binding : bindings)
      {
         for (String s : binding)
         {
            getInitialContext().unbind(s);
         }
      }
   }

   /**
    * Only for in-VM use!
    */
   public HornetQServer getServerPeer()
   {
      return getHornetQServer();
   }

   public void destroyQueue(String name, String jndiName) throws Exception
   {
      this.getJMSServerManager().destroyQueue(name);
   }

   public void destroyTopic(String name, String jndiName) throws Exception
   {
      this.getJMSServerManager().destroyTopic(name);
   }

   public void createQueue(String name, String jndiName) throws Exception
   {
      this.getJMSServerManager().createQueue(name, "/queue/" + (jndiName != null ? jndiName : name), null, true);
   }

   public void createTopic(String name, String jndiName) throws Exception
   {
      this.getJMSServerManager().createTopic(name, "/topic/" + (jndiName != null ? jndiName : name));
   }

   public void deployConnectionFactory(String clientId, String objectName, List<String> jndiBindings) throws Exception
   {
      deployConnectionFactory(clientId, objectName, jndiBindings, -1, -1, -1, -1, false, false, -1, false);
   }

   public void deployConnectionFactory(String objectName, List<String> jndiBindings, int consumerWindowSize) throws Exception
   {
      deployConnectionFactory(null, objectName, jndiBindings, consumerWindowSize, -1, -1, -1, false, false, -1, false);
   }

   public void deployConnectionFactory(String objectName, List<String> jndiBindings) throws Exception
   {
      deployConnectionFactory(null, objectName, jndiBindings, -1, -1, -1, -1, false, false, -1, false);
   }

   public void deployConnectionFactory(String objectName,
                                       List<String> jndiBindings,
                                       int prefetchSize,
                                       int defaultTempQueueFullSize,
                                       int defaultTempQueuePageSize,
                                       int defaultTempQueueDownCacheSize) throws Exception
   {
      this.deployConnectionFactory(null,
                                   objectName,
                                   jndiBindings,
                                   prefetchSize,
                                   defaultTempQueueFullSize,
                                   defaultTempQueuePageSize,
                                   defaultTempQueueDownCacheSize,
                                   false,
                                   false,
                                   -1,
                                   false);
   }

   public void deployConnectionFactory(String objectName,
                                       List<String> jndiBindings,
                                       boolean supportsFailover,
                                       boolean supportsLoadBalancing) throws Exception
   {
      this.deployConnectionFactory(null,
                                   objectName,
                                   jndiBindings,
                                   -1,
                                   -1,
                                   -1,
                                   -1,
                                   supportsFailover,
                                   supportsLoadBalancing,
                                   -1,
                                   false);
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
      List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();

      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(new TransportConfiguration("org.hornetq.integration.transports.netty.NettyConnectorFactory"),
                                                                                    null));

      getJMSServerManager().createConnectionFactory(objectName,
                                                    connectorConfigs,
                                                    clientId,
                                                    DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                    DEFAULT_CONNECTION_TTL,
                                                    DEFAULT_CALL_TIMEOUT,                                                   
                                                    DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,                                                    
                                                    DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                    prefetchSize,
                                                    DEFAULT_CONSUMER_MAX_RATE,
                                                    DEFAULT_PRODUCER_WINDOW_SIZE,
                                                    DEFAULT_PRODUCER_MAX_RATE,
                                                    blockOnAcknowledge,
                                                    true,
                                                    true,
                                                    DEFAULT_AUTO_GROUP,
                                                    DEFAULT_PRE_ACKNOWLEDGE,
                                                    DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                    DEFAULT_ACK_BATCH_SIZE,
                                                    dupsOkBatchSize,
                                                    DEFAULT_USE_GLOBAL_POOLS,
                                                    DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                                    DEFAULT_THREAD_POOL_MAX_SIZE,                                                     
                                                    DEFAULT_RETRY_INTERVAL,
                                                    DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                    DEFAULT_MAX_RETRY_INTERVAL,
                                                    DEFAULT_RECONNECT_ATTEMPTS,
                                                    DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN,
                                                    jndiBindings);
   }

   public void undeployConnectionFactory(String objectName) throws Exception
   {
      getJMSServerManager().destroyConnectionFactory(objectName);
   }

   public void configureSecurityForDestination(String destName, boolean isQueue, Set<Role> roles) throws Exception
   {
      String destination = (isQueue ? "jms.queue." : "jms.topic.") + destName;
      if (roles != null)
      {
         getHornetQServer().getSecurityRepository().addMatch(destination, roles);
      }
      else
      {
         getHornetQServer().getSecurityRepository().removeMatch(destination);
      }
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   public HornetQServer getHornetQServer()
   {
      return (HornetQServer)bootstrap.getKernel().getRegistry().getEntry("HornetQServer").getTarget();
   }

   public JMSServerManager getJMSServerManager()
   {
      return (JMSServerManager)bootstrap.getKernel().getRegistry().getEntry("JMSServerManager").getTarget();
   }

   public InitialContext getInitialContext() throws Exception
   {
      Properties props = new Properties();
      props.setProperty("java.naming.factory.initial",
                        "org.hornetq.jms.tests.tools.container.InVMInitialContextFactory");
      props.setProperty(Constants.SERVER_INDEX_PROPERTY_NAME, "" + getServerID());
      // props.setProperty("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
      return new InitialContext(props);
   }

   public void run()
   {
      bootstrap.run();

      started = true;

      synchronized (this)
      {
         notify();
         try
         {
            wait();
         }
         catch (InterruptedException e)
         {
            // e.printStackTrace();
         }
      }

   }

   public Integer getMessageCountForQueue(String queueName) throws Exception
   {
      JMSQueueControl queue = (JMSQueueControl)getHornetQServer().getManagementService()
                                                                             .getResource(ResourceNames.JMS_QUEUE + queueName);
      if (queue != null)
      {
         return queue.getMessageCount();
      }
      else
      {
         return -1;
      }
   }

   public void removeAllMessages(String destination, boolean isQueue) throws Exception
   {
      SimpleString address = HornetQQueue.createAddressFromName(destination);
      if (!isQueue)
      {
         address = HornetQTopic.createAddressFromName(destination);
      }
      Binding binding = getHornetQServer().getPostOffice().getBinding(address);
      if (binding != null && binding.getType() == BindingType.LOCAL_QUEUE)
      {
         ((Queue)binding.getBindable()).deleteAllReferences();
      }
   }

   public List<String> listAllSubscribersForTopic(String s) throws Exception
   {
      ObjectName objectName = ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(s);
      TopicControl topic = (TopicControl)MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                                                 objectName,
                                                                                                 TopicControl.class,
                                                                                                 false);
      Object[] subInfos = topic.listAllSubscriptions();
      List<String> subs = new ArrayList<String>();
      for (Object o : subInfos)
      {
         Object[] data = (Object[])o;
         subs.add((String)data[2]);
      }
      return subs;
   }

   public Set<Role> getSecurityConfig() throws Exception
   {
      return getHornetQServer().getSecurityRepository().getMatch("*");
   }

   public void setSecurityConfig(Set<Role> defConfig) throws Exception
   {
      getHornetQServer().getSecurityRepository().removeMatch("#");
      getHornetQServer().getSecurityRepository().addMatch("#", defConfig);
   }

   // Inner classes --------------------------------------------------------------------------------

}
