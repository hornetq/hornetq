/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.jms.server;

import java.net.URL;
import java.util.List;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.impl.JMSServerDeployer;
import org.jboss.messaging.tests.unit.util.InVMContext;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.Pair;
import org.w3c.dom.Element;

/**
 * A JMSServerDeployerTest
 *
 * @author jmesnil
 *
 *
 */
public class JMSServerDeployerTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   private JMSServerManager jmsServer;

   private Context context;
   
   private DeploymentManager deploymentManager;
   
   private Configuration config;

   // Public --------------------------------------------------------

   public void testValidateEmptyConfiguration() throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer,
                                                         deploymentManager,
                                                         config);

      String xml = "<deployment xmlns='urn:jboss:messaging'> " + "</deployment>";

      Element rootNode = org.jboss.messaging.utils.XMLUtil.stringToElement(xml);
      deployer.validate(rootNode);
   }

   public void testDeployFullConfiguration() throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer,
                                                         deploymentManager,
                                                         config);

      String conf = "jbm-jms-for-JMSServerDeployerTest.xml";
      URL confURL = Thread.currentThread().getContextClassLoader().getResource(conf);

      deployer.deploy(confURL);

      JBossConnectionFactory cf = (JBossConnectionFactory)context.lookup("/testConnectionFactory");
      assertNotNull(cf);
      assertEquals(1234, cf.getPingPeriod());
      assertEquals(5678, cf.getCallTimeout());
      assertEquals(12345, cf.getConsumerWindowSize());
      assertEquals(6789, cf.getConsumerMaxRate());
      assertEquals(123456, cf.getProducerWindowSize());
      assertEquals(789, cf.getProducerMaxRate());
      assertEquals(12, cf.getMinLargeMessageSize());
      assertEquals("TestClientID", cf.getClientID());
      assertEquals(3456, cf.getDupsOKBatchSize());
      assertEquals(4567, cf.getTransactionBatchSize());
      assertEquals(true, cf.isBlockOnAcknowledge());
      assertEquals(false, cf.isBlockOnNonPersistentSend());
      assertEquals(true, cf.isBlockOnPersistentSend());
      assertEquals(false, cf.isAutoGroup());
      assertEquals(true, cf.isPreAcknowledge());
      assertEquals(2345, cf.getConnectionTTL());
      assertEquals(false, cf.isFailoverOnServerShutdown());
      assertEquals(12, cf.getMaxConnections());
      assertEquals(34, cf.getReconnectAttempts());
      assertEquals(5, cf.getRetryInterval());
      assertEquals(6.0, cf.getRetryIntervalMultiplier());
      
      Queue queue = (Queue)context.lookup("/testQueue");
      assertNotNull(queue);
      assertEquals("testQueue", queue.getQueueName());

      Queue queue2 = (Queue)context.lookup("/queue/testQueue");
      assertNotNull(queue2);
      assertEquals("testQueue", queue2.getQueueName());

      Topic topic = (Topic)context.lookup("/testTopic");
      assertNotNull(topic);
      assertEquals("testTopic", topic.getTopicName());

      Topic topic2 = (Topic)context.lookup("/topic/testTopic");
      assertNotNull(topic2);
      assertEquals("testTopic", topic2.getTopicName());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      config = new ConfigurationImpl();
      
      jmsServer = new DummyJMSServerManager();

      context = new InVMContext();
      
      jmsServer.setContext(context);
   }


   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
   class DummyJMSServerManager implements JMSServerManager
   {

      public boolean closeConnectionsForAddress(String ipAddress) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean createConnectionFactory(String name,
                                             DiscoveryGroupConfiguration discoveryGroupConfig,
                                             long discoveryInitialWait,
                                             String connectionLoadBalancingPolicyClassName,
                                             long pingPeriod,
                                             long connectionTTL,
                                             long callTimeout,
                                             String clientID,
                                             int dupsOKBatchSize,
                                             int transactionBatchSize,
                                             int consumerWindowSize,
                                             int consumerMaxRate,
                                             int sendWindowSize,
                                             int producerMaxRate,
                                             int minLargeMessageSize,
                                             boolean blockOnAcknowledge,
                                             boolean blockOnNonPersistentSend,
                                             boolean blockOnPersistentSend,
                                             boolean autoGroup,
                                             int maxConnections,
                                             boolean preAcknowledge,
                                             long retryInterval,
                                             double retryIntervalMultiplier,
                                             int reconnectAttempts,
                                             boolean failoverOnNodeShutdown,
                                             List<String> jndiBindings) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean createConnectionFactory(String name,
                                             List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                             boolean blockOnAcknowledge,
                                             boolean blockOnNonPersistentSend,
                                             boolean blockOnPersistentSend,
                                             boolean preAcknowledge,
                                             List<String> bindings) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean createConnectionFactory(String name,
                                             List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                             List<String> jndiBindings) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean createConnectionFactory(String name,
                                             List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                             String connectionLoadBalancingPolicyClassName,
                                             long pingPeriod,
                                             long connectionTTL,
                                             long callTimeout,
                                             String clientID,
                                             int dupsOKBatchSize,
                                             int transactionBatchSize,
                                             int consumerWindowSize,
                                             int consumerMaxRate,
                                             int sendWindowSize,
                                             int producerMaxRate,
                                             int minLargeMessageSize,
                                             boolean blockOnAcknowledge,
                                             boolean blockOnNonPersistentSend,
                                             boolean blockOnPersistentSend,
                                             boolean autoGroup,
                                             int maxConnections,
                                             boolean preAcknowledge,
                                             long retryInterval,
                                             double retryIntervalMultiplier,
                                             int reconnectAttempts,
                                             boolean failoverOnNodeShutdown,
                                             List<String> jndiBindings) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean createQueue(String queueName, String jndiBinding) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean createTopic(String topicName, String jndiBinding) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean destroyConnectionFactory(String name) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean destroyQueue(String name) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean destroyTopic(String name) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public String getVersion()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public boolean isStarted()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public String[] listConnectionIDs() throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String[] listRemoteAddresses() throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String[] listRemoteAddresses(String ipAddress) throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String[] listSessions(String connectionID) throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void setContext(Context context)
      {
         // TODO Auto-generated method stub
         
      }

      public boolean undeployDestination(String name) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void start() throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      public void stop() throws Exception
      {
         // TODO Auto-generated method stub
         
      }
      
   }

}
