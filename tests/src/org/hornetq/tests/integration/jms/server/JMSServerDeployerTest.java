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

package org.hornetq.tests.integration.jms.server;

import java.net.URL;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;

import junit.framework.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.DiscoveryGroupConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.deployers.impl.FileDeploymentManager;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerDeployer;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;
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

   private static final Logger log = Logger.getLogger(JMSServerDeployerTest.class);

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
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager);

      String xml = "<configuration xmlns='urn:hornetq'> " + "</configuration>";

      Element rootNode = org.hornetq.utils.XMLUtil.stringToElement(xml);
      deployer.validate(rootNode);
   }

   public void testDeployUnusualQueueNames() throws Exception
   {
      doTestDeployQueuesWithUnusualNames("queue.with.dots.in.name", "/myqueue");

      doTestDeployQueuesWithUnusualNames("queue with spaces in name", "/myqueue2");

      doTestDeployQueuesWithUnusualNames("queue/with/slashes/in/name", "/myqueue3");

      doTestDeployQueuesWithUnusualNames("queue\\with\\backslashes\\in\\name", "/myqueue4");

      doTestDeployQueuesWithUnusualNames("queue with # chars and * chars in name",
                                         "queue with &#35; chars and &#42; chars in name",
                                         "/myqueue5");
   }

   public void testDeployUnusualTopicNames() throws Exception
   {
      doTestDeployTopicsWithUnusualNames("topic.with.dots.in.name", "/mytopic");

      doTestDeployTopicsWithUnusualNames("topic with spaces in name", "/mytopic2");

      doTestDeployTopicsWithUnusualNames("topic/with/slashes/in/name", "/mytopic3");

      doTestDeployTopicsWithUnusualNames("topic\\with\\backslashes\\in\\name", "/mytopic4");

      doTestDeployTopicsWithUnusualNames("topic with # chars and * chars in name",
                                         "topic with &#35; chars and &#42; chars in name",
                                         "/mytopic5");
   }

   private void doTestDeployQueuesWithUnusualNames(final String queueName,
                                                   final String htmlEncodedName,
                                                   final String jndiName) throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager);

      String xml =

      "<queue name=\"" + htmlEncodedName + "\">" + "<entry name=\"" + jndiName + "\"/>" + "</queue>";

      Element rootNode = org.hornetq.utils.XMLUtil.stringToElement(xml);

      deployer.deploy(rootNode);

      Queue queue = (Queue)context.lookup(jndiName);
      Assert.assertNotNull(queue);
      Assert.assertEquals(queueName, queue.getQueueName());
   }

   private void doTestDeployTopicsWithUnusualNames(final String topicName,
                                                   final String htmlEncodedName,
                                                   final String jndiName) throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager);

      String xml =

      "<topic name=\"" + htmlEncodedName + "\">" + "<entry name=\"" + jndiName + "\"/>" + "</topic>";

      Element rootNode = org.hornetq.utils.XMLUtil.stringToElement(xml);

      deployer.deploy(rootNode);

      Topic topic = (Topic)context.lookup(jndiName);
      Assert.assertNotNull(topic);
      Assert.assertEquals(topicName, topic.getTopicName());
   }

   private void doTestDeployQueuesWithUnusualNames(final String queueName, final String jndiName) throws Exception
   {
      doTestDeployQueuesWithUnusualNames(queueName, queueName, jndiName);
   }

   private void doTestDeployTopicsWithUnusualNames(final String topicName, final String jndiName) throws Exception
   {
      doTestDeployTopicsWithUnusualNames(topicName, topicName, jndiName);
   }

   public void testDeployFullConfiguration() throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager);

      String conf = "hornetq-jms-for-JMSServerDeployerTest.xml";
      URL confURL = Thread.currentThread().getContextClassLoader().getResource(conf);

      String[] connectionFactoryBindings = new String[] { "/fullConfigurationConnectionFactory",
                                                         "/acme/fullConfigurationConnectionFactory",
                                                         "java:/xyz/tfullConfigurationConnectionFactory",
                                                         "java:/connectionfactories/acme/fullConfigurationConnectionFactory" };
      String[] queueBindings = new String[] { "/fullConfigurationQueue", "/queue/fullConfigurationQueue" };
      String[] topicBindings = new String[] { "/fullConfigurationTopic", "/topic/fullConfigurationTopic" };

      for (String binding : connectionFactoryBindings)
      {
         UnitTestCase.checkNoBinding(context, binding);
      }
      for (String binding : queueBindings)
      {
         UnitTestCase.checkNoBinding(context, binding);
      }
      for (String binding : topicBindings)
      {
         UnitTestCase.checkNoBinding(context, binding);
      }

      deployer.deploy(confURL);

      for (String binding : connectionFactoryBindings)
      {
         UnitTestCase.checkBinding(context, binding);
      }
      for (String binding : queueBindings)
      {
         UnitTestCase.checkBinding(context, binding);
      }
      for (String binding : topicBindings)
      {
         UnitTestCase.checkBinding(context, binding);
      }

      for (String binding : connectionFactoryBindings)
      {
         HornetQConnectionFactory cf = (HornetQConnectionFactory)context.lookup(binding);
         Assert.assertNotNull(cf);
         Assert.assertEquals(1234, cf.getClientFailureCheckPeriod());
         Assert.assertEquals(5678, cf.getCallTimeout());
         Assert.assertEquals(12345, cf.getConsumerWindowSize());
         Assert.assertEquals(6789, cf.getConsumerMaxRate());
         Assert.assertEquals(123456, cf.getConfirmationWindowSize());
         Assert.assertEquals(7712652, cf.getProducerWindowSize());
         Assert.assertEquals(789, cf.getProducerMaxRate());
         Assert.assertEquals(12, cf.getMinLargeMessageSize());
         Assert.assertEquals("TestClientID", cf.getClientID());
         Assert.assertEquals(3456, cf.getDupsOKBatchSize());
         Assert.assertEquals(4567, cf.getTransactionBatchSize());
         Assert.assertEquals(true, cf.isBlockOnAcknowledge());
         Assert.assertEquals(false, cf.isBlockOnNonDurableSend());
         Assert.assertEquals(true, cf.isBlockOnDurableSend());
         Assert.assertEquals(false, cf.isAutoGroup());
         Assert.assertEquals(true, cf.isPreAcknowledge());
         Assert.assertEquals(2345, cf.getConnectionTTL());
         assertEquals(true, cf.isFailoverOnInitialConnection());
         Assert.assertEquals(34, cf.getReconnectAttempts());
         Assert.assertEquals(5, cf.getRetryInterval());
         Assert.assertEquals(6.0, cf.getRetryIntervalMultiplier());
         Assert.assertEquals(true, cf.isCacheLargeMessagesClient());
      }

      for (String binding : queueBindings)
      {
         Queue queue = (Queue)context.lookup(binding);
         Assert.assertNotNull(queue);
         Assert.assertEquals("fullConfigurationQueue", queue.getQueueName());
      }

      for (String binding : topicBindings)
      {
         Topic topic = (Topic)context.lookup(binding);
         Assert.assertNotNull(topic);
         Assert.assertEquals("fullConfigurationTopic", topic.getTopicName());
      }
   }
   
   public void testDeployFullConfiguration2() throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager);

      String conf = "hornetq-jms-for-JMSServerDeployerTest2.xml";
      URL confURL = Thread.currentThread().getContextClassLoader().getResource(conf);

      String[] connectionFactoryBindings = new String[] { "/fullConfigurationConnectionFactory",
                                                         "/acme/fullConfigurationConnectionFactory",
                                                         "java:/xyz/tfullConfigurationConnectionFactory",
                                                         "java:/connectionfactories/acme/fullConfigurationConnectionFactory" };
      String[] queueBindings = new String[] { "/fullConfigurationQueue", "/queue/fullConfigurationQueue" };
      String[] topicBindings = new String[] { "/fullConfigurationTopic", "/topic/fullConfigurationTopic" };

      for (String binding : connectionFactoryBindings)
      {
         UnitTestCase.checkNoBinding(context, binding);
      }
      for (String binding : queueBindings)
      {
         UnitTestCase.checkNoBinding(context, binding);
      }
      for (String binding : topicBindings)
      {
         UnitTestCase.checkNoBinding(context, binding);
      }

      deployer.deploy(confURL);

      for (String binding : connectionFactoryBindings)
      {
         UnitTestCase.checkBinding(context, binding);
      }
      for (String binding : queueBindings)
      {
         UnitTestCase.checkBinding(context, binding);
      }
      for (String binding : topicBindings)
      {
         UnitTestCase.checkBinding(context, binding);
      }

      for (String binding : connectionFactoryBindings)
      {
         HornetQConnectionFactory cf = (HornetQConnectionFactory)context.lookup(binding);
         Assert.assertNotNull(cf);
         Assert.assertEquals(1234, cf.getClientFailureCheckPeriod());
         Assert.assertEquals(5678, cf.getCallTimeout());
         Assert.assertEquals(12345, cf.getConsumerWindowSize());
         Assert.assertEquals(6789, cf.getConsumerMaxRate());
         Assert.assertEquals(123456, cf.getConfirmationWindowSize());
         Assert.assertEquals(7712652, cf.getProducerWindowSize());
         Assert.assertEquals(789, cf.getProducerMaxRate());
         Assert.assertEquals(12, cf.getMinLargeMessageSize());
         Assert.assertEquals("TestClientID", cf.getClientID());
         Assert.assertEquals(3456, cf.getDupsOKBatchSize());
         Assert.assertEquals(4567, cf.getTransactionBatchSize());
         Assert.assertEquals(true, cf.isBlockOnAcknowledge());
         Assert.assertEquals(false, cf.isBlockOnNonDurableSend());
         Assert.assertEquals(true, cf.isBlockOnDurableSend());
         Assert.assertEquals(false, cf.isAutoGroup());
         Assert.assertEquals(true, cf.isPreAcknowledge());
         Assert.assertEquals(2345, cf.getConnectionTTL());
         assertEquals(true, cf.isFailoverOnInitialConnection());
         Assert.assertEquals(34, cf.getReconnectAttempts());
         Assert.assertEquals(5, cf.getRetryInterval());
         Assert.assertEquals(6.0, cf.getRetryIntervalMultiplier());
         Assert.assertEquals(true, cf.isCacheLargeMessagesClient());
         
         assertEquals("243.7.7.7", cf.getDiscoveryAddress());
         assertEquals("172.16.8.10", cf.getLocalBindAddress());
         assertEquals(12345, cf.getDiscoveryPort());
         assertEquals(5432, cf.getDiscoveryRefreshTimeout());
      }

      for (String binding : queueBindings)
      {
         Queue queue = (Queue)context.lookup(binding);
         Assert.assertNotNull(queue);
         Assert.assertEquals("fullConfigurationQueue", queue.getQueueName());
      }

      for (String binding : topicBindings)
      {
         Topic topic = (Topic)context.lookup(binding);
         Assert.assertNotNull(topic);
         Assert.assertEquals("fullConfigurationTopic", topic.getTopicName());
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      config = new ConfigurationImpl();
      config.getConnectorConfigurations().put("netty",
                                              new TransportConfiguration(NettyConnectorFactory.class.getName()));
      
      DiscoveryGroupConfiguration dcg = new DiscoveryGroupConfiguration("mygroup", "172.16.8.10",
                                                                        "243.7.7.7", 12345,
                                                                        5432, 5432);
      config.getDiscoveryGroupConfigurations().put("mygroup", dcg);
      HornetQServer server = createServer(false, config);

      deploymentManager = new FileDeploymentManager(config.getFileDeployerScanPeriod());

      jmsServer = new JMSServerManagerImpl(server);
      context = new InVMContext();
      jmsServer.setContext(context);
      jmsServer.start();

   }

   @Override
   protected void tearDown() throws Exception
   {
      jmsServer.stop();
      jmsServer = null;
      context = null;
      deploymentManager = null;
      config = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}