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

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.deployers.impl.FileDeploymentManager;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.integration.transports.netty.NettyConnectorFactory;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerDeployer;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.tests.util.ServiceTestBase;
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
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager, config);

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
      
      doTestDeployQueuesWithUnusualNames("queue with # chars and * chars in name", "queue with &#35; chars and &#42; chars in name", "/myqueue5");      
   }
   
   public void testDeployUnusualTopicNames() throws Exception
   {
      doTestDeployTopicsWithUnusualNames("topic.with.dots.in.name", "/mytopic");
      
      doTestDeployTopicsWithUnusualNames("topic with spaces in name", "/mytopic2");
      
      doTestDeployTopicsWithUnusualNames("topic/with/slashes/in/name", "/mytopic3");
      
      doTestDeployTopicsWithUnusualNames("topic\\with\\backslashes\\in\\name", "/mytopic4");
      
      doTestDeployTopicsWithUnusualNames("topic with # chars and * chars in name", "topic with &#35; chars and &#42; chars in name", "/mytopic5");      
   }
   
   private void doTestDeployQueuesWithUnusualNames(final String queueName, String htmlEncodedName, final String jndiName) throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager, config);

      String xml =

      "<queue name=\"" + htmlEncodedName + "\">" + "<entry name=\"" + jndiName + "\"/>" + "</queue>";

      Element rootNode = org.hornetq.utils.XMLUtil.stringToElement(xml);

      deployer.deploy(rootNode);

      Queue queue = (Queue)context.lookup(jndiName);
      assertNotNull(queue);
      assertEquals(queueName, queue.getQueueName());
   }
   
   private void doTestDeployTopicsWithUnusualNames(final String topicName, String htmlEncodedName, final String jndiName) throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager, config);

      String xml =

      "<topic name=\"" + htmlEncodedName + "\">" + "<entry name=\"" + jndiName + "\"/>" + "</topic>";

      Element rootNode = org.hornetq.utils.XMLUtil.stringToElement(xml);

      deployer.deploy(rootNode);

      Topic topic = (Topic)context.lookup(jndiName);
      assertNotNull(topic);
      assertEquals(topicName, topic.getTopicName());
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
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer, deploymentManager, config);

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
         checkNoBinding(context, binding);
      }
      for (String binding : queueBindings)
      {
         checkNoBinding(context, binding);
      }
      for (String binding : topicBindings)
      {
         checkNoBinding(context, binding);
      }

      deployer.deploy(confURL);

      for (String binding : connectionFactoryBindings)
      {
         checkBinding(context, binding);
      }
      for (String binding : queueBindings)
      {
         checkBinding(context, binding);
      }
      for (String binding : topicBindings)
      {
         checkBinding(context, binding);
      }

      for (String binding : connectionFactoryBindings)
      {
         HornetQConnectionFactory cf = (HornetQConnectionFactory)context.lookup(binding);
         assertNotNull(cf);
         assertEquals(1234, cf.getClientFailureCheckPeriod());
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
         assertEquals(34, cf.getReconnectAttempts());
         assertEquals(5, cf.getRetryInterval());
         assertEquals(6.0, cf.getRetryIntervalMultiplier());
      }

      for (String binding : queueBindings)
      {
         Queue queue = (Queue)context.lookup(binding);
         assertNotNull(queue);
         assertEquals("fullConfigurationQueue", queue.getQueueName());
      }

      for (String binding : topicBindings)
      {
         Topic topic = (Topic)context.lookup(binding);
         assertNotNull(topic);
         assertEquals("fullConfigurationTopic", topic.getTopicName());
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