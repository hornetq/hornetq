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

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.deployers.impl.FileDeploymentManager;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.impl.JMSServerDeployer;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.tests.unit.util.InVMContext;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.w3c.dom.Element;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import java.net.URL;

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

      String xml = "<configuration xmlns='urn:jboss:messaging'> " + "</configuration>";

      Element rootNode = org.jboss.messaging.utils.XMLUtil.stringToElement(xml);
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

      Element rootNode = org.jboss.messaging.utils.XMLUtil.stringToElement(xml);

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

      Element rootNode = org.jboss.messaging.utils.XMLUtil.stringToElement(xml);

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

      String conf = "jbm-jms-for-JMSServerDeployerTest.xml";
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
         JBossConnectionFactory cf = (JBossConnectionFactory)context.lookup(binding);
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
      MessagingServer server = createServer(false, config);

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

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}