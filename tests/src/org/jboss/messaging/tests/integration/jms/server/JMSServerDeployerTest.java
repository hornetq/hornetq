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

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.impl.JMSServerDeployer;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.tests.unit.util.InVMContext;
import org.jboss.messaging.tests.util.ServiceTestBase;
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

   private MessagingServer server;

   private JMSServerManager jmsServer;

   private Context context;

   // Public --------------------------------------------------------

   public void testValidateEmptyConfiguration() throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer,
                                                         server.getDeploymentManager(),
                                                         server.getConfiguration());

      String xml = "<deployment xmlns='urn:jboss:messaging'> " + "</deployment>";

      Element rootNode = org.jboss.messaging.utils.XMLUtil.stringToElement(xml);
      deployer.validate(rootNode);
   }

   public void testDeployFullConfiguration() throws Exception
   {
      JMSServerDeployer deployer = new JMSServerDeployer(jmsServer,
                                                         server.getDeploymentManager(),
                                                         server.getConfiguration());

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

      Configuration conf = new ConfigurationImpl();
      conf.getConnectorConfigurations().put("netty", new TransportConfiguration(NettyConnectorFactory.class.getName()));
      server = createServer(false, conf);
      server.start();

      jmsServer = new JMSServerManagerImpl(server);
      jmsServer.start();

      context = new InVMContext();
      jmsServer.setContext(context);
   }

   @Override
   protected void tearDown() throws Exception
   {
      jmsServer.stop();
      server.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
