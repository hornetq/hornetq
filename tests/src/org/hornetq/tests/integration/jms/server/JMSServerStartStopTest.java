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

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSFactoryType;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.spi.core.security.HornetQSecurityManagerImpl;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A JMSServerStartStopTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class JMSServerStartStopTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(JMSServerStartStopTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private JMSServerManager liveJMSServer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testStopStart1() throws Exception
   {
      final int numMessages = 5;

      for (int j = 0; j < numMessages; j++)
      {
         JMSServerStartStopTest.log.info("Iteration " + j);

         start();

         HornetQConnectionFactory jbcf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName()));

         jbcf.setBlockOnDurableSend(true);
         jbcf.setBlockOnNonDurableSend(true);

         Connection conn = jbcf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue queue = sess.createQueue("myJMSQueue");

         MessageProducer producer = sess.createProducer(queue);

         TextMessage tm = sess.createTextMessage("message" + j);

         producer.send(tm);

         conn.close();

         jbcf.close();

         stop();

      }

      start();

      HornetQConnectionFactory jbcf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName()));

      jbcf.setBlockOnDurableSend(true);
      jbcf.setBlockOnNonDurableSend(true);

      Connection conn = jbcf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Queue queue = sess.createQueue("myJMSQueue");

      MessageConsumer consumer = sess.createConsumer(queue);

      conn.start();

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage tm = (TextMessage)consumer.receive(10000);

         Assert.assertNotNull(tm);

         Assert.assertEquals("message" + i, tm.getText());
      }

      conn.close();

      jbcf.close();

      stop();
   }
   
   // https://jira.jboss.org/jira/browse/HORNETQ-315
   public void testCloseConnectionAfterServerIsShutdown() throws Exception
   {
      start();
      
      HornetQConnectionFactory jbcf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName()));

      jbcf.setBlockOnDurableSend(true);
      jbcf.setBlockOnNonDurableSend(true);
      jbcf.setReconnectAttempts(-1);
      
      Connection conn = jbcf.createConnection();
      
      stop();
      
      conn.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
   }

   @Override
   protected void tearDown() throws Exception
   {
      liveJMSServer.stop();
      liveJMSServer = null;
      super.tearDown();
   }

   // Private -------------------------------------------------------

   private void stop() throws Exception
   {
      liveJMSServer.stop();
   }

   private void start() throws Exception
   {
      FileConfiguration fc = new FileConfiguration();

      fc.setConfigurationUrl("server-start-stop-config1.xml");

      fc.start();
      
      fc.setJournalDirectory(getJournalDir());
      fc.setBindingsDirectory(getBindingsDir());
      fc.setLargeMessagesDirectory(getLargeMessagesDir());

      HornetQSecurityManager sm = new HornetQSecurityManagerImpl();

      HornetQServer liveServer = new HornetQServerImpl(fc, sm);

      liveJMSServer = new JMSServerManagerImpl(liveServer, "server-start-stop-jms-config1.xml");

      liveJMSServer.setContext(null);

      liveJMSServer.start();
   }

   // Inner classes -------------------------------------------------

}
