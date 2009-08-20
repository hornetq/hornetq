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

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.security.HornetQSecurityManager;
import org.hornetq.core.security.impl.HornetQSecurityManagerImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.integration.transports.netty.NettyConnectorFactory;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A JMSServerStartStopWithReplicationTest
 * 
 * Make sure live backup pair can be stopped and started ok multiple times with predefined queues etc
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class JMSServerStartStopWithReplicationTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(JMSServerStartStopWithReplicationTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   private JMSServerManager liveJMSServer;

   private JMSServerManager backupJMSServer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testStopStartBackupBeforeLive() throws Exception
   {
      testStopStart1(true);
   }

   public void testStopStartLiveBeforeBackup() throws Exception
   {
      testStopStart1(false);
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
      this.liveJMSServer = null;
      this.backupJMSServer = null;
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   private void testStopStart1(final boolean backupBeforeLive) throws Exception
   {
      final int numMessages = 5;

      for (int j = 0; j < numMessages; j++)
      {
         log.info("Iteration " + j);

         startBackup();
         startLive();

         HornetQConnectionFactory jbcf = new HornetQConnectionFactory(new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName()),
                                                                  new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName(),
                                                                                             backupParams));

         jbcf.setBlockOnPersistentSend(true);
         jbcf.setBlockOnNonPersistentSend(true);

         Connection conn = jbcf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue queue = sess.createQueue("myJMSQueue");

         MessageProducer producer = sess.createProducer(queue);

         TextMessage tm = sess.createTextMessage("message" + j);

         producer.send(tm);

         conn.close();

         jbcf.close();

         if (backupBeforeLive)
         {
            stopBackup();
            stopLive();
         }
         else
         {
            stopLive();
            stopBackup();
         }
      }

      startBackup();
      startLive();

      HornetQConnectionFactory jbcf = new HornetQConnectionFactory(new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName()),
                                                               new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName(),
                                                                                          backupParams));

      jbcf.setBlockOnPersistentSend(true);
      jbcf.setBlockOnNonPersistentSend(true);

      Connection conn = jbcf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Queue queue = sess.createQueue("myJMSQueue");

      MessageConsumer consumer = sess.createConsumer(queue);

      conn.start();

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage tm = (TextMessage)consumer.receive(1000);

         assertNotNull(tm);

         assertEquals("message" + i, tm.getText());
      }

      conn.close();

      jbcf.close();

      if (backupBeforeLive)
      {
         stopBackup();
         stopLive();
      }
      else
      {
         stopLive();
         stopBackup();
      }
   }

   private void stopLive() throws Exception
   {
      liveJMSServer.stop();
   }

   private void stopBackup() throws Exception
   {
      backupJMSServer.stop();
   }

   private void startLive() throws Exception
   {
      FileConfiguration fcLive = new FileConfiguration();

      fcLive.setConfigurationUrl("server-start-stop-live-config1.xml");

      fcLive.start();

      HornetQSecurityManager smLive = new HornetQSecurityManagerImpl();

      HornetQServer liveServer = new HornetQServerImpl(fcLive, smLive);

      liveJMSServer = new JMSServerManagerImpl(liveServer, "server-start-stop-live-jms-config1.xml");

      liveJMSServer.setContext(null);

      liveJMSServer.start();
   }

   private void startBackup() throws Exception
   {
      FileConfiguration fcBackup = new FileConfiguration();

      fcBackup.setConfigurationUrl("server-start-stop-backup-config1.xml");

      fcBackup.start();

      HornetQSecurityManager smBackup = new HornetQSecurityManagerImpl();

      HornetQServer liveServer = new HornetQServerImpl(fcBackup, smBackup);

      backupJMSServer = new JMSServerManagerImpl(liveServer, "server-start-stop-backup-jms-config1.xml");

      backupJMSServer.setContext(null);

      backupJMSServer.start();
   }

   // Inner classes -------------------------------------------------

}
