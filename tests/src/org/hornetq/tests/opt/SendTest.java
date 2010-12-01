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

package org.hornetq.tests.opt;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.JournalType;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.jms.client.HornetQSession;
import org.hornetq.jms.server.impl.JMSFactoryType;
import org.hornetq.tests.util.RandomUtil;

/**
 * A SendTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class SendTest
{
   private static final Logger log = Logger.getLogger(SendTest.class);

   public static void main(final String[] args)
   {
      try
      {
         new SendTest().runRouteBlast();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   private HornetQServer server;

   private void startServer() throws Exception
   {
      SendTest.log.info("*** Starting server");

      // System.setProperty("org.hornetq.opt.dontadd", "true");
      // System.setProperty("org.hornetq.opt.routeblast", "true");
      // System.setProperty("org.hornetq.opt.generatemessages", "true");
      System.setProperty("org.hornetq.opt.directblast", "true");

      Configuration configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setJMXManagementEnabled(false);
      configuration.setJournalMinFiles(10);

      configuration.setPersistenceEnabled(true);
      configuration.setFileDeploymentEnabled(false);
      // configuration.setJournalFlushOnSync(true);
      // configuration.setRunSyncSpeedTest(true);
      // configuration.setJournalPerfBlastPages(10000);

      configuration.setJournalType(JournalType.ASYNCIO);

      configuration.setJournalBufferTimeout_NIO(1000000000 / 100); // this is in nanoseconds
      configuration.setJournalBufferSize_NIO(490 * 1024);
      configuration.setJournalMaxIO_NIO(1);

      configuration.setJournalBufferTimeout_AIO(1000000000 / 1000); // this is in nanoseconds
      configuration.setJournalBufferSize_AIO(490 * 1024);
      configuration.setJournalMaxIO_AIO(500);

      configuration.setLogJournalWriteRate(true);

      // configuration.setRunSyncSpeedTest(true);

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.USE_NIO_PROP_NAME, Boolean.FALSE);

      TransportConfiguration transportConfig1 = new TransportConfiguration(NettyAcceptorFactory.class.getCanonicalName(),
                                                                           params);
      TransportConfiguration transportConfig2 = new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName(),
                                                                           null);
      configuration.getAcceptorConfigurations().add(transportConfig1);
      configuration.getAcceptorConfigurations().add(transportConfig2);

      server = HornetQServers.newHornetQServer(configuration);

      server.start();

      SendTest.log.info("Started server");

   }

   public void runRouteBlast() throws Exception
   {
      startServer();
   }

   public void runTextMessage() throws Exception
   {
      startServer();

      Map<String, Object> params = new HashMap<String, Object>();

      // params.put(TransportConstants.HOST_PROP_NAME, "localhost");

      // params.put(TransportConstants.PORT_PROP_NAME, 5445);

      params.put(TransportConstants.TCP_NODELAY_PROPNAME, Boolean.FALSE);
      // params.put(TransportConstants.USE_NIO_PROP_NAME, Boolean.FALSE);

      TransportConfiguration tc = new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName(), params);

      // TransportConfiguration tc = new TransportConfiguration(InVMConnectorFactory.class.getCanonicalName(), params);

      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, tc);

      cf.setProducerWindowSize(1024 * 1024);

      Connection conn = cf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession coreSession = ((HornetQSession)sess).getCoreSession();

      coreSession.createQueue("jms.queue.test_queue", "jms.queue.test_queue");

      Queue queue = HornetQJMSClient.createQueue("test_queue");

      MessageProducer prod = sess.createProducer(queue);

      prod.setDisableMessageID(true);

      prod.setDisableMessageTimestamp(true);

      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      byte[] bytes1 = new byte[] { (byte)'A', (byte)'B', (byte)'C', (byte)'D' };

      String s = new String(bytes1);

      System.out.println("Str is " + s);

      byte[] bytes = RandomUtil.randomBytes(512);

      String str = new String(bytes);

      final int warmup = 500000;

      SendTest.log.info("Warming up");

      TextMessage tm = sess.createTextMessage();

      tm.setText(str);

      for (int i = 0; i < warmup; i++)
      {
         prod.send(tm);

         if (i % 10000 == 0)
         {
            SendTest.log.info("sent " + i);
         }
      }

      SendTest.log.info("** WARMUP DONE");

      final int numMessages = 2000000;

      tm = sess.createTextMessage();

      tm.setText(str);

      long start = System.currentTimeMillis();

      for (int i = 0; i < numMessages; i++)
      {
         prod.send(tm);

         if (i % 10000 == 0)
         {
            SendTest.log.info("sent " + i);
         }
      }

      sess.close();

      long end = System.currentTimeMillis();

      double rate = 1000 * (double)numMessages / (end - start);

      System.out.println("Rate of " + rate + " msgs / sec");

      server.stop();
   }

   public void runSendConsume() throws Exception
   {
      startServer();

      Map<String, Object> params = new HashMap<String, Object>();

      // params.put(TransportConstants.HOST_PROP_NAME, "localhost");

      // params.put(TransportConstants.PORT_PROP_NAME, 5445);

      params.put(TransportConstants.TCP_NODELAY_PROPNAME, Boolean.FALSE);
      // params.put(TransportConstants.USE_NIO_PROP_NAME, Boolean.FALSE);

      TransportConfiguration tc = new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName(), params);

      // TransportConfiguration tc = new TransportConfiguration(InVMConnectorFactory.class.getCanonicalName(), params);

      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, tc);

      cf.setProducerWindowSize(1024 * 1024);

      Connection conn = cf.createConnection();

      Session sess = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);

      ClientSession coreSession = ((HornetQSession)sess).getCoreSession();

      coreSession.createQueue("jms.queue.test_queue", "jms.queue.test_queue");

      Queue queue = HornetQJMSClient.createQueue("test_queue");

      MessageConsumer cons = sess.createConsumer(queue);

      conn.start();

      final CountDownLatch latch = new CountDownLatch(1);

      final int warmup = 500000;

      final int numMessages = 2000000;

      MessageListener listener = new MessageListener()
      {
         int count;

         public void onMessage(final Message message)
         {
            count++;

            if (count % 10000 == 0)
            {
               SendTest.log.info("received " + count);
            }

            if (count == numMessages + warmup)
            {
               latch.countDown();
            }
         }
      };

      cons.setMessageListener(listener);

      MessageProducer prod = sess.createProducer(queue);

      prod.setDisableMessageID(true);

      prod.setDisableMessageTimestamp(true);

      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      byte[] bytes = RandomUtil.randomBytes(1024);

      String str = new String(bytes);

      SendTest.log.info("Warming up");

      for (int i = 0; i < warmup; i++)
      {
         TextMessage tm = sess.createTextMessage();

         tm.setText(str);

         prod.send(tm);

         if (i % 10000 == 0)
         {
            SendTest.log.info("sent " + i);
         }
      }

      SendTest.log.info("** WARMUP DONE");

      long start = System.currentTimeMillis();

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage tm = sess.createTextMessage();

         tm.setText(str);
         prod.send(tm);

         if (i % 10000 == 0)
         {
            SendTest.log.info("sent " + i);
         }
      }

      latch.countDown();

      sess.close();

      long end = System.currentTimeMillis();

      double rate = 1000 * (double)numMessages / (end - start);

      System.out.println("Rate of " + rate + " msgs / sec");

      server.stop();
   }

   public void runObjectMessage() throws Exception
   {
      startServer();

      Map<String, Object> params = new HashMap<String, Object>();

      // params.put(TransportConstants.HOST_PROP_NAME, "localhost");

      // params.put(TransportConstants.PORT_PROP_NAME, 5445);

      params.put(TransportConstants.TCP_NODELAY_PROPNAME, Boolean.FALSE);
      // params.put(TransportConstants.USE_NIO_PROP_NAME, Boolean.FALSE);

      TransportConfiguration tc = new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName(), params);

      // TransportConfiguration tc = new TransportConfiguration(InVMConnectorFactory.class.getCanonicalName(), params);

      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, tc);

      cf.setProducerWindowSize(1024 * 1024);

      Connection conn = cf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession coreSession = ((HornetQSession)sess).getCoreSession();

      coreSession.createQueue("jms.queue.test_queue", "jms.queue.test_queue");

      Queue queue = HornetQJMSClient.createQueue("test_queue");

      MessageProducer prod = sess.createProducer(queue);

      prod.setDisableMessageID(true);

      prod.setDisableMessageTimestamp(true);

      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      byte[] bytes = RandomUtil.randomBytes(512);

      String str = new String(bytes);

      SendTest.log.info("str length " + str.length());

      final int warmup = 50000;

      SendTest.log.info("sending messages");

      for (int i = 0; i < warmup; i++)
      {
         // ObjectMessage om = sess.createObjectMessage(str);
         //         
         // prod.send(om);

         TextMessage tm = sess.createTextMessage(str);

         prod.send(tm);

         if (i % 10000 == 0)
         {
            SendTest.log.info("sent " + i);
         }

         // om.setObject(str);
      }

      SendTest.log.info("** WARMUP DONE");

      final int numMessages = 500000;

      long start = System.currentTimeMillis();

      for (int i = 0; i < numMessages; i++)
      {
         // ObjectMessage om = sess.createObjectMessage(str);
         //         
         // prod.send(om);

         TextMessage tm = sess.createTextMessage(str);

         prod.send(tm);

         if (i % 10000 == 0)
         {
            SendTest.log.info("sent " + i);
         }

         // om.setObject(str);
      }

      long end = System.currentTimeMillis();

      double rate = 1000 * (double)numMessages / (end - start);

      System.out.println("Rate of " + rate + " msgs / sec");

      server.stop();
   }

   public void runConsume() throws Exception
   {
      startServer();

      Map<String, Object> params = new HashMap<String, Object>();

      // params.put(TransportConstants.HOST_PROP_NAME, "localhost");

      // params.put(TransportConstants.PORT_PROP_NAME, 5445);

      params.put(TransportConstants.TCP_NODELAY_PROPNAME, Boolean.FALSE);
      // params.put(TransportConstants.USE_NIO_PROP_NAME, Boolean.FALSE);

      TransportConfiguration tc = new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName(), params);

      // TransportConfiguration tc = new TransportConfiguration(InVMConnectorFactory.class.getCanonicalName(), params);

      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, tc);

      Connection conn = cf.createConnection();

      Session sess = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);

      ClientSession coreSession = ((HornetQSession)sess).getCoreSession();

      coreSession.createQueue("jms.queue.test_queue", "jms.queue.test_queue");

      Queue queue = HornetQJMSClient.createQueue("test_queue");

      MessageConsumer cons = sess.createConsumer(queue);

      final CountDownLatch latch = new CountDownLatch(1);

      final int warmup = 50000;

      final int numMessages = 2000000;

      MessageListener listener = new MessageListener()
      {
         int count;

         long start;

         public void onMessage(final Message message)
         {
            count++;

            SendTest.log.info("got message " + ((HornetQMessage)message).getCoreMessage().getMessageID());

            if (count == warmup)
            {
               SendTest.log.info("** WARMED UP");

               start = System.currentTimeMillis();
            }

            if (count % 10000 == 0)
            {
               SendTest.log.info("received " + count);
            }

            if (count == numMessages + warmup)
            {
               long end = System.currentTimeMillis();

               double rate = 1000 * (double)numMessages / (end - start);

               System.out.println("Rate of " + rate + " msgs / sec");

               latch.countDown();
            }
         }
      };

      cons.setMessageListener(listener);

      MessageProducer prod = sess.createProducer(queue);

      prod.setDisableMessageID(true);

      prod.setDisableMessageTimestamp(true);

      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      byte[] bytes = RandomUtil.randomBytes(1);

      String str = new String(bytes);

      // Load up the queue with messages

      TextMessage tm = sess.createTextMessage();

      tm.setText(str);

      SendTest.log.info("loading queue with messages");

      for (int i = 0; i < numMessages + warmup; i++)
      {
         prod.send(tm);

         if (i % 10000 == 0)
         {
            SendTest.log.info("sent " + i);
         }
      }

      SendTest.log.info("** loaded queue");

      conn.start();

      latch.await();

      sess.close();

      server.stop();
   }

}
