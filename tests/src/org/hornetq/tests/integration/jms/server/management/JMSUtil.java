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

package org.hornetq.tests.integration.jms.server.management;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.impl.ClusterManagerImpl;
import org.hornetq.jms.client.HornetQConnection;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQJMSConnectionFactory;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.RandomUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A JMSUtil
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *         <p/>
 *         Created 14 nov. 2008 13:48:08
 */
public class JMSUtil
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static Connection createConnection(final String connectorFactory) throws JMSException
   {
      HornetQJMSConnectionFactory cf = (HornetQJMSConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                                                      new TransportConfiguration(connectorFactory));

      cf.setBlockOnNonDurableSend(true);
      cf.setBlockOnDurableSend(true);
      cf.setBlockOnAcknowledge(true);

      return cf.createConnection();
   }

   public static ConnectionFactory createFactory(final String connectorFactory,
                                                 final long connectionTTL,
                                                 final long clientFailureCheckPeriod) throws JMSException
   {
      HornetQJMSConnectionFactory cf = (HornetQJMSConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                                                      new TransportConfiguration(connectorFactory));

      cf.setBlockOnNonDurableSend(true);
      cf.setBlockOnDurableSend(true);
      cf.setBlockOnAcknowledge(true);
      cf.setConnectionTTL(connectionTTL);
      cf.setClientFailureCheckPeriod(clientFailureCheckPeriod);

      return cf;
   }

   static MessageConsumer createConsumer(final Connection connection, final Destination destination) throws JMSException
   {
      return createConsumer(connection, destination, Session.AUTO_ACKNOWLEDGE);
   }

   static MessageConsumer createConsumer(final Connection connection, final Destination destination, int ackMode) throws JMSException
   {
      Session s = connection.createSession(false, ackMode);

      return s.createConsumer(destination);
   }

   static TopicSubscriber createDurableSubscriber(final Connection connection,
                                                  final Topic topic,
                                                  final String clientID,
                                                  final String subscriptionName) throws JMSException
   {
      return createDurableSubscriber(connection, topic, clientID, subscriptionName, Session.AUTO_ACKNOWLEDGE);
   }

   static TopicSubscriber createDurableSubscriber(final Connection connection,
                                                  final Topic topic,
                                                  final String clientID,
                                                  final String subscriptionName,
                                                  final int ackMode) throws JMSException
   {
      connection.setClientID(clientID);
      Session s = connection.createSession(false, ackMode);

      return s.createDurableSubscriber(topic, subscriptionName);
   }

   public static String[] sendMessages(final Destination destination, final int messagesToSend) throws Exception
   {
      HornetQJMSConnectionFactory cf = (HornetQJMSConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                                                      new TransportConfiguration(InVMConnectorFactory.class.getName()));
      return JMSUtil.sendMessages(cf, destination, messagesToSend);
   }

   public static String[] sendMessages(final ConnectionFactory cf,
                                       final Destination destination,
                                       final int messagesToSend) throws Exception
   {
      String[] messageIDs = new String[messagesToSend];

      Connection conn = cf.createConnection();

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = s.createProducer(destination);

      for (int i = 0; i < messagesToSend; i++)
      {
         Message m = s.createTextMessage(RandomUtil.randomString());
         producer.send(m);
         messageIDs[i] = m.getJMSMessageID();
      }

      conn.close();

      return messageIDs;
   }

   public static Message sendMessageWithProperty(final Session session,
                                                 final Destination destination,
                                                 final String key,
                                                 final long value) throws JMSException
   {
      MessageProducer producer = session.createProducer(destination);
      Message message = session.createMessage();
      message.setLongProperty(key, value);
      producer.send(message);
      return message;
   }

   public static void consumeMessages(final int expected, final Destination dest) throws JMSException
   {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      try
      {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(dest);

         connection.start();

         Message m = null;
         for (int i = 0; i < expected; i++)
         {
            m = consumer.receive(500);
            Assert.assertNotNull("expected to received " + expected + " messages, got only " + (i + 1), m);
         }
         m = consumer.receiveNoWait();
         Assert.assertNull("received one more message than expected (" + expected + ")", m);
      }
      finally
      {
         if (connection != null)
         {
            connection.close();
         }
      }
   }

   public static void waitForServer(HornetQServer server) throws InterruptedException
   {
      long timetowait = System.currentTimeMillis() + 5000;
      while (!server.isStarted())
      {
         Thread.sleep(100);
         if (server.isStarted())
         {
            break;
         }
         else if (System.currentTimeMillis() > timetowait)
         {
            throw new IllegalStateException("server didnt start");
         }
      }
   }

   public static void crash(HornetQServer server, ClientSession... sessions) throws Exception
   {
      final CountDownLatch latch = new CountDownLatch(sessions.length);

      class MyListener implements SessionFailureListener
      {
         public void connectionFailed(final HornetQException me, boolean failedOver)
         {
            latch.countDown();
         }

         public void beforeReconnect(HornetQException exception)
         {
            System.out.println("MyListener.beforeReconnect");
         }
      }
      for (ClientSession session : sessions)
      {
         session.addFailureListener(new MyListener());
      }

      ClusterManagerImpl clusterManager = (ClusterManagerImpl)server.getClusterManager();
      clusterManager.clear();
      server.stop(true);

      // Wait to be informed of failure
      boolean ok = latch.await(10000, TimeUnit.MILLISECONDS);

      Assert.assertTrue(ok);
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   public static HornetQConnection createConnectionAndWaitForTopology(HornetQConnectionFactory factory,
                                                                      int topologyMembers,
                                                                      int timeout) throws Exception
   {
      HornetQConnection conn;
      CountDownLatch countDownLatch = new CountDownLatch(topologyMembers);

      ServerLocator locator = factory.getServerLocator();

      locator.addClusterTopologyListener(new LatchClusterTopologyListener(countDownLatch));

      conn = (HornetQConnection)factory.createConnection();

      boolean ok = countDownLatch.await(timeout, TimeUnit.SECONDS);
      if (!ok)
      {
         throw new IllegalStateException("timed out waiting for topology");
      }
      return conn;
   }

   static class LatchClusterTopologyListener implements ClusterTopologyListener
   {
      final CountDownLatch latch;

      int liveNodes = 0;

      int backUpNodes = 0;

      List<String> liveNode = new ArrayList<String>();

      List<String> backupNode = new ArrayList<String>();

      public LatchClusterTopologyListener(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void nodeUP(final long uniqueEventID, String nodeID,
                         Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                         boolean last)
      {
         if (connectorPair.getA() != null && !liveNode.contains(connectorPair.getA().getName()))
         {
            liveNode.add(connectorPair.getA().getName());
            latch.countDown();
         }
         if (connectorPair.getB() != null && !backupNode.contains(connectorPair.getB().getName()))
         {
            backupNode.add(connectorPair.getB().getName());
            latch.countDown();
         }
      }

      public void nodeDown(final long uniqueEventID, String nodeID)
      {
         // To change body of implemented methods use File | Settings | File Templates.
      }
   }
}
