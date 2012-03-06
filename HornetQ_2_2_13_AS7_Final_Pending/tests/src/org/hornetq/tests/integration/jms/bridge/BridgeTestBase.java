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
package org.hornetq.tests.integration.jms.bridge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.XAConnectionFactory;
import javax.transaction.TransactionManager;

import junit.framework.Assert;

import com.arjuna.ats.arjuna.coordinator.TransactionReaper;
import com.arjuna.ats.arjuna.coordinator.TxControl;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.api.jms.management.JMSQueueControl;
import org.hornetq.api.jms.management.TopicControl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.jms.bridge.ConnectionFactoryFactory;
import org.hornetq.jms.bridge.DestinationFactory;
import org.hornetq.jms.bridge.QualityOfServiceMode;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQJMSConnectionFactory;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.jms.client.HornetQXAConnectionFactory;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A BridgeTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public abstract class BridgeTestBase extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(BridgeTestBase.class);

   protected ConnectionFactoryFactory cff0, cff1;

   protected ConnectionFactoryFactory cff0xa, cff1xa;

   protected ConnectionFactory cf0, cf1;

   protected XAConnectionFactory cf0xa, cf1xa;

   protected DestinationFactory sourceQueueFactory, targetQueueFactory, localTargetQueueFactory, sourceTopicFactory;

   protected Queue sourceQueue, targetQueue, localTargetQueue;

   protected Topic sourceTopic;

   protected HornetQServer server0;

   protected JMSServerManager jmsServer0;

   protected HornetQServer server1;

   protected JMSServerManager jmsServer1;

   private InVMContext context0;

   protected InVMContext context1;

   private HashMap<String, Object> params1;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      // Start the servers
      Configuration conf0 = createBasicConfig();
      conf0.setJournalDirectory(getJournalDir(0, false));
      conf0.setBindingsDirectory(getBindingsDir(0, false));
      conf0.setSecurityEnabled(false);
      conf0.getAcceptorConfigurations()
           .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      server0 = HornetQServers.newHornetQServer(conf0, false);

      context0 = new InVMContext();
      jmsServer0 = new JMSServerManagerImpl(server0);
      jmsServer0.setContext(context0);
      jmsServer0.start();

      Configuration conf1 = createBasicConfig();
      conf1.setSecurityEnabled(false);
      conf1.setJournalDirectory(getJournalDir(1, false));
      conf1.setBindingsDirectory(getBindingsDir(1, false));
      params1 = new HashMap<String, Object>();
      params1.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      conf1.getAcceptorConfigurations()
           .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory", params1));

      server1 = HornetQServers.newHornetQServer(conf1, false);

      context1 = new InVMContext();

      jmsServer1 = new JMSServerManagerImpl(server1);
      jmsServer1.setContext(context1);
      jmsServer1.start();

      createQueue("sourceQueue", 0);

      jmsServer0.createTopic(false, "sourceTopic", "/topic/sourceTopic");

      createQueue("localTargetQueue", 0);

      createQueue("targetQueue", 1);

      setUpAdministeredObjects();

      // We need a local transaction and recovery manager
      // We must start this after the remote servers have been created or it won't
      // have deleted the database and the recovery manager may attempt to recover transactions

   }

   protected void createQueue(final String queueName, final int index) throws Exception
   {
      JMSServerManager server = jmsServer0;
      if (index == 1)
      {
         server = jmsServer1;
      }
      server.createQueue(false, queueName, null, true, "/queue/" + queueName);
   }

   @Override
   protected void tearDown() throws Exception
   {
      checkEmpty(sourceQueue, 0);
      checkEmpty(localTargetQueue, 0);
      checkEmpty(targetQueue, 1);

      // Check no subscriptions left lying around

      checkNoSubscriptions(sourceTopic, 0);

      jmsServer0.stop();

      jmsServer1.stop();

      server1.stop();

      server0.stop();

      cff0 = cff1 = null;

      cff0xa = cff1xa = null;

      cf0 = cf1 = null;

      cf0xa = cf1xa = null;

      sourceQueueFactory = targetQueueFactory = localTargetQueueFactory = sourceTopicFactory = null;

      sourceQueue = targetQueue = localTargetQueue = null;

      sourceTopic = null;

      server0 = null;

      jmsServer0 = null;

      server1 = null;

      jmsServer1 = null;

      context0 = null;

      context1 = null;
      
      // Shutting down Arjuna threads
      TxControl.disable(true);
      
      TransactionReaper.terminate(false);

      super.tearDown();
   }

   protected void setUpAdministeredObjects() throws Exception
   {
      cff0 = new ConnectionFactoryFactory()
      {
         public ConnectionFactory createConnectionFactory() throws Exception
         {
            HornetQConnectionFactory cf = (HornetQConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                                                      new TransportConfiguration(InVMConnectorFactory.class.getName()));

            // Note! We disable automatic reconnection on the session factory. The bridge needs to do the reconnection
            cf.setReconnectAttempts(0);
            cf.setBlockOnNonDurableSend(true);
            cf.setBlockOnDurableSend(true);
            cf.setCacheLargeMessagesClient(true);

            return (ConnectionFactory)cf;
         }

      };

      cff0xa = new ConnectionFactoryFactory()
      {
         public Object createConnectionFactory() throws Exception
         {
            HornetQXAConnectionFactory cf = (HornetQXAConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.XA_CF,
                                                                                                                          new TransportConfiguration(InVMConnectorFactory.class.getName()));

            // Note! We disable automatic reconnection on the session factory. The bridge needs to do the reconnection
            cf.setReconnectAttempts(0);
            cf.setBlockOnNonDurableSend(true);
            cf.setBlockOnDurableSend(true);
            cf.setCacheLargeMessagesClient(true);

            return cf;
         }

      };

      cf0 = (ConnectionFactory)cff0.createConnectionFactory();
      cf0xa = (XAConnectionFactory)cff0xa.createConnectionFactory();

      cff1 = new ConnectionFactoryFactory()
      {

         public ConnectionFactory createConnectionFactory() throws Exception
         {
            HornetQJMSConnectionFactory cf = (HornetQJMSConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                                                            new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                                                                                                       params1));

            // Note! We disable automatic reconnection on the session factory. The bridge needs to do the reconnection
            cf.setReconnectAttempts(0);
            cf.setBlockOnNonDurableSend(true);
            cf.setBlockOnDurableSend(true);
            cf.setCacheLargeMessagesClient(true);

            return cf;
         }
      };

      cff1xa = new ConnectionFactoryFactory()
      {

         public XAConnectionFactory createConnectionFactory() throws Exception
         {
            HornetQXAConnectionFactory cf = (HornetQXAConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.XA_CF,
                                                                                                                          new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                                                                                                     params1));

            // Note! We disable automatic reconnection on the session factory. The bridge needs to do the reconnection
            cf.setReconnectAttempts(0);
            cf.setBlockOnNonDurableSend(true);
            cf.setBlockOnDurableSend(true);
            cf.setCacheLargeMessagesClient(true);

            return cf;
         }
      };

      cf1 = (ConnectionFactory)cff1.createConnectionFactory();
      cf1xa = (XAConnectionFactory)cff1xa.createConnectionFactory();

      sourceQueueFactory = new DestinationFactory()
      {
         public Destination createDestination() throws Exception
         {
            return (Destination)context0.lookup("/queue/sourceQueue");
         }
      };

      sourceQueue = (Queue)sourceQueueFactory.createDestination();

      targetQueueFactory = new DestinationFactory()
      {
         public Destination createDestination() throws Exception
         {
            return (Destination)context1.lookup("/queue/targetQueue");
         }
      };

      targetQueue = (Queue)targetQueueFactory.createDestination();

      sourceTopicFactory = new DestinationFactory()
      {
         public Destination createDestination() throws Exception
         {
            return (Destination)context0.lookup("/topic/sourceTopic");
         }
      };

      sourceTopic = (Topic)sourceTopicFactory.createDestination();

      localTargetQueueFactory = new DestinationFactory()
      {
         public Destination createDestination() throws Exception
         {
            return (Destination)context0.lookup("/queue/localTargetQueue");
         }
      };

      localTargetQueue = (Queue)localTargetQueueFactory.createDestination();
   }

   protected void sendMessages(final ConnectionFactory cf,
                               final Destination dest,
                               final int start,
                               final int numMessages,
                               final boolean persistent,
                               final boolean largeMessage) throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(dest);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         for (int i = start; i < start + numMessages; i++)
         {
            if (largeMessage)
            {
               BytesMessage msg = sess.createBytesMessage();
               ((HornetQMessage)msg).setInputStream(UnitTestCase.createFakeLargeStream(1024l * 1024l));
               msg.setStringProperty("msg", "message" + i);
               prod.send(msg);
            }
            else
            {
               TextMessage tm = sess.createTextMessage("message" + i);
               prod.send(tm);
            }

         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   protected void checkMessagesReceived(final ConnectionFactory cf,
                                        final Destination dest,
                                        final QualityOfServiceMode qosMode,
                                        final int numMessages,
                                        final boolean longWaitForFirst,
                                        final boolean largeMessage) throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         conn.start();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = sess.createConsumer(dest);

         // Consume the messages

         Set<String> msgs = new HashSet<String>();

         int count = 0;

         // We always wait longer for the first one - it may take some time to arrive especially if we are
         // waiting for recovery to kick in
         while (true)
         {
            Message tm = cons.receive(count == 0 ? (longWaitForFirst ? 60000 : 10000) : 5000);

            if (tm == null)
            {
               break;
            }

            // log.info("Got message " + tm.getText());

            if (largeMessage)
            {
               BytesMessage bmsg = (BytesMessage)tm;
               msgs.add(tm.getStringProperty("msg"));
               byte buffRead[] = new byte[1024];
               for (int i = 0; i < 1024; i++)
               {
                  Assert.assertEquals(1024, bmsg.readBytes(buffRead));
               }
            }
            else
            {
               msgs.add(((TextMessage)tm).getText());
            }

            count++;

         }

         if (qosMode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE || qosMode == QualityOfServiceMode.DUPLICATES_OK)
         {
            // All the messages should be received

            for (int i = 0; i < numMessages; i++)
            {
               Assert.assertTrue("" + i, msgs.contains("message" + i));
            }

            // Should be no more
            if (qosMode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE)
            {
               Assert.assertEquals(numMessages, msgs.size());
            }
         }
         else if (qosMode == QualityOfServiceMode.AT_MOST_ONCE)
         {
            // No *guarantee* that any messages will be received
            // but you still might get some depending on how/where the crash occurred
         }

         BridgeTestBase.log.trace("Check complete");

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   protected void checkAllMessageReceivedInOrder(final ConnectionFactory cf,
                                                 final Destination dest,
                                                 final int start,
                                                 final int numMessages,
                                                 final boolean largeMessage) throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection();

         conn.start();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = sess.createConsumer(dest);

         // Consume the messages

         for (int i = 0; i < numMessages; i++)
         {
            Message tm = cons.receive(30000);

            Assert.assertNotNull(tm);

            if (largeMessage)
            {
               BytesMessage bmsg = (BytesMessage)tm;
               Assert.assertEquals("message" + (i + start), tm.getStringProperty("msg"));
               byte buffRead[] = new byte[1024];
               for (int j = 0; j < 1024; j++)
               {
                  Assert.assertEquals(1024, bmsg.readBytes(buffRead));
               }
            }
            else
            {
               Assert.assertEquals("message" + (i + start), ((TextMessage)tm).getText());
            }
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public boolean checkEmpty(final Queue queue, final int index) throws Exception
   {
      ManagementService managementService = server0.getManagementService();
      if (index == 1)
      {
         managementService = server1.getManagementService();
      }
      JMSQueueControl queueControl = (JMSQueueControl)managementService.getResource(ResourceNames.JMS_QUEUE + queue.getQueueName());

      Long messageCount = queueControl.getMessageCount();

      if (messageCount > 0)
      {
         queueControl.removeMessages(null);
      }
      return true;
   }

   protected void checkNoSubscriptions(final Topic topic, final int index) throws Exception
   {
      ManagementService managementService = server0.getManagementService();
      if (index == 1)
      {
         managementService = server1.getManagementService();
      }
      TopicControl topicControl = (TopicControl)managementService.getResource(ResourceNames.JMS_TOPIC + topic.getTopicName());
      Assert.assertEquals(0, topicControl.getSubscriptionCount());

   }

   protected void removeAllMessages(final String queueName, final int index) throws Exception
   {
      ManagementService managementService = server0.getManagementService();
      if (index == 1)
      {
         managementService = server1.getManagementService();
      }
      JMSQueueControl queueControl = (JMSQueueControl)managementService.getResource(ResourceNames.JMS_QUEUE + queueName);
      queueControl.removeMessages(null);
   }

   protected TransactionManager newTransactionManager()
   {
      return new TransactionManagerImple();
   }

   // Inner classes -------------------------------------------------------------------
}
