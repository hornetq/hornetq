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

package org.hornetq.jms.tests;

import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnection;
import javax.jms.XATopicConnectionFactory;

import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.tests.util.ProxyAssertSupport;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFactoryTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp(); // To change body of overridden methods use File | Settings | File Templates.
   }

   // Public --------------------------------------------------------

   /**
    * Test that ConnectionFactory can be cast to QueueConnectionFactory and QueueConnection can be
    * created.
    */
   public void testQueueConnectionFactory() throws Exception
   {
      QueueConnectionFactory qcf = (QueueConnectionFactory)JMSTestCase.ic.lookup("/CF_QUEUE_XA_FALSE");
      QueueConnection qc = qcf.createQueueConnection();
      qc.close();
   }

   /**
    * Test that ConnectionFactory can be cast to TopicConnectionFactory and TopicConnection can be
    * created.
    */
   public void testTopicConnectionFactory() throws Exception
   {
      TopicConnectionFactory qcf = (TopicConnectionFactory)JMSTestCase.ic.lookup("/CF_TOPIC_XA_FALSE");
      TopicConnection tc = qcf.createTopicConnection();
      tc.close();
   }

   public void testAdministrativelyConfiguredClientID() throws Exception
   {
      // deploy a connection factory that has an administatively configured clientID
      HornetQServerTestCase.deployConnectionFactory("sofiavergara", "TestConnectionFactory", "TestConnectionFactory");

      ConnectionFactory cf = (ConnectionFactory)JMSTestCase.ic.lookup("/TestConnectionFactory");
      Connection c = cf.createConnection();

      ProxyAssertSupport.assertEquals("sofiavergara", c.getClientID());

      try
      {
         c.setClientID("somethingelse");
         ProxyAssertSupport.fail("should throw exception");

      }
      catch (javax.jms.IllegalStateException e)
      {
         // OK
      }
      c.close();
      HornetQServerTestCase.undeployConnectionFactory("TestConnectionFactory");
   }

   public void testNoClientIDConfigured_1() throws Exception
   {
      // the ConnectionFactories that ship with HornetQ do not have their clientID
      // administratively configured.

      ConnectionFactory cf = (ConnectionFactory)JMSTestCase.ic.lookup("/CF_XA_FALSE");
      Connection c = cf.createConnection();

      ProxyAssertSupport.assertNull(c.getClientID());

      c.close();
   }

   public void testNoClientIDConfigured_2() throws Exception
   {
      // the ConnectionFactories that ship with HornetQ do not have their clientID
      // administratively configured.

      ConnectionFactory cf = (ConnectionFactory)JMSTestCase.ic.lookup("/CF_XA_FALSE");
      Connection c = cf.createConnection();

      // set the client id immediately after the connection is created

      c.setClientID("sofiavergara2");
      ProxyAssertSupport.assertEquals("sofiavergara2", c.getClientID());

      c.close();
   }

   // Added for http://jira.jboss.org/jira/browse/JBMESSAGING-939
   public void testDurableSubscriptionOnPreConfiguredConnectionFactory() throws Exception
   {
      HornetQServerTestCase.deployConnectionFactory("TestConnectionFactory1", "cfTest", "/TestDurableCF");

      createTopic("TestSubscriber");

      Connection conn = null;

      try
      {
         Topic topic = (Topic)JMSTestCase.ic.lookup("/topic/TestSubscriber");
         ConnectionFactory cf = (ConnectionFactory)JMSTestCase.ic.lookup("/TestDurableCF");
         conn = cf.createConnection();

         // I have to remove this asertion, as the test would work if doing this assertion
         // as getClientID performed some operation that cleared the bug condition during
         // the creation of this testcase
         // assertEquals("cfTest", conn.getClientID());

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         session.createDurableSubscriber(topic, "durableSubscriberChangeSelectorTest", "TEST = 'test'", false);
      }
      finally
      {
         try
         {
            if (conn != null)
            {
               conn.close();
            }
         }
         catch (Exception e)
         {
            log.warn(e.toString(), e);
         }

         try
         {
            destroyTopic("TestSubscriber");
         }
         catch (Exception e)
         {
            log.warn(e.toString(), e);
         }

      }

   }

   public void testSlowConsumers() throws Exception
   {
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("TestSlowConsumersCF");
      HornetQServerTestCase.deployConnectionFactory(0, "TestSlowConsumersCF", 1, "TestSlowConsumersCF");

      Connection conn = null;

      try
      {
         ConnectionFactory cf = (ConnectionFactory)JMSTestCase.ic.lookup("/TestSlowConsumersCF");

         conn = cf.createConnection();

         Session session1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         final Object waitLock = new Object();

         final int numMessages = 500;

         class FastListener implements MessageListener
         {
            volatile int processed;

            public void onMessage(final Message msg)
            {
               processed++;

               if (processed == numMessages - 2)
               {
                  synchronized (waitLock)
                  {
                     waitLock.notifyAll();
                  }
               }
            }
         }

         final FastListener fast = new FastListener();

         class SlowListener implements MessageListener
         {
            volatile int processed;

            public void onMessage(final Message msg)
            {
               processed++;

               synchronized (waitLock)
               {
                  // Should really cope with spurious wakeups
                  while (fast.processed != numMessages - 2)
                  {
                     try
                     {
                        waitLock.wait(20000);
                     }
                     catch (InterruptedException e)
                     {
                     }
                  }

                  waitLock.notify();
               }
            }
         }

         final SlowListener slow = new SlowListener();

         MessageConsumer cons1 = session1.createConsumer(HornetQServerTestCase.queue1);

         cons1.setMessageListener(slow);

         MessageConsumer cons2 = session2.createConsumer(HornetQServerTestCase.queue1);

         cons2.setMessageListener(fast);

         Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sessSend.createProducer(HornetQServerTestCase.queue1);

         conn.start();

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);

            prod.send(tm);
         }

         // All the messages bar one should be consumed by the fast listener - since the slow listener shouldn't buffer
         // any.

         synchronized (waitLock)
         {
            // Should really cope with spurious wakeups
            while (fast.processed != numMessages - 2)
            {
               waitLock.wait(20000);
            }

            while (slow.processed != 2)
            {
               waitLock.wait(20000);
            }
         }

         ProxyAssertSupport.assertTrue(fast.processed == numMessages - 2);

      }
      finally
      {
         try
         {
            if (conn != null)
            {
               conn.close();
            }
         }
         catch (Exception e)
         {
            log.warn(e.toString(), e);
         }

         try
         {
            HornetQServerTestCase.undeployConnectionFactory("TestSlowConsumersCF");
         }
         catch (Exception e)
         {
            log.warn(e.toString(), e);
         }

      }

   }
   
   public void testFactoryTypes() throws Exception
   {
      HornetQConnectionFactory factory = null;
      
      factory = (HornetQConnectionFactory)JMSTestCase.ic.lookup("/ConnectionFactory");
      
      assertTrue(factory instanceof ConnectionFactory);
      assertEquals(3, getTypes(factory));
      
      factory = (HornetQConnectionFactory)JMSTestCase.ic.lookup("/CF_XA_TRUE");
      
      assertTrue(factory instanceof XAConnectionFactory);
      assertEquals(6, getTypes(factory));
      
      factory = (HornetQConnectionFactory)JMSTestCase.ic.lookup("/CF_XA_FALSE");
      
      assertTrue(factory instanceof ConnectionFactory);
      assertEquals(3, getTypes(factory));
      
      factory = (HornetQConnectionFactory)JMSTestCase.ic.lookup("/CF_GENERIC");
      
      assertTrue(factory instanceof ConnectionFactory);
      assertEquals(3, getTypes(factory));
      
      factory = (HornetQConnectionFactory)JMSTestCase.ic.lookup("/CF_GENERIC_XA_TRUE");
      
      assertTrue(factory instanceof XAConnectionFactory);
      assertEquals(6, getTypes(factory));
      
      factory = (HornetQConnectionFactory)JMSTestCase.ic.lookup("/CF_GENERIC_XA_FALSE");
      
      assertTrue(factory instanceof ConnectionFactory);
      assertEquals(3, getTypes(factory));
      
      factory = (HornetQConnectionFactory)JMSTestCase.ic.lookup("/CF_QUEUE");
      
      assertTrue(factory instanceof QueueConnectionFactory);
      assertEquals(2, getTypes(factory));
      
      factory = (HornetQConnectionFactory)JMSTestCase.ic.lookup("/CF_QUEUE_XA_TRUE");
      
      assertTrue(factory instanceof XAQueueConnectionFactory);
      assertEquals(4, getTypes(factory));
      
      factory = (HornetQConnectionFactory)JMSTestCase.ic.lookup("/CF_QUEUE_XA_FALSE");
      
      assertTrue(factory instanceof QueueConnectionFactory);
      assertEquals(2, getTypes(factory));
      
      factory = (HornetQConnectionFactory)JMSTestCase.ic.lookup("/CF_TOPIC");
      
      assertTrue(factory instanceof TopicConnectionFactory);
      assertEquals(2, getTypes(factory));
      
      factory = (HornetQConnectionFactory)JMSTestCase.ic.lookup("/CF_TOPIC_XA_TRUE");
      
      assertTrue(factory instanceof XATopicConnectionFactory);
      assertEquals(4, getTypes(factory));
      
      factory = (HornetQConnectionFactory)JMSTestCase.ic.lookup("/CF_TOPIC_XA_FALSE");
      
      assertTrue(factory instanceof TopicConnectionFactory);
      assertEquals(2, getTypes(factory));
   }
   
   public void testConnectionTypes() throws Exception
   {
      Connection genericConnection = null;
      XAConnection xaConnection = null;
      QueueConnection queueConnection = null;
      TopicConnection topicConnection = null;
      XAQueueConnection xaQueueConnection = null;
      XATopicConnection xaTopicConnection = null;

      ConnectionFactory genericFactory = (ConnectionFactory)JMSTestCase.ic.lookup("/ConnectionFactory");
      genericConnection = genericFactory.createConnection();
      assertConnectionType(genericConnection, "generic");

      XAConnectionFactory xaFactory = (XAConnectionFactory)JMSTestCase.ic.lookup("/CF_XA_TRUE");
      xaConnection = xaFactory.createXAConnection();
      assertConnectionType(xaConnection, "xa");

      QueueConnectionFactory queueCF = (QueueConnectionFactory)JMSTestCase.ic.lookup("/CF_QUEUE");
      queueConnection = queueCF.createQueueConnection();
      assertConnectionType(queueConnection, "queue");

      TopicConnectionFactory topicCF = (TopicConnectionFactory)JMSTestCase.ic.lookup("/CF_TOPIC");
      topicConnection = topicCF.createTopicConnection();
      assertConnectionType(topicConnection, "topic");

      XAQueueConnectionFactory xaQueueCF = (XAQueueConnectionFactory)JMSTestCase.ic.lookup("/CF_QUEUE_XA_TRUE");
      xaQueueConnection = xaQueueCF.createXAQueueConnection();
      assertConnectionType(xaQueueConnection, "xa-queue");

      XATopicConnectionFactory xaTopicCF = (XATopicConnectionFactory)JMSTestCase.ic.lookup("/CF_TOPIC_XA_TRUE");
      xaTopicConnection = xaTopicCF.createXATopicConnection();
      assertConnectionType(xaTopicConnection, "xa-topic");

      genericConnection.close();
      xaConnection.close();
      queueConnection.close();
      topicConnection.close();
      xaQueueConnection.close();
      xaTopicConnection.close();
   }

   private void assertConnectionType(Connection conn, String type)
   {
      if ("generic".equals(type))
      {
         //generic
         assertTrue(conn instanceof Connection);
         assertFalse(conn instanceof XAConnection);
         assertTrue(conn instanceof QueueConnection);
         assertFalse(conn instanceof XAQueueConnection);
         assertTrue(conn instanceof TopicConnection);
         assertFalse(conn instanceof XATopicConnection);
      }
      else if ("queue".equals(type))
      {
         assertTrue(conn instanceof Connection);
         assertFalse(conn instanceof XAConnection);
         assertTrue(conn instanceof QueueConnection);
         assertFalse(conn instanceof XAQueueConnection);
         assertTrue(conn instanceof TopicConnection);
         assertFalse(conn instanceof XATopicConnection);
      }
      else if ("topic".equals(type))
      {
         assertTrue(conn instanceof Connection);
         assertFalse(conn instanceof XAConnection);
         assertTrue(conn instanceof QueueConnection);
         assertFalse(conn instanceof XAQueueConnection);
         assertTrue(conn instanceof TopicConnection);
         assertFalse(conn instanceof XATopicConnection);
      }
      else if ("xa".equals(type))
      {
         assertTrue(conn instanceof Connection);
         assertTrue(conn instanceof XAConnection);
         assertTrue(conn instanceof QueueConnection);
         assertFalse(conn instanceof XAQueueConnection);
         assertTrue(conn instanceof TopicConnection);
         assertFalse(conn instanceof XATopicConnection);
      }
      else if ("xa-queue".equals(type))
      {
         assertTrue(conn instanceof Connection);
         assertTrue(conn instanceof XAConnection);
         assertTrue(conn instanceof QueueConnection);
         assertTrue(conn instanceof XAQueueConnection);
         assertTrue(conn instanceof TopicConnection);
         assertFalse(conn instanceof XATopicConnection);
      }
      else if ("xa-topic".equals(type))
      {
         assertTrue(conn instanceof Connection);
         assertTrue(conn instanceof XAConnection);
         assertTrue(conn instanceof QueueConnection);
         assertFalse(conn instanceof XAQueueConnection);
         assertTrue(conn instanceof TopicConnection);
         assertTrue(conn instanceof XATopicConnection);
      }
      else
      {
         fail("Unknown connection type: " + type);
      }
   }

   private int getTypes(HornetQConnectionFactory factory)
   {
      int num = 0;
      if (factory instanceof ConnectionFactory)
      {
         num++;
      }
      if (factory instanceof XAConnectionFactory)
      {
         num++;
      }
      if (factory instanceof QueueConnectionFactory)
      {
         num++;
      }
      if (factory instanceof TopicConnectionFactory)
      {
         num++;
      }
      if (factory instanceof XAQueueConnectionFactory)
      {
         num++;
      }
      if (factory instanceof XATopicConnectionFactory)
      {
         num++;
      }
      return num;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
