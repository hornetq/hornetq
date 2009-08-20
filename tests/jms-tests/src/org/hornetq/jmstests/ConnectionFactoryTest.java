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

package org.hornetq.jmstests;

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

   protected void setUp() throws Exception
   {
      super.setUp();    //To change body of overridden methods use File | Settings | File Templates.
   }

   // Public --------------------------------------------------------

   /**
    * Test that ConnectionFactory can be cast to QueueConnectionFactory and QueueConnection can be
    * created.
    */
   public void testQueueConnectionFactory() throws Exception
   {
      QueueConnectionFactory qcf =
         (QueueConnectionFactory)ic.lookup("/ConnectionFactory");
      QueueConnection qc = qcf.createQueueConnection();
      qc.close();
   }
   
   /**
    * Test that ConnectionFactory can be cast to TopicConnectionFactory and TopicConnection can be
    * created.
    */
   public void testTopicConnectionFactory() throws Exception
   {
      TopicConnectionFactory qcf =
         (TopicConnectionFactory)ic.lookup("/ConnectionFactory");
      TopicConnection tc = qcf.createTopicConnection();
      tc.close();
   }

   public void testAdministrativelyConfiguredClientID() throws Exception
   {
      // deploy a connection factory that has an administatively configured clientID
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("TestConnectionFactory");
      deployConnectionFactory("sofiavergara", "TestConnectionFactory", bindings );

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/TestConnectionFactory");
      Connection c = cf.createConnection();

      assertEquals("sofiavergara", c.getClientID());

      try
      {
         c.setClientID("somethingelse");
         fail("should throw exception");

      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
      c.close();
      undeployConnectionFactory("TestConnectionFactory");
   }
   
   public void testNoClientIDConfigured_1() throws Exception
   {
      // the ConnectionFactories that ship with Messaging do not have their clientID
      // administratively configured.

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Connection c = cf.createConnection();

      assertNull(c.getClientID());

      c.close();
   }

   public void testNoClientIDConfigured_2() throws Exception
   {
      // the ConnectionFactories that ship with Messaging do not have their clientID
      // administratively configured.

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Connection c = cf.createConnection();

      // set the client id immediately after the connection is created

      c.setClientID("sofiavergara2");
      assertEquals("sofiavergara2", c.getClientID());

      c.close();
   }

   // Added for http://jira.jboss.org/jira/browse/JBMESSAGING-939
   public void testDurableSubscriptionOnPreConfiguredConnectionFactory() throws Exception
   {
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("/TestDurableCF");
      deployConnectionFactory("TestConnectionFactory1","cfTest", bindings);

      createTopic("TestSubscriber");

      Connection conn = null;

      try
      {
         Topic topic = (Topic) ic.lookup("/topic/TestSubscriber");
         ConnectionFactory cf = (ConnectionFactory) ic.lookup("/TestDurableCF");
         conn = cf.createConnection();

         // I have to remove this asertion, as the test would work if doing this assertion
         // as getClientID performed some operation that cleared the bug condition during
         // the creation of this testcase
         //assertEquals("cfTest", conn.getClientID());

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         session.createDurableSubscriber(topic,
            "durableSubscriberChangeSelectorTest", "TEST = 'test'", false);
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
      deployConnectionFactory(0, "TestSlowConsumersCF", bindings, 1);

      Connection conn = null;

      try
      {
         ConnectionFactory cf = (ConnectionFactory) ic.lookup("/TestSlowConsumersCF");

         conn = cf.createConnection();

         Session session1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         final Object waitLock = new Object();

         final int numMessages = 500;

         class FastListener implements MessageListener
         {
            volatile int processed;

            public void onMessage(Message msg)
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

            public void onMessage(Message msg)
            {
               processed++;
               
               synchronized (waitLock)
               {
                  //Should really cope with spurious wakeups
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

         MessageConsumer cons1 = session1.createConsumer(queue1);

         cons1.setMessageListener(slow);

         MessageConsumer cons2 = session2.createConsumer(queue1);

         cons2.setMessageListener(fast);


         Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sessSend.createProducer(queue1);

         conn.start();

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);

            prod.send(tm);
         }

         //All the messages bar one should be consumed by the fast listener  - since the slow listener shouldn't buffer any.

         synchronized (waitLock)
         {
            //Should really cope with spurious wakeups
            while (fast.processed != numMessages - 2)
            {
               waitLock.wait(20000);
            }
            
            while (slow.processed != 2)
            {
               waitLock.wait(20000);
            }
         }

         assertTrue(fast.processed == numMessages - 2);
         
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
            undeployConnectionFactory("TestSlowConsumersCF");
         }
         catch (Exception e)
         {
            log.warn(e.toString(), e);
         }

      }

   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   
   // Inner classes -------------------------------------------------

}
