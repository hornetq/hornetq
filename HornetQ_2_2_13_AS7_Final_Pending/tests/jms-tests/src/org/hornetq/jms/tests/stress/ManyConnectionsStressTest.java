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
package org.hornetq.jms.tests.stress;

import java.util.HashSet;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.hornetq.core.logging.Logger;
import org.hornetq.jms.tests.HornetQServerTestCase;
import org.hornetq.jms.tests.util.ProxyAssertSupport;

/**
 * 
 * Create 500 connections each with a consumer, consuming from a topic
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>4 Jul 2007
 *
 * $Id: $
 *
 */
public class ManyConnectionsStressTest extends HornetQServerTestCase
{
   // Constants -----------------------------------------------------

   private static Logger log = Logger.getLogger(RelayStressTest.class);

   private static final int NUM_CONNECTIONS = 500;

   private static final int NUM_MESSAGES = 100;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private InitialContext ic;

   private volatile boolean failed;

   private final Set listeners = new HashSet();

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      // ServerManagement.start("all");

      ic = getInitialContext();

      createTopic("StressTestTopic");

      ManyConnectionsStressTest.log.debug("setup done");
   }

   @Override
   protected void tearDown() throws Exception
   {
      destroyTopic("StressTestTopic");

      ic.close();

      super.tearDown();
   }

   public void testManyConnections() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      Topic topic = (Topic)ic.lookup("/topic/StressTestTopic");

      Connection[] conns = new Connection[ManyConnectionsStressTest.NUM_CONNECTIONS];

      Connection connSend = null;

      try
      {
         for (int i = 0; i < ManyConnectionsStressTest.NUM_CONNECTIONS; i++)
         {
            conns[i] = cf.createConnection();

            Session sess = conns[i].createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer cons = sess.createConsumer(topic);

            MyListener listener = new MyListener();

            synchronized (listeners)
            {
               listeners.add(listener);
            }

            cons.setMessageListener(listener);

            conns[i].start();

            ManyConnectionsStressTest.log.info("Created " + i);
         }

         // Thread.sleep(100 * 60 * 1000);

         connSend = cf.createConnection();

         Session sessSend = connSend.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sessSend.createProducer(topic);

         for (int i = 0; i < ManyConnectionsStressTest.NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSend.createTextMessage("message" + i);

            tm.setIntProperty("count", i);

            prod.send(tm);
         }

         long wait = 30000;

         synchronized (listeners)
         {
            while (!listeners.isEmpty() && wait > 0)
            {
               long start = System.currentTimeMillis();
               try
               {
                  listeners.wait(wait);
               }
               catch (InterruptedException e)
               {
                  // Ignore
               }
               wait -= System.currentTimeMillis() - start;
            }
         }

         if (wait <= 0)
         {
            ProxyAssertSupport.fail("Timed out");
         }

         ProxyAssertSupport.assertFalse(failed);

         ManyConnectionsStressTest.log.info("Done");
      }
      finally
      {
         for (int i = 0; i < ManyConnectionsStressTest.NUM_CONNECTIONS; i++)
         {
            try
            {
               if (conns[i] != null)
               {
                  conns[i].close();
               }
            }
            catch (Throwable t)
            {
               ManyConnectionsStressTest.log.error("Failed to close connection", t);
            }
         }

         if (connSend != null)
         {
            connSend.close();
         }
      }
   }

   private void finished(final MyListener listener)
   {
      synchronized (listeners)
      {
         ManyConnectionsStressTest.log.info("consumer " + listener + " has finished");

         listeners.remove(listener);

         listeners.notify();
      }
   }

   private void failed(final MyListener listener)
   {
      synchronized (listeners)
      {
         ManyConnectionsStressTest.log.error("consumer " + listener + " has failed");

         listeners.remove(listener);

         failed = true;

         listeners.notify();
      }
   }

   private class MyListener implements MessageListener
   {
      public void onMessage(final Message msg)
      {
         try
         {
            int count = msg.getIntProperty("count");

            // log.info(this + " got message " + msg);

            if (count == ManyConnectionsStressTest.NUM_MESSAGES - 1)
            {
               finished(this);
            }
         }
         catch (JMSException e)
         {
            ManyConnectionsStressTest.log.error("Failed to get int property", e);

            failed(this);
         }
      }

   }
}
