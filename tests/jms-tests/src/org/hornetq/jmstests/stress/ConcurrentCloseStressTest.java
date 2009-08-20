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

package org.hornetq.jmstests.stress;

import java.util.ArrayList;
import java.util.Iterator;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.hornetq.core.logging.Logger;
import org.hornetq.jmstests.HornetQServerTestCase;

/**
 * This test was added to test regression on http://jira.jboss.com/jira/browse/JBMESSAGING-660
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class ConcurrentCloseStressTest extends HornetQServerTestCase
{
   private static final Logger log = Logger.getLogger(ConcurrentCloseStressTest.class);

   InitialContext ic;
   ConnectionFactory cf;
   Queue queue;

   public void setUp() throws Exception
   {
      super.setUp();

      //ServerManagement.start("all");


      ic = getInitialContext();
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      destroyQueue("TestQueue");
      createQueue("TestQueue");

      queue = (Queue) ic.lookup("queue/TestQueue");

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      destroyQueue("TestQueue");

      super.tearDown();

      log.debug("tear down done");
   }


   public void testProducersAndConsumers() throws Exception
   {
      Connection connectionProducer = cf.createConnection();
      Connection connectionReader = cf.createConnection();

      connectionReader.start();
      connectionProducer.start(); // try with and without this...

      ProducerThread producerThread[] = new ProducerThread[20];
      ReaderThread readerThread[] = new ReaderThread[20];
      TestThread threads[] = new TestThread[40];


      for (int i = 0; i < 20; i++)
      {
         producerThread[i] = new ProducerThread(i, connectionProducer, queue);
         readerThread[i] = new ReaderThread(i, connectionReader, queue);
         threads[i] = producerThread[i];
         threads[i+20] = readerThread[i];
      }


      for (int i = 0; i < 40; i++)
      {
         threads[i].start();
      }

      for (int i = 0; i < 40; i++)
      {
         threads[i].join();
      }


      boolean hasFailure=false;

      for (int i = 0; i < 40; i++)
      {
         if (threads[i].exceptions.size() > 0)
         {
            hasFailure = true;
            for (Iterator failureIter = threads[i].exceptions.iterator(); failureIter.hasNext();)
            {
               Exception ex = (Exception) failureIter.next();
               log.error("Exception occurred in one of the threads - " + ex, ex);
            }
         }
      }

      int messagesProduced=0;
      int messagesRead=0;
      for (int i = 0; i < producerThread.length; i++)
      {
         messagesProduced += producerThread[i].messagesProduced;
      }

      for (int i = 0; i < producerThread.length; i++)
      {
         messagesRead += readerThread[i].messagesRead;
      }

      if (hasFailure)
      {
         fail ("An exception has occurred in one of the threads");
      }
   }


   static class TestThread extends Thread
   {
      ArrayList exceptions = new ArrayList();
      protected int index;
      public int messageCount = 0;
   }


   static class ReaderThread extends TestThread
   {
      private static final Logger log = Logger.getLogger(ReaderThread.class);
      Connection conn;

      Queue queue;

      int messagesRead = 0;

      public ReaderThread(int index, Connection conn, Queue queue) throws Exception
      {
         this.index = index;
         this.conn = conn;
         this.queue = queue;
      }


      public void run()
      {
         int commitCounter = 0;
         try
         {
            Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer((Destination)queue);

            int lastCount = messageCount;
            while (true)
            {
               TextMessage message = (TextMessage) consumer.receive(5000);
               if (message == null)
               {
                  break;
               }
               log.debug("read message " + message.getText());

               // alternating commits and rollbacks
               if ( (commitCounter++) % 2 == 0)
               {
                  messagesRead += (messageCount - lastCount);
                  lastCount = messageCount;
                  log.debug("commit");
                  session.commit();
               }
               else
               {
                  lastCount = messageCount;
                  log.debug("rollback");
                  session.rollback();
               }

               messageCount++;

               if (messageCount %7 == 0)
               {
                  session.close();

                  session = conn.createSession(true, Session.SESSION_TRANSACTED);
                  consumer = session.createConsumer((Destination)queue);
               }
            }

            messagesRead += (messageCount - lastCount);

            session.commit();
            consumer.close();
            session.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
            exceptions.add(e);
         }
      }

   }


   static class ProducerThread extends TestThread
   {
      private static final Logger log = Logger.getLogger(ProducerThread.class);

      Connection conn;
      Queue queue;
      int messagesProduced=0;

      public ProducerThread(int index, Connection conn, Queue queue) throws Exception
      {
         this.index = index;
         this.conn = conn;
         this.queue = queue;
      }


      public void run()
      {
         for (int i = 0; i < 10; i++)
         {
            try
            {
               int lastMessage = messageCount;
               Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
               MessageProducer producer = sess.createProducer((Destination)queue);

               for (int j = 0; j < 20; j++)
               {
                  producer.send(sess.createTextMessage("Message " + i + ", " + j));

                  if (j % 2 == 0)
                  {
                     log.debug("commit");
                     messagesProduced += (messageCount - lastMessage);
                     lastMessage = messageCount;
                     
                     sess.commit();
                  }
                  else
                  {
                     log.debug("rollback");
                     lastMessage = messageCount;
                     sess.rollback();
                  }
                  messageCount ++;

               }

               messagesProduced += ((messageCount) - lastMessage);
               sess.commit();
               sess.close();
            }
            catch (Exception e)
            {
               e.printStackTrace();
               exceptions.add(e);
            }
         }
      }

   }

}
