/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

package org.jboss.test.messaging.jms.stress;

import java.util.ArrayList;
import java.util.Iterator;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.jms.ConnectionTest;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * This test was added to test regression on http://jira.jboss.com/jira/browse/JBMESSAGING-660
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class ConcurrentCloseStressTest extends MessagingTestCase
{
   private static final Logger log = Logger.getLogger(ConnectionTest.class);

   public ConcurrentCloseStressTest(String name)
   {
      super(name);
   }

   InitialContext ic;
   JBossConnectionFactory cf;
   Queue queue;

   public void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");


      ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");

      ServerManagement.undeployQueue("TestQueue");
      ServerManagement.deployQueue("TestQueue");

      queue = (Queue) ic.lookup("queue/TestQueue");

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("TestQueue");

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

      log.info("The test produced " + messagesProduced + " and read " + messagesRead);

      // This test bounces between commits and rollbacks in between several threads...
      // The test is then non deterministic to provide a counter... I will keep the log.info
      // but won't be doing an assertion here
      //assertEquals("Messages Produced must be the same as Messages Read", messagesProduced, messagesRead);

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

               if (messageCount % 50 == 0)
               {
                  log.info("Reader " + index + " read " + messageCount + " messages");
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
//            log.debug("ReaderThread " + index + " died");
//            System.exit(1);
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

               if (messageCount % 50 == 0)
               {
                  log.info("Producer " + index + " sent " + messageCount + " messages");
               }

            }
            catch (Exception e)
            {
               e.printStackTrace();
               exceptions.add(e);
//               log.debug("ProducerThread " + index + " died");
//               System.exit(1);
            }
         }
      }

   }

}
