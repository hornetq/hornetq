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

package org.jboss.test.messaging.jms.clustering;

import java.util.ArrayList;
import java.util.Iterator;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.jms.clustering.base.ClusteringTestBase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.aop.PoisonInterceptor;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * 
 * $Id$
 */
public class MultiThreadFailoverTest extends ClusteringTestBase
{

   // Constants ------------------------------------------------------------------------------------

   int NUMBER_OF_PRODUCER_THREADS=1;
   int NUMBER_OF_CONSUMER_THREADS=1;

   // Attributes -----------------------------------------------------------------------------------
   int messageCounterConsumer = 0;
   int messageCounterProducer = 0;


   Object lockReader = new Object();
   Object lockWriter = new Object();
   Object semaphore = new Object();
   boolean started = false;

   boolean shouldStop = false;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------
   public MultiThreadFailoverTest(String name)
   {
      super(name);
   }


   // Public ---------------------------------------------------------------------------------------


   /**
    * Created per http://jira.jboss.org/jira/browse/JBMESSAGING-790
    */
   public void testMultiThreadOnReceive() throws Exception
   {

      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();

      getConnection(new Connection[]{conn1, conn2, conn3}, 0).close();
      getConnection(new Connection[]{conn1, conn2, conn3}, 2).close();

      log.info("Created connections");

      checkConnectionsDifferentServers(new Connection[]{conn1, conn2, conn3});

      Connection conn = getConnection(new Connection[]{conn1, conn2, conn3}, 1);

      conn.start();

      ReceiveConsumerThread consumerThread = new ReceiveConsumerThread(
         conn.createSession(false,Session.AUTO_ACKNOWLEDGE),queue[1]);

      consumerThread.start();

      // Just give some time to line up the thread on receive
      Thread.sleep(1000);

      Session session = conn.createSession(false,Session.AUTO_ACKNOWLEDGE); 

      MessageProducer producer = session.createProducer(queue[1]);

      ServerManagement.killAndWait(1);

      producer.send(session.createTextMessage("Have a nice day!"));

      consumerThread.join();

      if (consumerThread.exception != null)
      {
         throw consumerThread.exception;
      }

      assertNotNull (consumerThread.message);

      assertEquals("Have a nice day!", consumerThread.message.getText());

      conn.close();
   }


   // Crash the Server when you have two clients in receive and send simultaneously
   public void testFailureOnSendReceiveSynchronized() throws Throwable
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();
         conn.close();

         conn = cf.createConnection();

         assertEquals(1, ((JBossConnection)conn).getServerID());

         // we "cripple" the remoting connection by removing ConnectionListener. This way, failures
         // cannot be "cleanly" detected by the client-side pinger, and we'll fail on an invocation
         JMSRemotingConnection rc = ((ClientConnectionDelegate)((JBossConnection)conn).
            getDelegate()).getRemotingConnection();
         rc.removeConnectionListener();

         // poison the server
         ServerManagement.poisonTheServer(1, PoisonInterceptor.FAIL_SYNCHRONIZED_SEND_RECEIVE);

         conn.start();

         final Session sessionProducer  = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         final MessageProducer producer = sessionProducer.createProducer(queue[0]);

         final Session sessionConsumer  = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         final MessageConsumer consumer = sessionConsumer.createConsumer(queue[0]);

         final ArrayList failures = new ArrayList();

         producer.setDeliveryMode(DeliveryMode.PERSISTENT);

         
         Thread t1 = new Thread()
         {
            public void run()
            {
               try
               {
                  producer.send(sessionProducer.createTextMessage("before-poison"));
               }
               catch (Throwable e)
               {
                  failures.add(e);
               }
            }
         };

         Thread t2 = new Thread()
         {
            public void run()
            {
               try
               {
                  log.info("### Waiting message");
                  TextMessage text = (TextMessage)consumer.receive();
                  assertNotNull(text);
                  assertEquals("before-poison", text.getText());

                  Object obj = consumer.receive(5000);
                  if (obj != null)
                  {
                     log.info("!!!!!! it was not null", new Exception());
                  }
                  assertNull(obj);
               }
               catch (Throwable e)
               {
                  fail("Thread consumer failed");
               }
            }
         };

         t2.start();
         Thread.sleep(500);
         t1.start();

         t1.join();
         t2.join();

         if (!failures.isEmpty())
         {
            throw (Throwable)failures.iterator().next();
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

   /**
    * This test will open several Consumers at the same Connection and it will kill the server,
    * expecting failover to happen inside the Valve
    */
   public void testMultiThreadFailover() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();

      try
      {
         log.info("Created connections");

         checkConnectionsDifferentServers(new Connection[]{conn1, conn2, conn3});

         Connection conn = getConnection(new Connection[]{conn1, conn2, conn3}, 1);

         conn.start();

         for (int i = 0; i < 3; i++)
         {
            JBossConnection connTest = (JBossConnection)
               getConnection(new Connection[]{conn1, conn2, conn3}, i);

            String locator = ((ClientConnectionDelegate) connTest.getDelegate()).
               getRemotingConnection().getRemotingClient().getInvoker().getLocator().getLocatorURI();

            log.info("Server " + i + " has locator=" + locator);

         }

         ArrayList threadList = new ArrayList();

         for (int i = 0; i < NUMBER_OF_PRODUCER_THREADS; i++)
         {
            threadList.add(new LocalThreadProducer(i, conn.createSession(false,
               Session.AUTO_ACKNOWLEDGE), queue[1]));
         }

         for (int i = 0; i < NUMBER_OF_CONSUMER_THREADS; i++)
         {
            threadList.add(new LocalThreadConsumer(i, conn.createSession(false,
               Session.AUTO_ACKNOWLEDGE), queue[1]));
         }

         for (Iterator iter = threadList.iterator(); iter.hasNext();)
         {
            Thread t = (Thread) iter.next();
            t.start();
         }

         Thread.sleep(1000);
         synchronized (semaphore)
         {
            started = true;
            semaphore.notifyAll();
         }

         Thread.sleep(30000);

         log.info("Killing server 1");

         ServerManagement.log(ServerManagement.INFO, "Server 1 will be killed");

         ServerManagement.log(ServerManagement.INFO, "Server 1 will be killed", 2);

         log.info("messageCounterConsumer=" + messageCounterConsumer + ", messageCounterProducer=" +
            messageCounterProducer);

         ServerManagement.killAndWait(1);

         Thread.sleep(50000);

         log.info("messageCounterConsumer=" + messageCounterConsumer + ", messageCounterProducer=" +
            messageCounterProducer);

         shouldStop = true;

         boolean failed = false;

         for (Iterator iter = threadList.iterator(); iter.hasNext();)
         {

            LocalThread t = (LocalThread) iter.next();

            t.join();

            if (t.exception != null)
            {
               failed = true;
               log.error("Error: " + t.exception, t.exception);
            }
         }

         if (failed)
         {
            fail ("One of the threads has thrown an exception... Test Fails");
         }

         log.info("messageCounterConsumer=" + messageCounterConsumer + ", messageCounterProducer=" +
            messageCounterProducer);

         assertEquals(messageCounterProducer, messageCounterConsumer);

      }
      finally
      {
         conn1.close();

         conn2.close();

         conn3.close();
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 3;

      super.setUp();

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }


   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   class LocalThread extends Thread
   {
      Exception exception;
      TextMessage message;

      public LocalThread(String name)
      {
         super(name);
      }

   }

   // Inner classes used by testMultiThreadOnReceive -----------------------------------------------

   class ReceiveConsumerThread extends LocalThread
   {

      MessageConsumer consumer;
      Session session;

      public ReceiveConsumerThread(Session session, Destination destination)
          throws Exception
      {
         super("Consumer Thread");
         this.session = session;
         consumer = session.createConsumer(destination);
      }


      public void run()
      {
         try
         {
            message = (TextMessage)consumer.receive();
            if (message == null)
            {
               this.exception = new IllegalStateException("message.receive was null");
            }
         }
         catch (Exception e)
         {
            log.error(e,e);
            this.exception = e;
         }
      }
      
   }

   // Inner classes used by testMultiThreadFailover ------------------------------------------------

   class LocalThreadConsumer extends LocalThread
   {
      private final Logger log = Logger.getLogger(this.getClass());

      int id;
      MessageConsumer consumer;
      Session session;

      public LocalThreadConsumer(int id, Session session, Destination destination) throws Exception
      {
         super("LocalThreadConsumer-" + id);
         consumer = session.createConsumer(destination);
         this.session = session;
         this.id = id;
      }


      public void run()
      {
         try
         {
            synchronized (semaphore)
            {
               if (!started)
               {
                  semaphore.wait();
               }
            }

            int counter = 0;
            while (true)
            {
               Message message = consumer.receive(5000);
               if (message == null && shouldStop)
               {
                  break;
               }
               if (message != null)
               {
                  synchronized (lockReader)
                  {
                     messageCounterConsumer++;
                     if (counter ++ % 10 == 0)
                     {
                        log.info("Read = " + messageCounterConsumer);
                     }
                  }
                  log.trace("ReceiverID=" + id + " received message " + message);
                  if (counter++ % 10 == 0)
                  {
                     //log.info("Commit on id=" + id);
                     //session.commit();
                  }
               }
            }
            //session.commit();
         }
         catch (Exception e)
         {
            this.exception = e;
            log.info("Caught exception... finishing Thread " + id, e);
         }
      }
   }

   class LocalThreadProducer extends LocalThread
   {
      private final Logger log = Logger.getLogger(this.getClass());

      MessageProducer producer;
      Session session;
      int id;

      public LocalThreadProducer(int id, Session session, Destination destination) throws Exception
      {
         super("LocalThreadProducer-" + id);
         this.session = session;
         producer = session.createProducer(destination);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);
         this.id = id;
      }

      public void run()
      {
         try
         {
            synchronized (semaphore)
            {
               if (!started)
               {
                  semaphore.wait();
               }
            }

            int counter = 0;
            while (!shouldStop)
            {
               log.trace("Producer ID=" + id + " send message");
               producer.send(session.createTextMessage("Message from producer " + id + " counter=" + (counter)));

               synchronized (lockWriter)
               {
                  messageCounterProducer++;
                  if (counter ++ % 10 == 0)
                  {
                     log.info("Sent = " + messageCounterProducer);
                  }
               }

               if (counter++ % 10 == 0)
               {
                  //log.info("Committing message");
                  //session.commit();
               }
            }

         }
         catch (Exception e)
         {
            this.exception = e;
            log.info("Caught exception... finishing Thread " + id, e);
         }
      }
   }

}
