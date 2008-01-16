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

package org.jboss.test.messaging.jms.stress.clustering;

import org.jboss.test.messaging.jms.clustering.ClusteringTestBase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.FailoverListener;
import org.jboss.jms.client.FailoverEvent;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Message;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedInt;

/**
 * This test was added to duplicate this issue: http://jira.jboss.org/jira/browse/JBMESSAGING-1074
 * @author <a href="sergey.koshcheyev@jboss.com">Sergey Koshcheyev</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class FailoverOverDistributionStressTest extends ClusteringTestBase
{
   // Constants -----------------------------------------------------

   // The number of concurrent connections (worker threads)
   private final int CONNECTION_COUNT = 20;

   // The node that connections will initially connect to
   private final int CONNECT_NODE = 1;

   // The node the connections are expected to fail over to
   private final int FAILOVER_NODE = 0;

   // The number of messages each worker thread will send or receive
   private final int MESSAGE_COUNT_PER_CONNECTION = 1000;

   // Time in which all threads are supposed to fail over
   private final long FAILOVER_TIMEOUT = 45000L;

   private static int produced = 0;
   private static int consumed = 0;

   private static synchronized void incProduced()
   {
      produced++;
   }

   private static synchronized int getProduced()
   {
      return produced;
   }

   private static synchronized int getConsumed()
   {
      return consumed;
   }

   private static synchronized void incConsumed()
   {
      consumed++;
   }

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public FailoverOverDistributionStressTest(String name)
   {
      super(name);
      this.nodeCount = 2;
   }

   // Public --------------------------------------------------------

   /**
    * Establishes many connections to a single node, each connection
    * either sends or receives messages from a queue. Then kills the node.
    * All connections should successfully fail over to the failover node.
    */
//   public void testFailoverManyConnections() throws Exception
//   {
//      produced = 0;
//      consumed = 0;
//
//      Connection connections[] = new Connection[CONNECTION_COUNT];
//      FailoverOverDistributionStressTest.ConnectionWorker workers[] = new FailoverOverDistributionStressTest.ConnectionWorker[CONNECTION_COUNT];
//
//      try
//      {
//         log.info("creating " + CONNECTION_COUNT + " threads to connect to server " + CONNECT_NODE);
//
//         for (int i = 0; i < CONNECTION_COUNT; i++)
//         {
//            if (i % 2 == 0)
//            {
//               connections[i] = createConnectionOnServer(cf, 1);
//               workers[i] = new FailoverOverDistributionStressTest.ConnectionSenderThread(i, connections[i], queue[CONNECT_NODE]);
//            }
//            else
//            {
//               connections[i] = createConnectionOnServer(cf, 0);
//               workers[i] = new FailoverOverDistributionStressTest.ConnectionReceiverThread(i, connections[i], queue[CONNECT_NODE]);
//            }
//         }
//
//         for (int i = 0; i < CONNECTION_COUNT; i++)
//         {
//            workers[i].start();
//         }
//
//         log.info("waiting for a few seconds so that threads begin working");
//
//         while (true)
//         {
//            int produced = getProduced();
//            if (produced > (MESSAGE_COUNT_PER_CONNECTION * (CONNECTION_COUNT/2))/2 )
//            {
//               killAndWaitForFailover(connections);
//               break;
//            }
//            else
//            {
//               log.info("Produced " + produced + " messages.. Consumed (" + getConsumed() + ")" );
//            }
//            Thread.sleep(500);
//         }
//
//         for (int i = 0; i < CONNECTION_COUNT; i++)
//         {
//            assertEquals("Connection #" + i + " did not fail over", FAILOVER_NODE, getServerId(connections[i]));
//         }
//
//         log.info("waiting for connection threads to finish");
//
//         boolean fail = false;
//         for (int i = 0; i < CONNECTION_COUNT; i++)
//         {
//            workers[i].join();
//            Exception e = workers[i].getException();
//            if (e != null)
//            {
//               log.error("Thread " + workers[i].getName() + " terminated abnormally:", e);
//               fail = true;
//            }
//         }
//
//         if (fail) { fail("Some threads terminated abnormally"); }
//
//         assertEquals(produced, consumed);
//      }
//      finally
//      {
//         for (int i = 0; i < CONNECTION_COUNT; i++)
//         {
//            if (connections[i] != null)
//            {
//               connections[i].close();
//            }
//         }
//      }
//   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

//   private void killAndWaitForFailover(Connection[] connections) throws Exception
//   {
//      // register a failover listener
//      FailoverOverDistributionStressTest.LoggingFailoverListener failoverListener = new FailoverOverDistributionStressTest.LoggingFailoverListener();
//
//      int numConnections=0;
//      for (int i = 0; i < connections.length; i++)
//      {
//         if (getServerId(connections[i]) == 1)
//         {
//            ((JBossConnection)connections[i]).registerFailoverListener(failoverListener);
//            numConnections ++;
//         }
//      }
//
//      ServerManagement.kill(1);
//
//      log.info("killed node 1, now waiting for all connections to fail over");
//
//      long killTime = System.currentTimeMillis();
//      while (failoverListener.getFailoverCount() < numConnections
//            && System.currentTimeMillis() - killTime <= FAILOVER_TIMEOUT)
//      {
//         Thread.sleep(3000L);
//      }
//
//      assertEquals("Not all connections have failed over successfully",
//            numConnections, failoverListener.getFailoverCount());
//   }

   // Inner classes -------------------------------------------------

   private class LoggingFailoverListener implements FailoverListener
   {
      private final SynchronizedInt count = new SynchronizedInt(0);

      public void failoverEventOccured(FailoverEvent event)
      {
         if (FailoverEvent.FAILOVER_COMPLETED != event.getType())
         {
            // Not interested
            return;
         }

         int newCount = count.increment();

         log.info("received FAILOVER_COMPLETED event, " + newCount + " connections failed over so far");
      }

      public int getFailoverCount()
      {
         return count.get();
      }
   }

   private abstract class ConnectionWorker extends Thread
   {
      private volatile Exception exception;
      private final Connection connection;

      public ConnectionWorker(String name, Connection connection)
      {
         super(name);
         this.connection = connection;
      }

      public void run()
      {
         Session session = null;
         try
         {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            connection.start();
            runInSession(session);
         }
         catch (Exception e)
         {
            this.exception = e;
         }
         finally
         {
            if (session != null)
            {
               try { session.close(); } catch(JMSException e) { }
            }
         }
      }

      protected abstract void runInSession(Session session) throws Exception;

      public Exception getException()
      {
         return exception;
      }
   }

   private class ConnectionSenderThread extends FailoverOverDistributionStressTest.ConnectionWorker
   {
      private final Queue queue;

      public ConnectionSenderThread(int i, Connection connection, Queue queue)
      {
         super("ConnectionSenderThread#" + i, connection);
         this.queue = queue;
      }

      public void runInSession(Session session) throws Exception
      {
         MessageProducer producer = session.createProducer(queue);

         for (int i = 0; i < MESSAGE_COUNT_PER_CONNECTION; i++)
         {
            producer.send(session.createTextMessage("Hello"));
            incProduced();
         }
      }
   }

   private class ConnectionReceiverThread extends FailoverOverDistributionStressTest.ConnectionWorker
   {
      private final Queue queue;

      public ConnectionReceiverThread(int i, Connection connection, Queue queue)
      {
         super("ConnectionReceiverThread#" + i, connection);
         this.queue = queue;
      }

      public void runInSession(Session session) throws Exception
      {
         MessageConsumer consumer = session.createConsumer(queue);

         for (int i = 0; i < MESSAGE_COUNT_PER_CONNECTION; i++)
         {
            Message message = consumer.receive(20000);
            if (message != null)
            {
               log.info("Received message");
               incConsumed();
            }
            else
            {
               break;
            }
         }
      }
   }
}
