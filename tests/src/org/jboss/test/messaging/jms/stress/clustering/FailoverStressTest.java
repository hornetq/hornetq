/*
 * JBoss, Home of Professional Open Source
 * Copyright 2007, JBoss Inc., and individual contributors as indicated
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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.jboss.jms.client.FailoverEvent;
import org.jboss.jms.client.FailoverListener;
import org.jboss.jms.client.JBossConnection;
import org.jboss.test.messaging.jms.clustering.ClusteringTestBase;
import org.jboss.test.messaging.tools.ServerManagement;

import EDU.oswego.cs.dl.util.concurrent.SynchronizedInt;

/**
 * @author <a href="sergey.koshcheyev@jboss.com">Sergey Koshcheyev</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class FailoverStressTest extends ClusteringTestBase
{
   // Constants -----------------------------------------------------

   // The number of concurrent connections (worker threads)
   private final int CONNECTION_COUNT = 20;
   
   // The node that connections will initially connect to
   private final int CONNECT_NODE = 1;
   
   // The node the connections are expected to fail over to
   private final int FAILOVER_NODE = 0;

   // The number of messages each worker thread will send or receive
   private final int MESSAGE_COUNT_PER_CONNECTION = 100;
   
   // The interval between sending messages (for sender threads)
   private final long SLEEP_PER_MESSAGE = 100L;

   // The sleep interval between the time connection threads
   // are started and the time the server is killed. 
   private final long PAUSE_BEFORE_KILL = MESSAGE_COUNT_PER_CONNECTION * SLEEP_PER_MESSAGE / 2;
   
   // Time in which all threads are supposed to fail over
   private final long FAILOVER_TIMEOUT = 15000L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------
   
   public FailoverStressTest(String name)
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
   public void testFailoverManyConnections() throws Exception
   {
      Connection connections[] = new Connection[CONNECTION_COUNT];
      ConnectionWorker workers[] = new ConnectionWorker[CONNECTION_COUNT];
      
      try
      {
         log.info("creating " + CONNECTION_COUNT + " threads to connect to server " + CONNECT_NODE);

         for (int i = 0; i < CONNECTION_COUNT; i++)
         {
            connections[i] = createConnectionOnServer(cf, CONNECT_NODE);
            if (i % 2 == 0)
            {
               workers[i] = new ConnectionSenderThread(i, connections[i], queue[CONNECT_NODE]);
            }
            else
            {
               workers[i] = new ConnectionReceiverThread(i, connections[i], queue[CONNECT_NODE]);
            }
         }
         
         for (int i = 0; i < CONNECTION_COUNT; i++)
         {
            workers[i].start();
         }
         
         log.info("waiting for a few seconds so that threads begin working");
         
         Thread.sleep(PAUSE_BEFORE_KILL);

         log.info("killing node " + CONNECT_NODE);

         killAndWaitForFailover(connections);
         
         for (int i = 0; i < CONNECTION_COUNT; i++)
         {
            assertEquals("Connection #" + i + " did not fail over", FAILOVER_NODE, getServerId(connections[i]));
         }

         log.info("waiting for connection threads to finish");

         boolean fail = false;
         for (int i = 0; i < CONNECTION_COUNT; i++)
         {
            workers[i].join();
            Exception e = workers[i].getException();
            if (e != null)
            {
               log.error("Thread " + workers[i].getName() + " terminated abnormally:", e);
               fail = true;
            }
         }
         
         if (fail) { fail("Some threads terminated abnormally"); }
      }
      finally
      {
         for (int i = 0; i < CONNECTION_COUNT; i++)
         {
            if (connections[i] != null)
            {
               connections[i].close();
            }
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   private void killAndWaitForFailover(Connection[] connections) throws Exception
   {
      // register a failover listener
      LoggingFailoverListener failoverListener = new LoggingFailoverListener();
      
      for (int i = 0; i < connections.length; i++)
      {
         ((JBossConnection)connections[i]).registerFailoverListener(failoverListener);
      }

      int serverId = getServerId(connections[0]);

      ServerManagement.killAndWait(serverId);

      log.info("killed node " + serverId + ", now waiting for all connections to fail over");
      
      long killTime = System.currentTimeMillis();
      while (failoverListener.getFailoverCount() < connections.length
            && System.currentTimeMillis() - killTime <= FAILOVER_TIMEOUT)
      {
         Thread.sleep(3000L);
      }
      
      assertEquals("Not all connections have failed over successfully",
            connections.length, failoverListener.getFailoverCount());
   }

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

   private class ConnectionSenderThread extends ConnectionWorker
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
            Thread.sleep(SLEEP_PER_MESSAGE);
            producer.send(session.createTextMessage("Hello"));
         }
      }
   }
   
   private class ConnectionReceiverThread extends ConnectionWorker
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
            Message message = consumer.receive(2 * SLEEP_PER_MESSAGE);
            if (message == null)
            {
               break;
            }
         }
      }
   }
}
