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

import org.jboss.test.messaging.jms.clustering.base.ClusteringTestBase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.logging.Logger;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Destination;
import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Message;
import javax.jms.DeliveryMode;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision:$</tt>
 *
 * 
 * $Id:$
 */
public class HAStressTest extends ClusteringTestBase
{

   int NUMBER_OF_PRODUCER_THREADS=1;
   int NUMBER_OF_CONSUMER_THREADS=1;

   public HAStressTest(String name)
   {
      super(name);
   }

   int messageCounterConsumer = 0;
   int messageCounterProducer = 0;


   Object lockReader = new Object();
   Object lockWriter = new Object();
   Object semaphore = new Object();

   boolean shouldStop = false;


   class LocalThreadConsumer extends Thread
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
               semaphore.wait();
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
            log.info("Caught exception... finishing Thread " + id, e);
         }
      }
   }

   class LocalThreadProducer extends Thread
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
               semaphore.wait();
            }

            int counter = 0;
            while (!shouldStop)
            {
               log.trace("Producer ID=" + id + " send message");
               producer.send(session.createTextMessage("Message from producer " + id + " counter=" + (counter)));

               synchronized (lockWriter)
               {
                  messageCounterProducer++;
               }

               if (counter++ % 5 == 0)
               {
                  //log.info("Committing message");
                  //session.commit();
               }
            }

         }
         catch (Exception e)
         {
            log.info("Caught exception... finishing Thread " + id, e);
         }
      }
   }

   /**
    * This test will open several Consumers at the same Connection and it will kill the server, expecting failover
    * to happen inside the Valve
    */
   public void testMultiThreadFailover() throws Exception
   {
      // This test will be disabled until we implement the valve
      //JBossConnectionFactory factory = (JBossConnectionFactory) ic[1].lookup("/ConnectionFactory");

      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();

      log.info("Created connections");

      checkConnectionsDifferentServers(new Connection[]{conn1, conn2, conn3});

      Connection conn = getConnection(new Connection[]{conn1, conn2, conn3}, 1);
      conn.start();

      for (int i = 0; i < 3; i++)
      {
         JBossConnection connTest = (JBossConnection) getConnection(new Connection[]{conn1, conn2, conn3}, i);

         String locator = ((ClientConnectionDelegate) connTest.getDelegate()).getRemotingConnection().
            getInvokingClient().getInvoker().getLocator().getLocatorURI();

         log.info("Server " + i + " has locator=" + locator);

      }


      ArrayList threadList = new ArrayList();

      for (int i = 0; i < NUMBER_OF_PRODUCER_THREADS; i++)
      {
         threadList.add(new LocalThreadProducer(i, conn.createSession(false, Session.AUTO_ACKNOWLEDGE), queue[1]));
      }

      for (int i = 0; i < NUMBER_OF_CONSUMER_THREADS; i++)
      {
         threadList.add(new LocalThreadConsumer(i, conn.createSession(false, Session.AUTO_ACKNOWLEDGE), queue[1]));
      }

      for (Iterator iter = threadList.iterator(); iter.hasNext();)
      {
         Thread t = (Thread) iter.next();
         t.start();
      }

      Thread.sleep(1000);
      synchronized (semaphore)
      {
         semaphore.notifyAll();
      }

      Thread.sleep(30000);

      log.info("Killing server 1");
      ServerManagement.log(ServerManagement.INFO, "Server 1 will be killed");
      ServerManagement.log(ServerManagement.INFO, "Server 1 will be killed", 2);
      log.info("messageCounterConsumer=" + messageCounterConsumer + ", messageCounterProducer=" + messageCounterProducer);

      ServerManagement.kill(1);

      Thread.sleep(50000);
      log.info("messageCounterConsumer=" + messageCounterConsumer + ", messageCounterProducer=" + messageCounterProducer);
      shouldStop = true;

      for (Iterator iter = threadList.iterator(); iter.hasNext();)
      {
         Thread t = (Thread) iter.next();
         t.join();
      }

      log.info("messageCounterConsumer=" + messageCounterConsumer + ", messageCounterProducer=" + messageCounterProducer);

      assertEquals(messageCounterProducer, messageCounterConsumer);

      conn1.close();
      conn2.close();
      conn3.close();

   }

   // Protected -----------------------------------------------------

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

}
