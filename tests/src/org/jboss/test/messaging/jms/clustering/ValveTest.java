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
import org.jboss.logging.Logger;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Destination;
import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Message;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision:$</tt>
 *          <p/>
 *          $Id:$
 */
public class ValveTest extends ClusteringTestBase
{

   public ValveTest(String name)
   {
      super(name);
   }

   int messageCounterConsumer =0;
   int messageCounterProducer=0;


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
               Message message = consumer.receive(50);
               if (message==null && shouldStop)
               {
                  break;
               }
               if (message!=null)
               {
                  synchronized (lockReader)
                  {
                     messageCounterConsumer++;
                  }
                  log.trace("ReceiverID=" + id + " received message " + message);
                  if (counter++ % 10 == 0)
                  {
                     //log.info("Commit on id=" + id);
                     session.commit();
                  }
               }
            }
            session.commit();
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
         this.session = session;
         producer = session.createProducer(destination);
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
                  session.commit();
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
      JBossConnectionFactory factory = (JBossConnectionFactory) ic[1].lookup("/ConnectionFactory");
      Connection conn = factory.createConnection();
      conn.start();

      ArrayList list = new ArrayList();

      for (int i = 0; i < 5; i++)
      {
         list.add(new LocalThreadProducer(i, conn.createSession(true, Session.AUTO_ACKNOWLEDGE), queue[1]));
         list.add(new LocalThreadConsumer(i, conn.createSession(true, Session.AUTO_ACKNOWLEDGE), queue[1]));
      }

      for (Iterator iter = list.iterator(); iter.hasNext();)
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

      ServerManagement.kill(1);

      Thread.sleep(50000);
      shouldStop=true;

      for (Iterator iter = list.iterator(); iter.hasNext();)
      {
         Thread t = (Thread) iter.next();
         t.join();
      }

      log.info("produced " + messageCounterProducer + " and read " + messageCounterConsumer);

      assertEquals(messageCounterProducer, messageCounterConsumer);


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
