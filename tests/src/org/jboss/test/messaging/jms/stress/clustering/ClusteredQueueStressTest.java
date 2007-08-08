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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.jms.clustering.ClusteringTestBase;

import EDU.oswego.cs.dl.util.concurrent.SynchronizedInt;

public class ClusteredQueueStressTest extends ClusteringTestBase
{
   // Constants -----------------------------------------------------

   private static Logger log = Logger.getLogger(ClusteredQueueStressTest.class);


   // Static --------------------------------------------------------

   private static final int NODE_COUNT = 10;

   private static final int NUM_MESSAGES = 500;

   private static SynchronizedInt receivedMessages = new SynchronizedInt(0);
   private static SynchronizedInt sentMessages = new SynchronizedInt(0);

   // Attributes ----------------------------------------------------

   private Set listeners = new HashSet();

   private volatile boolean failed;

   // Constructors --------------------------------------------------

   public ClusteredQueueStressTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   protected void setUp() throws Exception
   {
   	this.nodeCount = NODE_COUNT;

      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testQueue() throws Throwable
   {
   	Connection[] conns = new Connection[nodeCount];

   	try
   	{
   		for (int i = 0; i < nodeCount; i++)
   		{
   			conns[i] = cf.createConnection();
   		}

   		this.checkConnectionsDifferentServers(conns);

   		for (int i = 0; i < nodeCount; i++)
   		{
   			Session sess = conns[i].createSession(false, Session.AUTO_ACKNOWLEDGE);

   			MessageConsumer cons = sess.createConsumer(queue[i]);

   			ClusteredQueueStressTest.MyListener listener =
                               new ClusteredQueueStressTest.MyListener(i%2==0?10000:0, i);

            listeners.add(listener);

   			cons.setMessageListener(listener);

   			conns[i].start();

   			log.info("Created " + i);
   		}


         ProducerThread producers[] = new ProducerThread[nodeCount];

         for (int i=0;i<producers.length;i++)
         {
            Connection conn = cf.createConnection();
            producers[i] = new ProducerThread(conn, i);
            producers[i].start();
         }


         for (int i=0; i<producers.length; i++)
         {
            producers[i].join();
         }

         for (int i=0;i<6;i++)
         {
            Iterator iter = listeners.iterator();
            while (iter.hasNext())
            {
               MyListener  listener = (MyListener)iter.next();
               if (listener.isFailed())
               {
                  fail("One of the listeners failed!");
               }
            }

            for (int j=0;j<producers.length;j++)
            {
               if (producers[j].isFailed())
               {
                  fail("One of the producers failed!");
               }
            }

            if (sentMessages.get() == receivedMessages.get())
            {
               return;
            }

            Thread.sleep(10000);

         }

      }
   	finally
   	{
   		for (int i = 0; i < nodeCount; i++)
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
   				log.error("Failed to close connection", t);
   			}
   		}
   	}

      
   }

   private class ProducerThread extends Thread
   {
      Connection conn;

      int counter = 0;

      boolean failed = false;

      public ProducerThread(Connection conn, int threadId)
      {
         super("Producer " + threadId);
         this.conn = conn;   
      }

      public synchronized boolean isFailed()
      {
         return failed;
      }

      public synchronized void setFailed(boolean failed)
      {
         this.failed = failed;
      }


      public void run()
      {
         try
         {
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer prod = session.createProducer(queue[0]);

            for (int i=0;i<NUM_MESSAGES;i++)
            {
               if (i%10==0)
               {
                  log.info(Thread.currentThread().getName() + " has sent " + i + " messages");
               }

               prod.send(session.createTextMessage("Text " + i));
               counter ++;
               sentMessages.increment();
            }
            
         }
         catch (JMSException e)
         {
            log.error(e);
            setFailed(true);
         }
         finally
         {
            try
            {
               conn.close();
            }
            catch (JMSException e)
            {
               log.error(e);
            }
         }


      }
   }

   private class MyListener implements MessageListener
   {
      int counter = 0;

      int threadId=0;

      boolean failed = false;

      int waitTime = 0;


      public synchronized boolean isFailed()
      {
         return failed;
      }

      public synchronized void setFailed(boolean failed)
      {
         this.failed = failed;
      }

      public MyListener(int waitTime, int threadId)
      {
         this.waitTime=waitTime;
         this.threadId = threadId;
      }


      public void onMessage(Message msg)
		{
         receivedMessages.increment();
         counter++;

         if (counter%10==0)
         {
            log.info("Consumer " + threadId + " has received " + counter + " messages");
         }

         if (waitTime>0)
         {
            try
            {
               Thread.sleep(waitTime);
            } catch (InterruptedException e)
            {
               setFailed(true);
            }
         }
		}

   }

}
