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

package org.jboss.test.messaging.jms.crash.simultaneousfailure;

import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.logging.Logger;
import javax.naming.InitialContext;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.jms.TextMessage;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          <p/>
 *          $Id$
 */
public class FailingClient
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   static InitialContext ic;

   static Logger log = Logger.getLogger(FailingClient.class);

   static ConnectionFactory cf;

   static Topic destination;

   static Object sem = new Object();

   static boolean running = false;

   // Static ---------------------------------------------------------------------------------------

   public static void main(String arg[])
   {
      try
      {
         ServerManagement.create(0);
         ServerManagement.deployTopic("Topic");
         ic = new InitialContext(ServerManagement.getJNDIEnvironment());
         cf = (ConnectionFactory) ic.lookup("/ConnectionFactory");
         destination = (Topic) ic.lookup("/topic/Topic");

         ProducerThread producer = new ProducerThread("ProdThread" , cf, destination);
         producer.start();

         RunningThread threads[] = new RunningThread[20];
         for (int i = 0; i < threads.length; i++)
         {
            threads[i] = new RunningThread("FailingThread" + i , cf, destination);
         }

         for (int i = 0; i < threads.length; i++)
         {
            threads[i].start();
         }

         Thread.sleep(1000);

         synchronized (sem)
         {
            running = true;
            sem.notifyAll();
         }


         threads[0].join();

      }
      catch (Exception e)
      {
         log.error(e.getMessage(), e);
      }


   }

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   static class RunningThread extends Thread
   {

      ConnectionFactory cf;

      Topic destination;

      static Logger log = Logger.getLogger(RunningThread.class);

      public RunningThread(String name, ConnectionFactory cf, Topic destination)
      {
         super(name);
         this.cf = cf;
         this.destination = destination;
      }

      public RunningThread(ConnectionFactory cf, Topic destination)
      {
         super();
         this.cf = cf;
         this.destination = destination;
      }

      public void run()
      {
         try
         {
            synchronized (sem)
            {
               if (!running)
               {
                  sem.wait();
               }
            }
            log.info("Destination - " + destination);

            while (true)
            {
               Connection conn = null;
               conn = cf.createConnection();
               conn.setClientID(Thread.currentThread().getName());
               Session sess = conn.createSession(false,Session.AUTO_ACKNOWLEDGE);
               TopicSubscriber subscriber = sess.createDurableSubscriber(destination,
                  "subscription " + Thread.currentThread().getName());
               conn.start();

               log.info("Another iteration");
               for (int i=0;i<10;i++)
               {
                  TextMessage tst = (TextMessage)subscriber.receive(10000);
                  log.info(Thread.currentThread().getName() + " received " + tst.getText());
               }
               conn.close();

            }
         }
         catch (Exception e)
         {
            log.error(e.getMessage(), e);
         }
      }
   }

   static class ProducerThread extends Thread
   {

      ConnectionFactory cf;

      Topic destination;

      static Logger log = Logger.getLogger(RunningThread.class);

      public ProducerThread(String name, ConnectionFactory cf, Topic destination)
      {
         super(name);
         this.cf = cf;
         this.destination = destination;
      }

      public ProducerThread(ConnectionFactory cf, Topic destination)
      {
         super();
         this.cf = cf;
         this.destination = destination;
      }

      public void run()
      {
         try
         {
            synchronized (sem)
            {
               if (!running)
               {
                  sem.wait();
               }
            }
            log.info("Destination - " + destination);

            Connection conn = null;
            conn = cf.createConnection();
            conn.setClientID(Thread.currentThread().getName());
            Session sess = conn.createSession(false,Session.AUTO_ACKNOWLEDGE);
            MessageProducer prod = sess.createProducer(destination);
            conn.start();

            int i=0;

            while (true)
            {
               prod.send(sess.createTextMessage("Test " + i++));
            }
         }
         catch (Exception e)
         {
            log.error(e.getMessage(), e);
         }
      }
   }
}
