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
package org.jboss.test.messaging.jms.crash;

import java.io.File;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.jms.server.ConnectionManager;
import org.jboss.messaging.util.Logger;
import org.jboss.test.messaging.JBMServerTestCase;
import org.jboss.test.messaging.jms.SerializedClientSupport;


/**
 * A test that makes sure that a Messaging server cleans up the associated
 * resources when one of its client crashes.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ClientCrashTest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------

   public static final String SERIALIZED_CF_FILE_NAME = "ClientCrashTest_CFandQueue.ser";
   public static final String MESSAGE_TEXT_FROM_SERVER = "ClientCrashTest from server";
   public static final String MESSAGE_TEXT_FROM_CLIENT = "ClientCrashTest from client";

   // Static --------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientCrashTest.class);

   // Attributes ----------------------------------------------------

   private File serialized;
   private ConnectionFactory cf;
   private Queue queue;

   // Constructors --------------------------------------------------

   public ClientCrashTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testCrashClientWithOneConnection() throws Exception
   {
      crashClient(1);
   }

   public void testCrashClientWithTwoConnections() throws Exception
   {
      crashClient(2);
   }

   public void crashClient(int numberOfConnectionsOnTheClient) throws Exception
   {
      Connection conn = null;

      try
      {
         assertActiveConnections(0);

         // spawn a JVM that creates a JMS client, which waits to receive a test
         // message
         Process p = SerializedClientSupport.spawnVM(CrashClient.class
               .getName(), new String[] { serialized.getAbsolutePath(),
               Integer.toString(numberOfConnectionsOnTheClient) });

         // send the message to the queue
         conn = cf.createConnection();
         conn.start();
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = sess.createProducer(queue);
         MessageConsumer consumer = sess.createConsumer(queue);

         TextMessage messageFromClient = (TextMessage) consumer.receive(5000);
         assertEquals(MESSAGE_TEXT_FROM_CLIENT, messageFromClient.getText());

         // 1 local connection to the server
         // + 1 per connection to the client
         assertActiveConnections(1 + numberOfConnectionsOnTheClient);

         producer.send(sess.createTextMessage(MESSAGE_TEXT_FROM_SERVER));

         log.info("waiting for the client VM to crash ...");
         p.waitFor();

         assertEquals(9, p.exitValue());

         Thread.sleep(2000);
         // the crash must have been detected and the client resources cleaned
         // up only the local connection remains
         assertActiveConnections(1);

         conn.close();

         assertActiveConnections(0);
      } finally
      {
         try
         {
            if (conn != null)
               conn.close();
         } catch (Throwable ignored)
         {
            log.warn("Exception ignored:" + ignored.toString(), ignored);
         }
      }
   }

   // Package protected ---------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      createQueue("Queue");

      InitialContext ic = getInitialContext();
      cf = (ConnectionFactory) ic.lookup("/ConnectionFactory");
      queue = (Queue) ic.lookup("/queue/Queue");

      serialized = SerializedClientSupport.writeToFile(SERIALIZED_CF_FILE_NAME,
            cf, queue);
   }

   @Override
   protected void tearDown() throws Exception
   {
      servers.get(0).destroyQueue("Queue", "/queue/Queue");

      if (serialized != null)
      {
         serialized.delete();
      }

      super.tearDown();
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static void assertActiveConnections(int expectedActiveConnections)
         throws Exception
   {
      ConnectionManager cm = servers.get(0).getMessagingServer()
            .getConnectionManager();
      assertEquals(expectedActiveConnections, cm.getActiveConnections().size());
   }

   // Inner classes -------------------------------------------------

}
