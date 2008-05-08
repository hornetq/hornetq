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
package org.jboss.messaging.tests.integration.core.remoting.impl;

import static org.jboss.messaging.core.remoting.TransportType.TCP;
import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.server.ConnectionManager;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;
import org.jboss.messaging.tests.unit.core.util.SpawnedVMSupport;
import org.jboss.messaging.util.SimpleString;

/**
 * A test that makes sure that a Messaging server cleans up the associated
 * resources when one of its client crashes.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision: 4032 $</tt>
 * 
 */
public class ClientCrashTest extends TestCase
{
   // Constants -----------------------------------------------------

   public static final SimpleString QUEUE = new SimpleString("ClientCrashTestQueue");
   public static final String MESSAGE_TEXT_FROM_SERVER = "ClientCrashTest from server";
   public static final String MESSAGE_TEXT_FROM_CLIENT = "ClientCrashTest from client";

   // Static --------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientCrashTest.class);

   // Attributes ----------------------------------------------------

   private MessagingServer server;
   private ClientConnectionFactory cf;

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
      ClientConnection connection = null;

      try
      {
         assertActiveConnections(0);

         // spawn a JVM that creates a JMS client, which waits to receive a test
         // message
         Process p = SpawnedVMSupport.spawnVM(CrashClient.class
               .getName(), new String[] { Integer
               .toString(numberOfConnectionsOnTheClient) });

         connection = cf.createConnection();
         ClientSession session = connection.createClientSession(false, true,
               true, -1, false, false);
         session.createQueue(QUEUE, QUEUE, null, false, false);
         ClientConsumer consumer = session.createConsumer(QUEUE, null, false, false, true);
         ClientProducer producer = session.createProducer(QUEUE);

         connection.start();

         // send the message to the queue
         Message messageFromClient = consumer.receive(5000);
         assertNotNull("no message received", messageFromClient);
         assertEquals(MESSAGE_TEXT_FROM_CLIENT, messageFromClient.getBody().getString());

         // 1 local connection to the server
         // + 1 per connection to the client
         assertActiveConnections(1 + numberOfConnectionsOnTheClient);

         MessageImpl message = new MessageImpl(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.getBody().putString(ClientCrashTest.MESSAGE_TEXT_FROM_SERVER);
         producer.send(message);

         log.info("waiting for the client VM to crash ...");
         p.waitFor();

         assertEquals(9, p.exitValue());

         Thread.sleep(2000);
         // the crash must have been detected and the client resources cleaned
         // up only the local connection remains
         assertActiveConnections(1);

         connection.close();

         assertActiveConnections(0);
      } finally
      {
         try
         {
            if (connection != null)
               connection.close();
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

      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration("localhost", ConfigurationImpl.DEFAULT_REMOTING_PORT);
      server = new MessagingServerImpl(config);
      server.start();

      cf = new ClientConnectionFactoryImpl(new LocationImpl(TCP, "localhost", ConfigurationImpl.DEFAULT_REMOTING_PORT));
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();

      super.tearDown();
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void assertActiveConnections(int expectedActiveConnections)
         throws Exception
   {
      ConnectionManager cm = server.getConnectionManager();
      assertEquals(expectedActiveConnections, cm.getActiveConnections().size());
   }

   // Inner classes -------------------------------------------------

}
