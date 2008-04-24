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
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;
import org.jboss.messaging.tests.unit.core.util.SpawnedVMSupport;

/**
 * A test that makes sure that a Messaging client gracefully exists after the last connection is
 * closed. Test for http://jira.jboss.org/jira/browse/JBMESSAGING-417.
 *
 * This is not technically a crash test, but it uses the same type of topology as the crash tests
 * (local server, remote VM client).
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * $Id$
 */
public class ClientExitTest extends TestCase
{
   // Constants ------------------------------------------------------------------------------------

   public static final String MESSAGE_TEXT = "kolowalu";

   public static final String QUEUE = "ClientExitTestQueue";
      
   // Static ---------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientExitTest.class);

   // Attributes -----------------------------------------------------------------------------------

   private MessagingServer server;

   private ClientConnection connection;

   private ClientConsumer consumer;   

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   public void testGracefulClientExit() throws Exception
   {
      // spawn a JVM that creates a JMS client, which sends a test message
      Process p = SpawnedVMSupport.spawnVM(GracefulClient.class.getName());

      // read the message from the queue

      Message message = consumer.receive(15000);

      assertNotNull(message);
      assertEquals(MESSAGE_TEXT, new String(message.getPayload()));

      // the client VM should exit by itself. If it doesn't, that means we have a problem
      // and the test will timeout
      log.info("waiting for the client VM to exit ...");
      p.waitFor();

      assertEquals(0, p.exitValue());
   }

   // Package protected ----------------------------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      ConfigurationImpl config = ConfigurationHelper.newConfiguration(TCP,
            "localhost", ConfigurationImpl.DEFAULT_REMOTING_PORT);
      server = new MessagingServerImpl(config);
      server.start();

      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(new LocationImpl(TCP, "localhost", ConfigurationImpl.DEFAULT_REMOTING_PORT));
      connection = cf.createConnection(null, null);
      ClientSession session = connection.createClientSession(false, true, true, -1, false, false);
      session.createQueue(QUEUE, QUEUE, null, false, false);
      consumer = session.createConsumer(QUEUE, null, false, false, true);
      connection.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      consumer.close();
      connection.close();

      server.stop();

      super.tearDown();
   }
   
   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
