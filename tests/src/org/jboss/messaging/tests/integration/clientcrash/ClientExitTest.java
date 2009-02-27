/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.clientcrash;

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.tests.util.SpawnedVMSupport;
import org.jboss.messaging.utils.SimpleString;

/**
 * A test that makes sure that a Messaging client gracefully exists after the last session is
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
public class ClientExitTest extends ClientTestBase
{
   // Constants ------------------------------------------------------------------------------------

   private static final String MESSAGE_TEXT = randomString();
   
   private static final SimpleString QUEUE = new SimpleString("ClientExitTestQueue");
      
   // Static ---------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientExitTest.class);

   // Attributes -----------------------------------------------------------------------------------

   private ClientSession session;

   private ClientConsumer consumer;   

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   public void testGracefulClientExit() throws Exception
   {
      // spawn a JVM that creates a JMS client, which sends a test message
      Process p = SpawnedVMSupport.spawnVM(GracefulClient.class.getName(), QUEUE.toString(), MESSAGE_TEXT);

      // read the message from the queue

      Message message = consumer.receive(15000);

      assertNotNull(message);
      assertEquals(MESSAGE_TEXT, message.getBody().readString());

      // the client VM should exit by itself. If it doesn't, that means we have a problem
      // and the test will timeout
      log.debug("waiting for the client VM to exit ...");
      p.waitFor();

      assertEquals(0, p.exitValue());
      
      // the local session
      assertActiveConnections(1);
      assertActiveSession(1);
      
      session.close();
      
      Thread.sleep(1000);
      assertActiveConnections(0);
      assertActiveSession(0);
   }

   // Package protected ----------------------------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.createQueue(QUEUE, QUEUE, null, false, false);
      consumer = session.createConsumer(QUEUE);
      session.start();
   }

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
