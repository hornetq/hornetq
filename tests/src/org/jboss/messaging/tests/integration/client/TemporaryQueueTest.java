/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.client;

import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.ServerSessionImpl;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * A TemporaryQueueTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
/**
 * A TemporaryQueueTest
 *
 * @author jmesnil
 *
 *
 */
public class TemporaryQueueTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingServer server;

   private ClientSession session;

   private ClientSessionFactoryImpl sf;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testConsumeFromTemporaryQueue() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createTemporaryQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      assertNotNull(message);
      message.acknowledge();

      consumer.close();
      session.deleteQueue(queue);

      session.close();
   }

   public void testConsumeFromTemporaryQueueCreatedByOtherSession() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createTemporaryQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      ClientSession session2 = sf.createSession(false, true, true);
      session2.start();

      ClientConsumer consumer = session2.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      assertNotNull(message);

      session2.close();
      session.close();
   }

   public void _testDeleteTemporaryQueueAfterConnectionIsClosed() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createTemporaryQueue(address, queue);

      session.close();

      session = sf.createSession(false, true, true);
      session.start();
      
      try
      {
         session.createConsumer(queue);
         fail("temp queue must not exist after the remoting connection is closed");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.QUEUE_DOES_NOT_EXIST, e.getCode());
      }

      session.close();
   }
   
   /**
    * @see ServerSessionImpl#doHandleCreateQueue()
    */
   public void _testDeleteTemporaryQueueAfterConnectionIsClosed_2() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createTemporaryQueue(address, queue);
      assertEquals(1, server.getConnectionCount());
      
      // we create a second session. the temp queue must be present
      // even after we closed the session which created it
      ClientSession session2 = sf.createSession(false, true, true);

      session.close();

      // let some time for the server to clean the connections
      Thread.sleep(1000);

      session2.start();
      
      ClientConsumer consumer = session2.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      assertNotNull(message);

      session2.close();
   }

   public void testDeleteTemporaryQueueWhenClientCrash() throws Exception
   {
      final SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createTemporaryQueue(address, queue);

      assertEquals(1, server.getConnectionCount());

      RemotingConnection remotingConnection = server
                                                     .getRemotingService()
                                                     .getConnections()
                                                     .iterator()
                                                     .next();
      remotingConnection.fail(new MessagingException(MessagingException.INTERNAL_ERROR, "simulate a client failure"));

      // let some time for the server to clean the connections
      Thread.sleep(1000);

      assertEquals(0, server.getConnectionCount());

      sf.close();
      sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.start();
      
      expectMessagingException("temp queue must not exist after the server detected the client crash", MessagingException.QUEUE_DOES_NOT_EXIST, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            session.createConsumer(queue);
         }
      });

      session.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration config = new ConfigurationImpl();
      config.setSecurityEnabled(false);
      server = Messaging.newMessagingServer(config);
      server.start();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();

      server.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
