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

package org.hornetq.tests.integration.client;

import static org.hornetq.tests.util.RandomUtil.randomSimpleString;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.Messaging;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/**
 * A MessageExpirationTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class MessageExpirationTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   private static final int EXPIRATION = 1000;

   // Attributes ----------------------------------------------------

   private MessagingServer server;

   private ClientSession session;
   
   private ClientSessionFactory sf;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testMessageExpiredWithoutExpiryAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, false);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.setExpiration(System.currentTimeMillis() + EXPIRATION);
      producer.send(message);

      Thread.sleep(EXPIRATION * 2);

      session.start();

      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message2 = consumer.receive(500);
      assertNull(message2);

      consumer.close();
      session.deleteQueue(queue);
   }

   public void testMessageExpiredWithExpiryAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      final SimpleString expiryAddress = randomSimpleString();
      SimpleString expiryQueue = randomSimpleString();

      session.createQueue(address, queue, false);
      session.createQueue(expiryAddress, expiryQueue, false);
      server.getAddressSettingsRepository().addMatch(address.toString(), new AddressSettings()
      {
         @Override
         public SimpleString getExpiryAddress()
         {
            return expiryAddress;
         }
      });

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      message.setExpiration(System.currentTimeMillis() + EXPIRATION);
      producer.send(message);

      Thread.sleep(EXPIRATION * 2);

      session.start();

      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message2 = consumer.receive(500);
      assertNull(message2);

      ClientConsumer expiryConsumer = session.createConsumer(expiryQueue);
      ClientMessage expiredMessage = expiryConsumer.receive(500);
      assertNotNull(expiredMessage);
      assertNotNull(expiredMessage.getProperty(MessageImpl.HDR_ACTUAL_EXPIRY_TIME));
      assertEquals(address, expiredMessage.getProperty(MessageImpl.HDR_ORIGINAL_DESTINATION));
      consumer.close();
      expiryConsumer.close();
      session.deleteQueue(queue);
      session.deleteQueue(expiryQueue);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);
      server.start();

      sf = createInVMFactory();
      session = sf.createSession(false, true, true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      sf.close();
      
      session.close();

      server.stop();
      
      session = null;
      
      server = null;
      
      sf = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
