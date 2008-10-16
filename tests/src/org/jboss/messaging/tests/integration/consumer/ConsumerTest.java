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
package org.jboss.messaging.tests.integration.consumer;

import junit.framework.TestCase;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ConsumerTest extends TestCase
{
   public void testConsumerAsSimpleBrowser() throws Exception
   {
      final SimpleString QUEUE = new SimpleString("CoreClientTestQueue");

      Configuration conf = new ConfigurationImpl();

      conf.setSecurityEnabled(false);

      conf.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));

      MessagingService messagingService = MessagingServiceImpl.newNullStorageMessagingServer(conf);

      messagingService.start();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sf.createSession(false, true, true, false);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createBrowser(QUEUE);
      consumer.start();
      int i = 0;
      while(consumer.awaitMessage(0))
      {
         ClientMessage m = consumer.receiveImmediate();
         assertEquals(m.getBody().getString(), "m" + i);
         i++;
      }
      assertEquals(100, i);
      consumer.restart();
      i = 0;
      while(consumer.awaitMessage(0))
      {
         ClientMessage m = consumer.receiveImmediate();
         assertEquals(m.getBody().getString(), "m" + i);
         i++;
      }
      assertEquals(100, i);
      session.close();
      messagingService.stop();
   }

   public void testConsumerAsSimpleBrowserReset() throws Exception
   {
      final SimpleString QUEUE = new SimpleString("CoreClientTestQueue");

      Configuration conf = new ConfigurationImpl();

      conf.setSecurityEnabled(false);

      conf.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));

      MessagingService messagingService = MessagingServiceImpl.newNullStorageMessagingServer(conf);

      messagingService.start();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sf.createSession(false, true, true, false);

      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createBrowser(QUEUE);
      consumer.start();
      int i = 0;
      while(consumer.awaitMessage(0))
      {
         ClientMessage m = consumer.receiveImmediate();
         assertEquals(m.getBody().getString(), "m" + i);
         i++;
         if(i == 50)
         {
            break;
         }
      }
      assertEquals(50, i);
      consumer.restart();
      i = 0;
      while(consumer.awaitMessage(0))
      {
         ClientMessage m = consumer.receiveImmediate();
         assertEquals(m.getBody().getString(), "m" + i);
         i++;
      }
      assertEquals(100, i);
      session.close();
      messagingService.stop();
   }

   private ClientMessage createMessage(ClientSession session, String body)
   {
      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
      message.getBody().putString(body);
      message.getBody().flip();
      return message;
   }
}
