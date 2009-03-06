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

package org.jboss.messaging.tests.integration.client;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientRequestor;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * A ClientRequestorTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ClientRequestorTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testRequest() throws Exception
   {
      final SimpleString key = randomSimpleString();
      long value = randomLong();
      SimpleString requestAddress = randomSimpleString();
      SimpleString requestQueue = randomSimpleString();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      final ClientSession session = sf.createSession(false, true, true);

      session.start();
     
      session.createQueue(requestAddress, requestQueue, null, false, true);

      ClientConsumer requestConsumer = session.createConsumer(requestQueue);
      requestConsumer.setMessageHandler(new SimpleMessageHandler(key, session));

      ClientRequestor requestor = new ClientRequestor(session, requestAddress);
      ClientMessage request = session.createClientMessage(false);
      request.putLongProperty(key, value);

      ClientMessage reply = requestor.request(request, 500);
      assertNotNull("reply was not received", reply);
      assertEquals(value, reply.getProperty(key));

      session.close();
   }

   public void testTwoRequests() throws Exception
   {
      final SimpleString key = randomSimpleString();
      long value = randomLong();
      SimpleString requestAddress = randomSimpleString();
      SimpleString requestQueue = randomSimpleString();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      final ClientSession session = sf.createSession(false, true, true);

      session.start();
      
      session.createQueue(requestAddress, requestQueue, null, false, true);

      ClientConsumer requestConsumer = session.createConsumer(requestQueue);
      requestConsumer.setMessageHandler(new SimpleMessageHandler(key, session));

      ClientRequestor requestor = new ClientRequestor(session, requestAddress);
      ClientMessage request = session.createClientMessage(false);
      request.putLongProperty(key, value);

      ClientMessage reply = requestor.request(request, 500);
      assertNotNull("reply was not received", reply);
      assertEquals(value, reply.getProperty(key));

      request = session.createClientMessage(false);
      request.putLongProperty(key, value + 1);

      reply = requestor.request(request, 500);
      assertNotNull("reply was not received", reply);
      assertEquals(value + 1, reply.getProperty(key));

      session.close();
   }

   public void testRequestWithRequestConsumerWhichDoesNotReply() throws Exception
   {
      SimpleString requestAddress = randomSimpleString();
      SimpleString requestQueue = randomSimpleString();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      final ClientSession session = sf.createSession(false, true, true);

      session.start();
      
      session.createQueue(requestAddress, requestQueue, null, false, true);

      ClientConsumer requestConsumer = session.createConsumer(requestQueue);
      requestConsumer.setMessageHandler(new MessageHandler()
      {
         // return a message with the negative request's value
         public void onMessage(ClientMessage request)
         {
            // do nothing -> no reply
         }
      });

      ClientRequestor requestor = new ClientRequestor(session, requestAddress);
      ClientMessage request = session.createClientMessage(false);

      ClientMessage reply = requestor.request(request, 500);
      assertNull(reply);

      session.close();
   }

   public void testClientRequestorConstructorWithClosedSession() throws Exception
   {
      SimpleString requestAddress = randomSimpleString();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      final ClientSession session = sf.createSession(false, true, true);

      session.close();

      try
      {
         new ClientRequestor(session, requestAddress);
         fail("ClientRequestor's session must not be closed");
      }
      catch (Exception e)
      {
      }
   }

   public void testClose() throws Exception
   {
      final SimpleString key = randomSimpleString();
      long value = randomLong();
      SimpleString requestAddress = randomSimpleString();
      SimpleString requestQueue = randomSimpleString();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      final ClientSession session = sf.createSession(false, true, true);

      session.start();
      
      session.createQueue(requestAddress, requestQueue, null, false, true);

      ClientConsumer requestConsumer = session.createConsumer(requestQueue);
      requestConsumer.setMessageHandler(new SimpleMessageHandler(key, session));

      ClientRequestor requestor = new ClientRequestor(session, requestAddress);
      ClientMessage request = session.createClientMessage(false);
      request.putLongProperty(key, value);

      ClientMessage reply = requestor.request(request, 500);
      assertNotNull("reply was not received", reply);
      assertEquals(value, reply.getProperty(key));

      request = session.createClientMessage(false);
      request.putLongProperty(key, value + 1);

      requestor.close();

      try
      {
         reply = requestor.request(request, 500);
         fail("can not send a request on a closed ClientRequestor");
      }
      catch (Exception e)
      {

      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = Messaging.newNullStorageMessagingService(conf);
      service.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      service.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private final class SimpleMessageHandler implements MessageHandler
   {
      private final SimpleString key;

      private final ClientSession session;

      private SimpleMessageHandler(SimpleString key, ClientSession session)
      {
         this.key = key;
         this.session = session;
      }

      public void onMessage(ClientMessage request)
      {
         try
         {
            ClientMessage reply = session.createClientMessage(false);
            SimpleString replyTo = (SimpleString)request.getProperty(ClientMessageImpl.REPLYTO_HEADER_NAME);
            long value = (Long)request.getProperty(key);
            reply.putLongProperty(key, value);
            ClientProducer replyProducer = session.createProducer(replyTo);
            replyProducer.send(reply);
            request.acknowledge();
         }
         catch (MessagingException e)
         {
            e.printStackTrace();
         }
      }
   }
}
