/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.client;

import static org.hornetq.tests.util.RandomUtil.randomLong;
import static org.hornetq.tests.util.RandomUtil.randomSimpleString;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientRequestor;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/**
 * A ClientRequestorTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class RequestorTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer service;
   
   private ClientSessionFactory sf;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testRequest() throws Exception
   {
      final SimpleString key = randomSimpleString();
      long value = randomLong();
      SimpleString requestAddress = randomSimpleString();
      SimpleString requestQueue = randomSimpleString();

      final ClientSession session = sf.createSession(false, true, true);

      session.start();
     
      session.createTemporaryQueue(requestAddress, requestQueue);

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
      
      session.createTemporaryQueue(requestAddress, requestQueue);

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
      
      session.createTemporaryQueue(requestAddress, requestQueue);

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
      final SimpleString requestAddress = randomSimpleString();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      final ClientSession session = sf.createSession(false, true, true);

      session.close();

      expectHornetQException("ClientRequestor's session must not be closed", HornetQException.OBJECT_CLOSED, new HornetQAction(){
         public void run() throws Exception
         {
            new ClientRequestor(session, requestAddress);
         }
      });
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
      
      session.createTemporaryQueue(requestAddress, requestQueue);

      ClientConsumer requestConsumer = session.createConsumer(requestQueue);
      requestConsumer.setMessageHandler(new SimpleMessageHandler(key, session));

      final ClientRequestor requestor = new ClientRequestor(session, requestAddress);
      ClientMessage request = session.createClientMessage(false);
      request.putLongProperty(key, value);

      ClientMessage reply = requestor.request(request, 500);
      assertNotNull("reply was not received", reply);
      assertEquals(value, reply.getProperty(key));

      request = session.createClientMessage(false);
      request.putLongProperty(key, value + 1);

      requestor.close();

      expectHornetQException("can not send a request on a closed ClientRequestor", HornetQException.OBJECT_CLOSED, new HornetQAction(){

         public void run() throws Exception
         {
            requestor.request(session.createClientMessage(false), 500);
         }
      });
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
      service = HornetQ.newHornetQServer(conf, false);
      service.start();
      
      sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
   }

   @Override
   protected void tearDown() throws Exception
   {
      service.stop();
      
      sf.close();
      
      sf = null;
      
      service = null;

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
         catch (HornetQException e)
         {
            e.printStackTrace();
         }
      }
   }
}
