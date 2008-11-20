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
package org.jboss.messaging.tests.integration.queue;

import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.util.SimpleString;


/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ExpiryAddressTest extends UnitTestCase
{
   private MessagingService messagingService;

   private ClientSession clientSession;

   public void testBasicSend() throws Exception
   {
      SimpleString ea = new SimpleString("EA");
      SimpleString qName = new SimpleString("q1");
      SimpleString eq = new SimpleString("EA1");
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setExpiryAddress(ea);
      messagingService.getServer().getQueueSettingsRepository().addMatch(qName.toString(), queueSettings);
      clientSession.createQueue(ea, eq, null, false, false, false);
      clientSession.createQueue(qName, qName, null, false, false, false);
      ClientProducer producer = clientSession.createProducer(qName);
      ClientMessage clientMessage = createTextMessage("heyho!", clientSession);
      clientMessage.setExpiration(System.currentTimeMillis());
      producer.send(clientMessage);
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      assertNull(m);
      m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();
      clientConsumer = clientSession.createConsumer(eq);
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "heyho!");
   }

   public void testBasicSendToMultipleQueues() throws Exception
   {
      SimpleString ea = new SimpleString("EA");
      SimpleString qName = new SimpleString("q1");
      SimpleString eq = new SimpleString("EQ1");
      SimpleString eq2 = new SimpleString("EQ2");
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setExpiryAddress(ea);
      messagingService.getServer().getQueueSettingsRepository().addMatch(qName.toString(), queueSettings);
      clientSession.createQueue(ea, eq, null, false, false, true);
      clientSession.createQueue(ea, eq2, null, false, false, true);
      clientSession.createQueue(qName, qName, null, false, false, true);
      ClientProducer producer = clientSession.createProducer(qName);
      ClientMessage clientMessage = createTextMessage("heyho!", clientSession);
      clientMessage.setExpiration(System.currentTimeMillis());
      producer.send(clientMessage);
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();
      clientConsumer = clientSession.createConsumer(eq);
      m = clientConsumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().getString(), "heyho!");
      clientConsumer.close();
      clientConsumer = clientSession.createConsumer(eq2);
      m = clientConsumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().getString(), "heyho!");
      clientConsumer.close();
   }

   public void testBasicSendToNoQueue() throws Exception
   {
      SimpleString ea = new SimpleString("EA");
      SimpleString qName = new SimpleString("q1");
      SimpleString eq = new SimpleString("EQ1");
      SimpleString eq2 = new SimpleString("EQ2");
      clientSession.createQueue(ea, eq, null, false, false, false);
      clientSession.createQueue(ea, eq2, null, false, false, false);
      clientSession.createQueue(qName, qName, null, false, false, false);
      ClientProducer producer = clientSession.createProducer(qName);
      ClientMessage clientMessage = createTextMessage("heyho!", clientSession);
      clientMessage.setExpiration(System.currentTimeMillis());
      producer.send(clientMessage);
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();
   }

   @Override
   protected void setUp() throws Exception
   {
      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      TransportConfiguration transportConfig = new TransportConfiguration(INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      messagingService = MessagingServiceImpl.newNullStorageMessagingServer(configuration);
      //start the server
      messagingService.start();
      //then we create a client as normal
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      clientSession = sessionFactory.createSession(true, true, false);
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (clientSession != null)
      {
         try
         {
            clientSession.close();
         }
         catch (MessagingException e1)
         {
            //
         }
      }
      if (messagingService != null && messagingService.isStarted())
      {
         try
         {
            messagingService.stop();
         }
         catch (Exception e1)
         {
            //
         }
      }
      messagingService = null;
      clientSession = null;
   }

}

