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

import static org.jboss.messaging.core.message.impl.MessageImpl.HDR_ACTUAL_EXPIRY_TIME;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ExpiryAddressTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ExpiryAddressTest.class);

   private MessagingService messagingService;

   private ClientSession clientSession;

   public void testBasicSend() throws Exception
   {
      SimpleString ea = new SimpleString("EA");
      SimpleString qName = new SimpleString("q1");
      SimpleString eq = new SimpleString("EA1");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setExpiryAddress(ea);
      messagingService.getServer().getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      clientSession.createQueue(ea, eq, null, false, false);
      clientSession.createQueue(qName, qName, null, false, false);
      
      ClientProducer producer = clientSession.createProducer(qName);
      ClientMessage clientMessage = createTextMessage("heyho!", clientSession);
      clientMessage.setExpiration(System.currentTimeMillis());
      producer.send(clientMessage);
      
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      assertNull(m);
      System.out.println("size3 = " + messagingService.getServer().getPostOffice().getPagingManager().getGlobalSize());
      m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();
      clientConsumer = clientSession.createConsumer(eq);
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "heyho!");
      m.acknowledge();
      
      // PageSize should be the same as when it started
      assertEquals(0, messagingService.getServer().getPostOffice().getPagingManager().getGlobalSize());
   }

   public void testBasicSendToMultipleQueues() throws Exception
   {
      SimpleString ea = new SimpleString("EA");
      SimpleString qName = new SimpleString("q1");
      SimpleString eq = new SimpleString("EQ1");
      SimpleString eq2 = new SimpleString("EQ2");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setExpiryAddress(ea);
      messagingService.getServer().getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      clientSession.createQueue(ea, eq, null, false, false);
      clientSession.createQueue(ea, eq2, null, false, false);
      clientSession.createQueue(qName, qName, null, false, false);
      ClientProducer producer = clientSession.createProducer(qName);
      ClientMessage clientMessage = createTextMessage("heyho!", clientSession);
      clientMessage.setExpiration(System.currentTimeMillis());
      
      System.out.println("initialPageSize = " + messagingService.getServer().getPostOffice().getPagingManager().getGlobalSize());
      
      producer.send(clientMessage);
      
      System.out.println("pageSize after message sent = " + messagingService.getServer().getPostOffice().getPagingManager().getGlobalSize());
      
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      
      System.out.println("pageSize after message received = " + messagingService.getServer().getPostOffice().getPagingManager().getGlobalSize());
      
      assertNull(m);
      
      clientConsumer.close();
      
      clientConsumer = clientSession.createConsumer(eq);
      
      m = clientConsumer.receive(500);
      
      assertNotNull(m);
      
      log.info("acking");
      m.acknowledge();
      
      assertEquals(m.getBody().getString(), "heyho!");
      
      clientConsumer.close();
      
      clientConsumer = clientSession.createConsumer(eq2);
      
      m = clientConsumer.receive(500);
      
      assertNotNull(m);
      
      log.info("acking");
      m.acknowledge();
      
      assertEquals(m.getBody().getString(), "heyho!");
      
      clientConsumer.close();
      
      clientSession.commit();

      // PageGlobalSize should be untouched as the message expired
      assertEquals(0, messagingService.getServer().getPostOffice().getPagingManager().getGlobalSize());
   }

   public void testBasicSendToNoQueue() throws Exception
   {
      SimpleString ea = new SimpleString("EA");
      SimpleString qName = new SimpleString("q1");
      SimpleString eq = new SimpleString("EQ1");
      SimpleString eq2 = new SimpleString("EQ2");
      clientSession.createQueue(ea, eq, null, false, false);
      clientSession.createQueue(ea, eq2, null, false, false);
      clientSession.createQueue(qName, qName, null, false, false);
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

   public void testHeadersSet() throws Exception
   {
      final int NUM_MESSAGES = 5;
      SimpleString ea = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setExpiryAddress(ea);
      messagingService.getServer().getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      SimpleString eq = new SimpleString("EA1");
      clientSession.createQueue(ea, eq, null, false, false);
      clientSession.createQueue(qName, qName, null, false, false);
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      ClientSession sendSession = sessionFactory.createSession(false, true, true);
      ClientProducer producer = sendSession.createProducer(qName);

      long expiration = System.currentTimeMillis();
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage tm = createTextMessage("Message:" + i, clientSession);
         tm.setExpiration(expiration);
         producer.send(tm);
      }

      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      clientSession.start();
      ClientMessage m = clientConsumer.receive(1000);
      assertNull(m);
      // All the messages should now be in the EQ

      ClientConsumer cc3 = clientSession.createConsumer(eq);

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage tm = cc3.receive(1000);

         assertNotNull(tm);

         String text = tm.getBody().getString();
         assertEquals("Message:" + i, text);

         // Check the headers
         Long actualExpiryTime = (Long)tm.getProperty(HDR_ACTUAL_EXPIRY_TIME);
         assertTrue(actualExpiryTime >= expiration);
      }

   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      TransportConfiguration transportConfig = new TransportConfiguration(INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      messagingService = Messaging.newNullStorageMessagingService(configuration);
      // start the server
      messagingService.start();
      // then we create a client as normal
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      sessionFactory.setBlockOnAcknowledge(true); // There are assertions over sizes that needs to be done after the ACK was received on server
      clientSession = sessionFactory.createSession(null, null, false, true, true, false, 0);
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
      
      super.tearDown();
   }

}
