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
package org.jboss.messaging.tests.integration.server;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class DeadLetterAddressTest extends UnitTestCase
{
   private MessagingService messagingService;

   private ClientSession clientSession;

   public void testBasicSend() throws Exception
   {
      Xid xid = new XidImpl("bq".getBytes(), 0, "gt".getBytes());
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxDeliveryAttempts(1);
      addressSettings.setDeadLetterAddress(dla);
      messagingService.getServer().getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      clientSession.createQueue(dla, dlq, null, false, false);
      clientSession.createQueue(qName, qName, null, false, false);
      ClientProducer producer = clientSession.createProducer(qName);
      producer.send(createTextMessage("heyho!", clientSession));
      clientSession.start();
      clientSession.start(xid, XAResource.TMNOFLAGS);
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "heyho!");
      //force a cancel
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.rollback(xid);
      m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();
      clientConsumer = clientSession.createConsumer(dlq);
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "heyho!");
   }

   public void testBasicSendToMultipleQueues() throws Exception
   {
      Xid xid = new XidImpl("bq".getBytes(), 0, "gt".getBytes());
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxDeliveryAttempts(1);
      addressSettings.setDeadLetterAddress(dla);
      messagingService.getServer().getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      SimpleString dlq2 = new SimpleString("DLQ2");
      clientSession.createQueue(dla, dlq, null, false, false);
      clientSession.createQueue(dla, dlq2, null, false, false);
      clientSession.createQueue(qName, qName, null, false, false);
      ClientProducer producer = clientSession.createProducer(qName);
      producer.send(createTextMessage("heyho!", clientSession));
      clientSession.start();
      clientSession.start(xid, XAResource.TMNOFLAGS);
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "heyho!");
      //force a cancel
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.rollback(xid);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();
      clientConsumer = clientSession.createConsumer(dlq);
      m = clientConsumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "heyho!");
      clientConsumer.close();
      clientConsumer = clientSession.createConsumer(dlq2);
      m = clientConsumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "heyho!");
      clientConsumer.close();
   }

   public void testBasicSendToNoQueue() throws Exception
   {
      Xid xid = new XidImpl("bq".getBytes(), 0, "gt".getBytes());
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxDeliveryAttempts(1);
      messagingService.getServer().getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      clientSession.createQueue(qName, qName, null, false, false);
      ClientProducer producer = clientSession.createProducer(qName);
      producer.send(createTextMessage("heyho!", clientSession));
      clientSession.start();
      clientSession.start(xid, XAResource.TMNOFLAGS);
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "heyho!");
      //force a cancel
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.rollback(xid);
      m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();
   }

   public void testHeadersSet() throws Exception
   {
      final int MAX_DELIVERIES = 16;
      final int NUM_MESSAGES = 5;
      Xid xid = new XidImpl("bq".getBytes(), 0, "gt".getBytes());
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxDeliveryAttempts(MAX_DELIVERIES);
      addressSettings.setDeadLetterAddress(dla);
      messagingService.getServer().getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      clientSession.createQueue(dla, dlq, null, false, false);
      clientSession.createQueue(qName, qName, null, false, false);
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      ClientSession sendSession = sessionFactory.createSession(false, true, true);
      ClientProducer producer = sendSession.createProducer(qName);
      Map<String, Long> origIds = new HashMap<String, Long>();

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage tm = createTextMessage("Message:" + i, clientSession);
         producer.send(tm);
      }

      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      clientSession.start();

      for (int i = 0; i < MAX_DELIVERIES; i++)
      {
         clientSession.start(xid, XAResource.TMNOFLAGS);
         for (int j = 0; j < NUM_MESSAGES; j++)
         {
            ClientMessage tm = clientConsumer.receive(1000);

            assertNotNull(tm);
            tm.acknowledge();
            if(i == 0)
            {
               origIds.put("Message:" + j, tm.getMessageID());
            }
            assertEquals("Message:" + j, tm.getBody().readString());
         }
         clientSession.end(xid, XAResource.TMSUCCESS);
         clientSession.rollback(xid);
      }
      
      assertEquals(0, ((Queue)messagingService.getServer().getPostOffice().getBinding(qName).getBindable()).getMessageCount());
      ClientMessage m = clientConsumer.receive(1000);
      assertNull(m);
      //All the messages should now be in the DLQ

      ClientConsumer cc3 = clientSession.createConsumer(dlq);

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage tm = cc3.receive(1000);

         assertNotNull(tm);

         String text = tm.getBody().readString();
         assertEquals("Message:" + i, text);

         // Check the headers
         SimpleString origDest =
               (SimpleString) tm.getProperty(MessageImpl.HDR_ORIGINAL_DESTINATION);

         Long origMessageId =
               (Long) tm.getProperty(MessageImpl.HDR_ORIG_MESSAGE_ID);

         assertEquals(qName, origDest);

         Long origId = origIds.get(text);

         assertEquals(origId, origMessageId);
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
      super.tearDown();
   }

}
