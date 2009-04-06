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
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class LVQTest extends UnitTestCase
{
   private MessagingServer server;

   private ClientSession clientSession;

   private ClientSession clientSessionTxReceives;

   private ClientSession clientSessionTxSends;

   private SimpleString address = new SimpleString("LVQTestAddress");

   private SimpleString qName1 = new SimpleString("LVQTestQ1");

   private FakeStorageManager storageManager;


   public void testSimple() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
      ClientMessage m1 = createTextMessage("m1", clientSession);
      SimpleString rh = new SimpleString("SMID1");
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      producer.send(m1);
      producer.send(m2);
      clientSession.start();
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
   }

   public void testMultipleMessages() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
      SimpleString messageId1 = new SimpleString("SMID1");
      SimpleString messageId2 = new SimpleString("SMID2");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, messageId1);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, messageId2);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, messageId1);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, messageId2);
      producer.send(m1);
      producer.send(m2);
      producer.send(m3);
      producer.send(m4);
      clientSession.start();
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m3");
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m4");
   }

   public void testFirstMessageReceivedButAckedAfter() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
      ClientMessage m1 = createTextMessage("m1", clientSession);
      SimpleString rh = new SimpleString("SMID1");
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      producer.send(m1);
      clientSession.start();
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      producer.send(m2);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m1");
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
   }

   public void testFirstMessageReceivedAndCancelled() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
      ClientMessage m1 = createTextMessage("m1", clientSession);
      SimpleString rh = new SimpleString("SMID1");
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      producer.send(m1);
      clientSession.start();
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      producer.send(m2);
      consumer.close();
      consumer = clientSession.createConsumer(qName1);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
      m = consumer.receive(1000);
      assertNull(m);
   }

   public void testManyMessagesReceivedAndCancelled() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSession.createConsumer(qName1);

      SimpleString rh = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      m5.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      m6.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      clientSession.start();
      producer.send(m1);
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m1");
      producer.send(m2);
      m = consumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m2");
      producer.send(m3);
      m = consumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m3");
      producer.send(m4);
      m = consumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m4");
      producer.send(m5);
      m = consumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m5");
      producer.send(m6);
      m = consumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m6");
      consumer.close();
      consumer = clientSession.createConsumer(qName1);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m6");
      m = consumer.receive(1000);
      assertNull(m);
   }

   public void testSimpleInTx() throws Exception
   {

      ClientProducer producer = clientSessionTxReceives.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSessionTxReceives.createConsumer(qName1);
      ClientMessage m1 = createTextMessage("m1", clientSession);
      SimpleString rh = new SimpleString("SMID1");
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      producer.send(m1);
      producer.send(m2);
      clientSessionTxReceives.start();
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
   }

   public void testMultipleMessagesInTx() throws Exception
   {
      ClientProducer producer = clientSessionTxReceives.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSessionTxReceives.createConsumer(qName1);
      SimpleString messageId1 = new SimpleString("SMID1");
      SimpleString messageId2 = new SimpleString("SMID2");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, messageId1);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, messageId2);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, messageId1);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, messageId2);
      producer.send(m1);
      producer.send(m2);
      producer.send(m3);
      producer.send(m4);
      clientSessionTxReceives.start();
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m3");
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m4");
      clientSessionTxReceives.commit();
      m = consumer.receive(1000);
      assertNull(m);
   }

   public void testMultipleMessagesInTxRollback() throws Exception
   {
      ClientProducer producer = clientSessionTxReceives.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSessionTxReceives.createConsumer(qName1);
      SimpleString messageId1 = new SimpleString("SMID1");
      SimpleString messageId2 = new SimpleString("SMID2");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, messageId1);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, messageId2);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, messageId1);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, messageId2);
      producer.send(m1);
      producer.send(m2);
      clientSessionTxReceives.start();
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m1");
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
      producer.send(m3);
      producer.send(m4);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m3");
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m4");
      clientSessionTxReceives.rollback();
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m3");
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m4");
   }

   public void testMultipleMessagesInTxSend() throws Exception
   {
      ClientProducer producer = clientSessionTxSends.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSessionTxSends.createConsumer(qName1);
      SimpleString rh = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      m5.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      m6.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      producer.send(m1);
      producer.send(m2);
      producer.send(m3);
      producer.send(m4);
      producer.send(m5);
      producer.send(m6);
      clientSessionTxSends.commit();
      clientSessionTxSends.start();
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m6");
   }

   public void testMultipleMessagesPersistedCorrectly() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
      SimpleString rh = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m1.setDurable(true);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m2.setDurable(true);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m3.setDurable(true);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m4.setDurable(true);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      m5.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m5.setDurable(true);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      m6.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m6.setDurable(true);
      producer.send(m1);
      producer.send(m2);
      producer.send(m3);
      producer.send(m4);
      producer.send(m5);
      producer.send(m6);
      assertEquals(1, storageManager.messageIds.size());
      clientSession.start();
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m6");
      assertEquals(0, storageManager.messageIds.size());
   }

   public void testMultipleMessagesPersistedCorrectlyInTx() throws Exception
   {
      ClientProducer producer = clientSessionTxSends.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSessionTxSends.createConsumer(qName1);
      SimpleString rh = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m1.setDurable(true);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m2.setDurable(true);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m3.setDurable(true);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m4.setDurable(true);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      m5.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m5.setDurable(true);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      m6.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m6.setDurable(true);
      producer.send(m1);
      producer.send(m2);
      producer.send(m3);
      producer.send(m4);
      producer.send(m5);
      producer.send(m6);
      clientSessionTxSends.commit();
      assertEquals(1, storageManager.messageIds.size());
      clientSessionTxSends.start();
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m6");
      assertEquals(0, storageManager.messageIds.size());
   }

   public void testMultipleAcksPersistedCorrectly() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
      SimpleString rh = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m1.setDurable(true);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m2.setDurable(true);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m3.setDurable(true);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m4.setDurable(true);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      m5.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m5.setDurable(true);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      m6.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m6.setDurable(true);
      clientSession.start();
      producer.send(m1);
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m1");
      producer.send(m2);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
      producer.send(m3);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m3");
      producer.send(m4);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m4");
      producer.send(m5);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m5");
      producer.send(m6);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m6");
      assertEquals(6, storageManager.ackIds.size());
   }

   public void testMultipleAcksPersistedCorrectlyInTx() throws Exception
   {
      ClientProducer producer = clientSessionTxReceives.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSessionTxReceives.createConsumer(qName1);
      SimpleString rh = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m1.setDurable(true);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m2.setDurable(true);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m3.setDurable(true);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m4.setDurable(true);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      m5.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m5.setDurable(true);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      m6.putStringProperty(MessageImpl.HDR_LAST_VALUE_NAME, rh);
      m6.setDurable(true);
      clientSessionTxReceives.start();
      producer.send(m1);
      ClientMessage m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m1");
      producer.send(m2);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
      producer.send(m3);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m3");
      producer.send(m4);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m4");
      producer.send(m5);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m5");
      producer.send(m6);
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m6");
      clientSessionTxReceives.commit();
      assertEquals(6, storageManager.ackIds.size());
   }

   


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
      if (server != null && server.isStarted())
      {
         try
         {
            server.stop();
         }
         catch (Exception e1)
         {
            //
         }
      }
      server = null;
      clientSession = null;
      
      super.tearDown();
   }

   protected void setUp() throws Exception
   {
      super.setUp();
      
      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      TransportConfiguration transportConfig = new TransportConfiguration(INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      storageManager = new FakeStorageManager();
      server = Messaging.newMessagingServer(configuration, storageManager);
      // start the server
      server.start();

      AddressSettings qs = new AddressSettings();
      qs.setLastValueQueue(true);
      server.getAddressSettingsRepository().addMatch(address.toString(), qs);
      // then we create a client as normal
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      sessionFactory.setBlockOnAcknowledge(true);
      sessionFactory.setAckBatchSize(0);
      clientSession = sessionFactory.createSession(false, true, true);
      clientSessionTxReceives = sessionFactory.createSession(false, true, false);
      clientSessionTxSends = sessionFactory.createSession(false, false, true);
      clientSession.createQueue(address, qName1, null, true);
   }
}
