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
package org.hornetq.tests.integration.server;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class LVQTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(LVQTest.class);

   private HornetQServer server;

   private ClientSession clientSession;

   private ClientSession clientSessionTxReceives;

   private ClientSession clientSessionTxSends;

   private final SimpleString address = new SimpleString("LVQTestAddress");

   private final SimpleString qName1 = new SimpleString("LVQTestQ1");

   public void testSimple() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
      ClientMessage m1 = createTextMessage("m1", clientSession);
      SimpleString rh = new SimpleString("SMID1");
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      producer.send(m1);
      producer.send(m2);
      clientSession.start();
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m2");
   }

   public void testMultipleMessages() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
      SimpleString messageId1 = new SimpleString("SMID1");
      SimpleString messageId2 = new SimpleString("SMID2");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId1);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId2);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId1);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId2);
      producer.send(m1);
      producer.send(m2);
      producer.send(m3);
      producer.send(m4);
      clientSession.start();
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m3");
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m4");
   }

   public void testFirstMessageReceivedButAckedAfter() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
      ClientMessage m1 = createTextMessage("m1", clientSession);
      SimpleString rh = new SimpleString("SMID1");
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      producer.send(m1);
      clientSession.start();
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      producer.send(m2);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m1");
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m2");
   }

   public void testFirstMessageReceivedAndCancelled() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
      ClientMessage m1 = createTextMessage("m1", clientSession);
      SimpleString rh = new SimpleString("SMID1");
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      producer.send(m1);
      clientSession.start();
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "m1");
      producer.send(m2);
      consumer.close();
      consumer = clientSession.createConsumer(qName1);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = consumer.receiveImmediate();
      Assert.assertNull(m);
   }

   public void testManyMessagesReceivedAndCancelled() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address);
      ClientConsumer consumer = clientSession.createConsumer(qName1);

      SimpleString rh = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      m5.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      m6.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      clientSession.start();
      producer.send(m1);
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "m1");
      producer.send(m2);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "m2");
      producer.send(m3);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "m3");
      producer.send(m4);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "m4");
      producer.send(m5);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "m5");
      producer.send(m6);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "m6");
      consumer.close();
      consumer = clientSession.createConsumer(qName1);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m6");
      m = consumer.receiveImmediate();
      Assert.assertNull(m);
   }

   public void testSimpleInTx() throws Exception
   {

      ClientProducer producer = clientSessionTxReceives.createProducer(address);
      ClientConsumer consumer = clientSessionTxReceives.createConsumer(qName1);
      ClientMessage m1 = createTextMessage("m1", clientSession);
      SimpleString rh = new SimpleString("SMID1");
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      producer.send(m1);
      producer.send(m2);
      clientSessionTxReceives.start();
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m2");
   }

   public void testMultipleMessagesInTx() throws Exception
   {
      ClientProducer producer = clientSessionTxReceives.createProducer(address);
      ClientConsumer consumer = clientSessionTxReceives.createConsumer(qName1);
      SimpleString messageId1 = new SimpleString("SMID1");
      SimpleString messageId2 = new SimpleString("SMID2");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId1);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId2);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId1);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId2);
      producer.send(m1);
      producer.send(m2);
      producer.send(m3);
      producer.send(m4);
      clientSessionTxReceives.start();
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m3");
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m4");
      clientSessionTxReceives.commit();
      m = consumer.receiveImmediate();
      Assert.assertNull(m);
   }

   public void testMultipleMessagesInTxRollback() throws Exception
   {
      ClientProducer producer = clientSessionTxReceives.createProducer(address);
      ClientConsumer consumer = clientSessionTxReceives.createConsumer(qName1);
      SimpleString messageId1 = new SimpleString("SMID1");
      SimpleString messageId2 = new SimpleString("SMID2");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId1);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId2);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId1);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId2);
      producer.send(m1);
      producer.send(m2);
      clientSessionTxReceives.start();
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m1");
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m2");
      producer.send(m3);
      producer.send(m4);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m3");
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m4");
      clientSessionTxReceives.rollback();
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m3");
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m4");
   }

   public void testSingleTXRollback() throws Exception
   {
      ClientProducer producer = clientSessionTxReceives.createProducer(address);
      ClientConsumer consumer = clientSessionTxReceives.createConsumer(qName1);
      SimpleString messageId1 = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, messageId1);
      producer.send(m1);
      clientSessionTxReceives.start();
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      clientSessionTxReceives.rollback();
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m1");
      Assert.assertNull(consumer.receiveImmediate());
   }

   public void testMultipleMessagesInTxSend() throws Exception
   {
      ClientProducer producer = clientSessionTxSends.createProducer(address);
      ClientConsumer consumer = clientSessionTxSends.createConsumer(qName1);
      SimpleString rh = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      m5.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      m6.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      producer.send(m1);
      producer.send(m2);
      producer.send(m3);
      producer.send(m4);
      producer.send(m5);
      producer.send(m6);
      clientSessionTxSends.commit();
      clientSessionTxSends.start();
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m6");
   }

   public void testMultipleMessagesPersistedCorrectly() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
      SimpleString rh = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m1.setDurable(true);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m2.setDurable(true);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m3.setDurable(true);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m4.setDurable(true);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      m5.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m5.setDurable(true);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      m6.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m6.setDurable(true);
      producer.send(m1);
      producer.send(m2);
      producer.send(m3);
      producer.send(m4);
      producer.send(m5);
      producer.send(m6);
      clientSession.start();
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m6");
      m = consumer.receiveImmediate();
      Assert.assertNull(m);
   }

   public void testMultipleMessagesPersistedCorrectlyInTx() throws Exception
   {
      ClientProducer producer = clientSessionTxSends.createProducer(address);
      ClientConsumer consumer = clientSessionTxSends.createConsumer(qName1);
      SimpleString rh = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m1.setDurable(true);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m2.setDurable(true);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m3.setDurable(true);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m4.setDurable(true);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      m5.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m5.setDurable(true);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      m6.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m6.setDurable(true);
      producer.send(m1);
      producer.send(m2);
      producer.send(m3);
      producer.send(m4);
      producer.send(m5);
      producer.send(m6);
      clientSessionTxSends.commit();
      clientSessionTxSends.start();
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m6");
      m = consumer.receiveImmediate();
      Assert.assertNull(m);
   }

   public void testMultipleAcksPersistedCorrectly() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(address);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
      SimpleString rh = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m1.setDurable(true);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m2.setDurable(true);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m3.setDurable(true);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m4.setDurable(true);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      m5.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m5.setDurable(true);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      m6.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m6.setDurable(true);
      clientSession.start();
      producer.send(m1);
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m1");
      producer.send(m2);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m2");
      producer.send(m3);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m3");
      producer.send(m4);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m4");
      producer.send(m5);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m5");
      producer.send(m6);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m6");
   }

   public void testMultipleAcksPersistedCorrectlyInTx() throws Exception
   {
      ClientProducer producer = clientSessionTxReceives.createProducer(address);
      ClientConsumer consumer = clientSessionTxReceives.createConsumer(qName1);
      SimpleString rh = new SimpleString("SMID1");
      ClientMessage m1 = createTextMessage("m1", clientSession);
      m1.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m1.setDurable(true);
      ClientMessage m2 = createTextMessage("m2", clientSession);
      m2.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m2.setDurable(true);
      ClientMessage m3 = createTextMessage("m3", clientSession);
      m3.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m3.setDurable(true);
      ClientMessage m4 = createTextMessage("m4", clientSession);
      m4.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m4.setDurable(true);
      ClientMessage m5 = createTextMessage("m5", clientSession);
      m5.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m5.setDurable(true);
      ClientMessage m6 = createTextMessage("m6", clientSession);
      m6.putStringProperty(Message.HDR_LAST_VALUE_NAME, rh);
      m6.setDurable(true);
      clientSessionTxReceives.start();
      producer.send(m1);
      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m1");
      producer.send(m2);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m2");
      producer.send(m3);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m3");
      producer.send(m4);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m4");
      producer.send(m5);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m5");
      producer.send(m6);
      m = consumer.receive(1000);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "m6");
      clientSessionTxReceives.commit();
   }

   protected void tearDown() throws Exception
   {
      if (clientSession != null)
      {
         try
         {
            clientSession.close();
         }
         catch (HornetQException e1)
         {
            //
         }
      }

      if (clientSessionTxReceives != null)
      {
         try
         {
            clientSessionTxReceives.close();
         }
         catch (HornetQException e1)
         {
            //
         }
      }

      if (clientSessionTxSends != null)
      {
         try
         {
            clientSessionTxSends.close();
         }
         catch (HornetQException e1)
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

      ConfigurationImpl configuration = createBasicConfig();
      configuration.setSecurityEnabled(false);
      TransportConfiguration transportConfig = new TransportConfiguration(UnitTestCase.INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      server = HornetQServers.newHornetQServer(configuration, false);
      // start the server
      server.start();

      AddressSettings qs = new AddressSettings();
      qs.setLastValueQueue(true);
      server.getAddressSettingsRepository().addMatch(address.toString(), qs);
      // then we create a client as normalServer
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY));
      locator.setBlockOnAcknowledge(true);
      locator.setAckBatchSize(0);
      ClientSessionFactory sessionFactory = locator.createSessionFactory();
      clientSession = sessionFactory.createSession(false, true, true);
      clientSessionTxReceives = sessionFactory.createSession(false, true, false);
      clientSessionTxSends = sessionFactory.createSession(false, false, true);
      clientSession.createQueue(address, qName1, null, true);
   }
}
