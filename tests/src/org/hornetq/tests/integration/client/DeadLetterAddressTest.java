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

import static org.hornetq.tests.util.RandomUtil.randomSimpleString;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class DeadLetterAddressTest extends UnitTestCase
{
   private HornetQServer server;

   private ClientSession clientSession;

   public void testBasicSend() throws Exception
   {
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxDeliveryAttempts(1);
      addressSettings.setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      clientSession.createQueue(dla, dlq, null, false);
      clientSession.createQueue(qName, qName, null, false);
      ClientProducer producer = clientSession.createProducer(qName);
      producer.send(createTextMessage("heyho!", clientSession));
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "heyho!");
      // force a cancel
      clientSession.rollback();
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
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxDeliveryAttempts(1);
      addressSettings.setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      SimpleString dlq2 = new SimpleString("DLQ2");
      clientSession.createQueue(dla, dlq, null, false);
      clientSession.createQueue(dla, dlq2, null, false);
      clientSession.createQueue(qName, qName, null, false);
      ClientProducer producer = clientSession.createProducer(qName);
      producer.send(createTextMessage("heyho!", clientSession));
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "heyho!");
      // force a cancel
      clientSession.rollback();
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
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxDeliveryAttempts(1);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      clientSession.createQueue(qName, qName, null, false);
      ClientProducer producer = clientSession.createProducer(qName);
      producer.send(createTextMessage("heyho!", clientSession));
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "heyho!");
      // force a cancel
      clientSession.rollback();
      m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();
   }

   public void testHeadersSet() throws Exception
   {
      final int MAX_DELIVERIES = 16;
      final int NUM_MESSAGES = 5;
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxDeliveryAttempts(MAX_DELIVERIES);
      addressSettings.setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      clientSession.createQueue(dla, dlq, null, false);
      clientSession.createQueue(qName, qName, null, false);
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
         for (int j = 0; j < NUM_MESSAGES; j++)
         {
            ClientMessage tm = clientConsumer.receive(1000);

            assertNotNull(tm);
            tm.acknowledge();
            if (i == 0)
            {
               origIds.put("Message:" + j, tm.getMessageID());
            }
            assertEquals("Message:" + j, tm.getBody().readString());
         }
         clientSession.rollback();
      }

      assertEquals(0, ((Queue)server.getPostOffice().getBinding(qName).getBindable()).getMessageCount());
      ClientMessage m = clientConsumer.receive(1000);
      assertNull(m);
      // All the messages should now be in the DLQ

      ClientConsumer cc3 = clientSession.createConsumer(dlq);

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage tm = cc3.receive(1000);

         assertNotNull(tm);

         String text = tm.getBody().readString();
         assertEquals("Message:" + i, text);

         // Check the headers
         SimpleString origDest = (SimpleString)tm.getProperty(MessageImpl.HDR_ORIGINAL_DESTINATION);

         Long origMessageId = (Long)tm.getProperty(MessageImpl.HDR_ORIG_MESSAGE_ID);

         assertEquals(qName, origDest);

         Long origId = origIds.get(text);

         assertEquals(origId, origMessageId);
      }
      
      sendSession.close();

   }

   public void testDeadlLetterAddressWithDefaultAddressSettings() throws Exception
   {
      int deliveryAttempt = 3;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString deadLetterAdress = randomSimpleString();
      SimpleString deadLetterQueue = randomSimpleString();
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxDeliveryAttempts(deliveryAttempt);
      addressSettings.setDeadLetterAddress(deadLetterAdress);
      server.getAddressSettingsRepository().setDefault(addressSettings);

      clientSession.createQueue(address, queue, false);
      clientSession.createQueue(deadLetterAdress, deadLetterQueue, false);

      ClientProducer producer = clientSession.createProducer(address);
      ClientMessage clientMessage = createTextMessage("heyho!", clientSession);
      producer.send(clientMessage);

      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(queue);
      for (int i = 0; i < deliveryAttempt; i++)
      {
         ClientMessage m = clientConsumer.receive(500);
         assertNotNull(m);
         assertEquals(i + 1, m.getDeliveryCount());
         m.acknowledge();
         clientSession.rollback();
      }
      ClientMessage m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();

      clientConsumer = clientSession.createConsumer(deadLetterQueue);
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "heyho!");
   }

   public void testDeadlLetterAddressWithWildcardAddressSettings() throws Exception
   {
      int deliveryAttempt = 3;

      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString deadLetterAdress = randomSimpleString();
      SimpleString deadLetterQueue = randomSimpleString();
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxDeliveryAttempts(deliveryAttempt);
      addressSettings.setDeadLetterAddress(deadLetterAdress);
      server.getAddressSettingsRepository().addMatch("*", addressSettings);

      clientSession.createQueue(address, queue, false);
      clientSession.createQueue(deadLetterAdress, deadLetterQueue, false);

      ClientProducer producer = clientSession.createProducer(address);
      ClientMessage clientMessage = createTextMessage("heyho!", clientSession);
      producer.send(clientMessage);

      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(queue);
      for (int i = 0; i < deliveryAttempt; i++)
      {
         ClientMessage m = clientConsumer.receive(500);
         assertNotNull(m);
         assertEquals(i + 1, m.getDeliveryCount());
         m.acknowledge();
         clientSession.rollback();
      }
      ClientMessage m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();

      clientConsumer = clientSession.createConsumer(deadLetterQueue);
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "heyho!");
   }

   public void testDeadLetterAddressWithOverridenSublevelAddressSettings() throws Exception
   {
      int defaultDeliveryAttempt = 3;
      int specificeDeliveryAttempt = defaultDeliveryAttempt + 1;

      SimpleString address = new SimpleString("prefix.address");
      SimpleString queue = randomSimpleString();
      SimpleString defaultDeadLetterAddress = randomSimpleString();
      SimpleString defaultDeadLetterQueue = randomSimpleString();
      SimpleString specificDeadLetterAddress = randomSimpleString();
      SimpleString specificDeadLetterQueue = randomSimpleString();

      AddressSettings defaultAddressSettings = new AddressSettings();
      defaultAddressSettings.setMaxDeliveryAttempts(defaultDeliveryAttempt);
      defaultAddressSettings.setDeadLetterAddress(defaultDeadLetterAddress);
      server.getAddressSettingsRepository().addMatch("*", defaultAddressSettings);
      AddressSettings specificAddressSettings = new AddressSettings();
      specificAddressSettings.setMaxDeliveryAttempts(specificeDeliveryAttempt);
      specificAddressSettings.setDeadLetterAddress(specificDeadLetterAddress);
      server.getAddressSettingsRepository().addMatch(address.toString(), specificAddressSettings);

      clientSession.createQueue(address, queue, false);
      clientSession.createQueue(defaultDeadLetterAddress, defaultDeadLetterQueue, false);
      clientSession.createQueue(specificDeadLetterAddress, specificDeadLetterQueue, false);

      ClientProducer producer = clientSession.createProducer(address);
      ClientMessage clientMessage = createTextMessage("heyho!", clientSession);
      producer.send(clientMessage);

      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(queue);
      ClientConsumer defaultDeadLetterConsumer = clientSession.createConsumer(defaultDeadLetterQueue);
      ClientConsumer specificDeadLetterConsumer = clientSession.createConsumer(specificDeadLetterQueue);

      for (int i = 0; i < defaultDeliveryAttempt; i++)
      {
         ClientMessage m = clientConsumer.receive(500);
         assertNotNull(m);
         assertEquals(i + 1, m.getDeliveryCount());
         m.acknowledge();
         clientSession.rollback();
      }

      assertNull(defaultDeadLetterConsumer.receive(500));
      assertNull(specificDeadLetterConsumer.receive(500));

      // one more redelivery attempt:
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals(specificeDeliveryAttempt, m.getDeliveryCount());
      m.acknowledge();
      clientSession.rollback();

      assertNull(defaultDeadLetterConsumer.receive(500));
      assertNotNull(specificDeadLetterConsumer.receive(500));
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      TransportConfiguration transportConfig = new TransportConfiguration(INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      server = HornetQ.newHornetQServer(configuration, false);
      // start the server
      server.start();
      // then we create a client as normal
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      clientSession = sessionFactory.createSession(false, true, false);
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

}
