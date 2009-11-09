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

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class LVQRecoveryTest extends ServiceTestBase
{
   private HornetQServer server;

   private ClientSession clientSession;

   private SimpleString address = new SimpleString("LVQTestAddress");

   private SimpleString qName1 = new SimpleString("LVQTestQ1");

   private ClientSession clientSessionXa;

   private Configuration configuration;

   private AddressSettings qs;

   public void testMultipleMessagesAfterRecovery() throws Exception
   {
      Xid xid = new XidImpl("bq1".getBytes(), 4, "gtid1".getBytes());
      ClientProducer producer = clientSessionXa.createProducer(address, -1, true, true);
      SimpleString messageId1 = new SimpleString("SMID1");
      SimpleString messageId2 = new SimpleString("SMID2");
      clientSessionXa.start(xid, XAResource.TMNOFLAGS);
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
      clientSessionXa.end(xid, XAResource.TMSUCCESS);
      clientSessionXa.prepare(xid);

      clientSession.close();
      clientSessionXa.close();
      restartServer();
      
      clientSessionXa.commit(xid, true);
      ClientConsumer consumer = clientSession.createConsumer(qName1);
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

   public void testManyMessagesReceivedWithRollback() throws Exception
   {
      Xid xid = new XidImpl("bq1".getBytes(), 4, "gtid1".getBytes());
      ClientProducer producer = clientSession.createProducer(address, -1, true, true);
      ClientConsumer consumer = clientSessionXa.createConsumer(qName1);

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
      clientSessionXa.start(xid, XAResource.TMNOFLAGS);
      clientSessionXa.start();
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
      clientSessionXa.end(xid, XAResource.TMSUCCESS);
      clientSessionXa.prepare(xid);

      clientSession.close();
      clientSessionXa.close();
      restartServer();
      
      clientSessionXa.rollback(xid);
      consumer = clientSession.createConsumer(qName1);
      clientSession.start();
      m = consumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m6");
      m = consumer.receiveImmediate();
      assertNull(m);
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
      if (clientSessionXa != null)
      {
         try
         {
            clientSessionXa.close();
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
      
      clearData();
      configuration = createDefaultConfig();
      configuration.setSecurityEnabled(false);
      server = createServer(true, configuration);
      // start the server
      server.start();

      qs = new AddressSettings();
      qs.setLastValueQueue(true);
      server.getAddressSettingsRepository().addMatch(address.toString(), qs);
      // then we create a client as normal
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      sessionFactory.setBlockOnAcknowledge(true);
      sessionFactory.setAckBatchSize(0);
      clientSession = sessionFactory.createSession(false, true, true);
      clientSessionXa = sessionFactory.createSession(true, false, false);
      clientSession.createQueue(address, qName1, null, true);
   }

   private void restartServer() throws Exception
   {
      server.stop();
      server = null;
      server = createServer(true, configuration);
      server.getAddressSettingsRepository().addMatch(address.toString(), qs);
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
      clientSessionXa = sessionFactory.createSession(true, false, false);
   }
}
