/*
 * Copyright 2005-2014 Red Hat, Inc.
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

import java.util.Arrays;
import java.util.Collection;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.impl.journal.LargeServerMessageImpl;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class ExportTest extends ClusterTestBase
{
   private boolean useExportGroupName;

   // this will ensure that all tests in this class are run twice,
   // once with "true" passed to the class' constructor and once with "false"
   @Parameterized.Parameters
   public static Collection getParameters()
   {
      return Arrays.asList(new Object[][]{
         {true},
         {false}
      });
   }

   public ExportTest(boolean useExportGroupName)
   {
      this.useExportGroupName = useExportGroupName;
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      if (useExportGroupName)
      {
         servers[0].getConfiguration().setExportGroupName("bill");
         servers[1].getConfiguration().setExportGroupName("bill");
      }
      servers[0].getConfiguration().setExportOnServerShutdown(true);
      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1);
      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0);
      startServers(0, 1);
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
   }

   protected boolean isNetty()
   {
      return true;
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      closeAllConsumers();
      closeAllSessionFactories();
      closeAllServerLocatorsFactories();
      servers[0].getConfiguration().setExportOnServerShutdown(false);
      servers[1].getConfiguration().setExportOnServerShutdown(false);
      stopServers(0, 1);
      super.tearDown();
   }

   @Test
   public void testBasicExport() throws Exception
   {
      final int TEST_SIZE = 2;
      final String addressName = "testAddress";
      final String queueName1 = "testQueue1";
      final String queueName2 = "testQueue2";

      // create 2 queues on each node mapped to the same address
      createQueue(0, addressName, queueName1, null, false);
      createQueue(0, addressName, queueName2, null, false);
      createQueue(1, addressName, queueName1, null, false);
      createQueue(1, addressName, queueName2, null, false);

      // send messages to node 0
      send(0, addressName, TEST_SIZE, false, null);

      // consume a message from queue 2
      addConsumer(1, 0, queueName2, null, false);
      ClientMessage clientMessage = consumers[1].getConsumer().receive();
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();
      consumers[1].getSession().commit();
//      removeConsumer(1);

      // at this point on node 0 there should be 2 messages in testQueue1 and 1 message in testQueue2
      Assert.assertEquals(TEST_SIZE, ((LocalQueueBinding) servers[0].getPostOffice().getBinding(new SimpleString(queueName1))).getQueue().getMessageCount());
      Assert.assertEquals(TEST_SIZE - 1, ((LocalQueueBinding) servers[0].getPostOffice().getBinding(new SimpleString(queueName2))).getQueue().getMessageCount());

      // trigger export from node 0 to node 1
      servers[0].stop();

      // get the 2 messages from queue 1
      addConsumer(0, 1, queueName1, null);
      clientMessage = consumers[0].getConsumer().receive();
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();
      clientMessage = consumers[0].getConsumer().receive();
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();

      // ensure there are no more messages on queue 1
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);

      // get the 1 message from queue 2
      addConsumer(0, 1, queueName2, null);
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();

      // ensure there are no more messages on queue 1
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);
   }

   @Test
   public void testExportWithMissingQueue() throws Exception
   {
      final int TEST_SIZE = 2;
      final String addressName = "testAddress";
      final String queueName1 = "testQueue1";
      final String queueName2 = "testQueue2";

      // create 2 queues on each node mapped to the same address
      createQueue(0, addressName, queueName1, null, false);
      createQueue(0, addressName, queueName2, null, false);
      createQueue(1, addressName, queueName1, null, false);

      // send messages to node 0
      send(0, addressName, TEST_SIZE, false, null);

      // consume a message from node 0
      addConsumer(1, 0, queueName2, null, false);
      ClientMessage clientMessage = consumers[1].getConsumer().receive();
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();
      consumers[1].getSession().commit();

      // trigger export from node 0 to node 1
      servers[0].stop();

      // get the 2 messages from queue 1
      addConsumer(0, 1, queueName1, null);
      clientMessage = consumers[0].getConsumer().receive();
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();
      clientMessage = consumers[0].getConsumer().receive();
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();

      // ensure there are no more messages on queue 1
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);

      // get the 1 message from queue 2
      addConsumer(0, 1, queueName2, null);
      clientMessage = consumers[0].getConsumer().receive();
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();

      // ensure there are no more messages on queue 1
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);
   }

   @Test
   public void testMessageProperties() throws Exception
   {
      final int TEST_SIZE = 5;
      final String addressName = "testAddress";
      final String queueName = "testQueue";

      createQueue(0, addressName, queueName, null, false);
      createQueue(1, addressName, queueName, null, false);

      ClientSessionFactory sf = sfs[0];
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      ClientProducer producer = addClientProducer(session.createProducer(addressName));

      StringBuilder international = new StringBuilder();
      for (char x = 800; x < 1200; x++)
      {
         international.append(x);
      }

      String special = "\"<>'&";

      for (int i = 0; i < TEST_SIZE; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeString("Bob the giant pig " + i);
         msg.putBooleanProperty("myBooleanProperty", Boolean.TRUE);
         msg.putByteProperty("myByteProperty", new Byte("0"));
         msg.putBytesProperty("myBytesProperty", new byte[]{0, 1, 2, 3, 4});
         msg.putDoubleProperty("myDoubleProperty", i * 1.6);
         msg.putFloatProperty("myFloatProperty", i * 2.5F);
         msg.putIntProperty("myIntProperty", i);
         msg.putLongProperty("myLongProperty", Long.MAX_VALUE - i);
         msg.putObjectProperty("myObjectProperty", i);
         msg.putShortProperty("myShortProperty", new Integer(i).shortValue());
         msg.putStringProperty("myStringProperty", "myStringPropertyValue_" + i);
         msg.putStringProperty("myNonAsciiStringProperty", international.toString());
         msg.putStringProperty("mySpecialCharacters", special);
         producer.send(msg);
      }

//      Assert.assertEquals(TEST_SIZE, ((HornetQServerImpl) servers[0]).export());

      servers[0].stop();

      sf = sfs[1];
      session = addClientSession(sf.createSession(false, true, true));
      ClientConsumer consumer = addClientConsumer(session.createConsumer(queueName));
      session.start();

      for (int i = 0; i < 5; i++)
      {
         ClientMessage msg = consumer.receive(250);
         byte[] body = new byte[msg.getBodySize()];
         msg.getBodyBuffer().readBytes(body);
         Assert.assertTrue(new String(body).contains("Bob the giant pig " + i));
         Assert.assertEquals(msg.getBooleanProperty("myBooleanProperty"), Boolean.TRUE);
         Assert.assertEquals(msg.getByteProperty("myByteProperty"), new Byte("0"));
         byte[] bytes = msg.getBytesProperty("myBytesProperty");
         for (int j = 0; j < 5; j++)
         {
            Assert.assertEquals(j, bytes[j]);
         }
         Assert.assertEquals(i * 1.6, msg.getDoubleProperty("myDoubleProperty"), 0.000001);
         Assert.assertEquals(i * 2.5F, msg.getFloatProperty("myFloatProperty"), 0.000001);
         Assert.assertEquals(i, msg.getIntProperty("myIntProperty").intValue());
         Assert.assertEquals(Long.MAX_VALUE - i, msg.getLongProperty("myLongProperty").longValue());
         Assert.assertEquals(i, msg.getObjectProperty("myObjectProperty"));
         Assert.assertEquals(new Integer(i).shortValue(), msg.getShortProperty("myShortProperty").shortValue());
         Assert.assertEquals("myStringPropertyValue_" + i, msg.getStringProperty("myStringProperty"));
         Assert.assertEquals(international.toString(), msg.getStringProperty("myNonAsciiStringProperty"));
         Assert.assertEquals(special, msg.getStringProperty("mySpecialCharacters"));
      }
   }

   @Test
   public void testLargeMessage() throws Exception
   {
      final int TEST_SIZE = 1;
      final String addressName = "testAddress";
      final String queueName = "testQueue";

      createQueue(0, addressName, queueName, null, false);
      createQueue(1, addressName, queueName, null, false);

      ClientSessionFactory sf = sfs[0];
      ClientSession session = addClientSession(sf.createSession(false, false));
      ClientProducer producer = addClientProducer(session.createProducer(addressName));

      LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) servers[0].getStorageManager());

      fileMessage.setMessageID(1005);
      fileMessage.setDurable(true);

      for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
      {
         fileMessage.addBytes(new byte[]{UnitTestCase.getSamplebyte(i)});
      }

      fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      fileMessage.releaseResources();

      producer.send(fileMessage);

      fileMessage.deleteFile();

      session.commit();

//      Assert.assertEquals(TEST_SIZE, ((HornetQServerImpl) servers[0]).export());

      servers[0].stop();

      sf = sfs[1];
      session = addClientSession(sf.createSession(false, true, true));
      ClientConsumer consumer = addClientConsumer(session.createConsumer(queueName));
      session.start();

      ClientMessage msg = consumer.receive(250);

      Assert.assertNotNull(msg);

      Assert.assertEquals(2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, msg.getBodySize());

      for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
      {
         Assert.assertEquals(UnitTestCase.getSamplebyte(i), msg.getBodyBuffer().readByte());
      }

      msg.acknowledge();
      session.commit();
   }

   @Test
   public void testPaging() throws Exception
   {
      final int CHUNK_SIZE = 50;
      int messageCount = 0;
      final String addressName = "testAddress";
      final String queueName = "testQueue";

      createQueue(0, addressName, queueName, null, false);
      createQueue(1, addressName, queueName, null, false);

      ClientSessionFactory sf = sfs[0];
      ClientSession session = addClientSession(sf.createSession(false, false));
      ClientProducer producer = addClientProducer(session.createProducer(addressName));

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(20 * 1024);
      servers[0].getAddressSettingsRepository().addMatch("#", defaultSetting);

      while (!servers[0].getPagingManager().getPageStore(new SimpleString(addressName)).isPaging())
      {
         for (int i = 0; i < CHUNK_SIZE; i++)
         {
            Message message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(new byte[1024]);
            producer.send(message);
            messageCount++;
         }
         session.commit();
      }

//      Assert.assertEquals(messageCount, ((HornetQServerImpl) servers[0]).export());

      servers[0].stop();

      addConsumer(0, 1, queueName, null);
      for (int i = 0; i < messageCount; i++)
      {
         Assert.assertNotNull(consumers[0].getConsumer().receive());
      }

      Assert.assertNull(consumers[0].getConsumer().receive(250));
      removeConsumer(0);
   }

   @Test
   public void testOrderWithPaging() throws Exception
   {
      final int CHUNK_SIZE = 50;
      int messageCount = 0;
      final String addressName = "testAddress";
      final String queueName = "testQueue";

      createQueue(0, addressName, queueName, null, false);
      createQueue(1, addressName, queueName, null, false);

      ClientSessionFactory sf = sfs[0];
      ClientSession session = addClientSession(sf.createSession(false, false));
      ClientProducer producer = addClientProducer(session.createProducer(addressName));

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(20 * 1024);
      servers[0].getAddressSettingsRepository().addMatch("#", defaultSetting);

      while (!servers[0].getPagingManager().getPageStore(new SimpleString(addressName)).isPaging())
      {
         for (int i = 0; i < CHUNK_SIZE; i++)
         {
            Message message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(new byte[1024]);
            message.putIntProperty("order", i);
            producer.send(message);
            messageCount++;
         }
         session.commit();
      }

      servers[0].stop();

      addConsumer(0, 1, queueName, null);
      for (int i = 0; i < messageCount; i++)
      {
         Assert.assertEquals(i, consumers[0].getConsumer().receive().getIntProperty("order").intValue());
      }

      Assert.assertNull(consumers[0].getConsumer().receive(250));
      removeConsumer(0);
   }

   @Test
   public void testFilters() throws Exception
   {
      final int TEST_SIZE = 50;
      final String addressName = "testAddress";
      final String evenQueue = "evenQueue";
      final String oddQueue = "oddQueue";

      createQueue(0, addressName, evenQueue, "0", false);
      createQueue(0, addressName, oddQueue, "1", false);
      createQueue(1, addressName, evenQueue, "0", false);
      createQueue(1, addressName, oddQueue, "1", false);

      ClientSessionFactory sf = sfs[0];
      ClientSession session = addClientSession(sf.createSession(false, false));
      ClientProducer producer = addClientProducer(session.createProducer(addressName));

      for (int i = 0; i < TEST_SIZE; i++)
      {
         Message message = session.createMessage(false);
         if (i % 2 == 0)
            message.putStringProperty(ClusterTestBase.FILTER_PROP, new SimpleString("0"));
         else
            message.putStringProperty(ClusterTestBase.FILTER_PROP, new SimpleString("1"));
         producer.send(message);
      }
      session.commit();

      servers[0].stop();

      addConsumer(0, 1, evenQueue, null);
      addConsumer(1, 1, oddQueue, null);
      for (int i = 0; i < TEST_SIZE; i++)
      {
         String compare;
         ClientMessage message;
         if (i % 2 == 0)
         {
            message = consumers[0].getConsumer().receive();
            compare = "0";
         }
         else
         {
            message = consumers[1].getConsumer().receive();
            compare = "1";
         }
         Assert.assertEquals(compare, message.getStringProperty(ClusterTestBase.FILTER_PROP));
      }

      Assert.assertNull(consumers[0].getConsumer().receive(250));
      Assert.assertNull(consumers[1].getConsumer().receive(250));
      removeConsumer(0);
      removeConsumer(1);
   }
}
