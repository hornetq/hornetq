/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.integration.persistence;

import junit.framework.Assert;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.impl.journal.LargeServerMessageImpl;
import org.hornetq.core.persistence.impl.journal.XmlDataExporter;
import org.hornetq.core.persistence.impl.journal.XmlDataImporter;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.UUIDGenerator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * A test of the XML export/import functionality
 *
 * @author Justin Bertram
 */
public class XmlImportExportTest extends ServiceTestBase
{
   public static final int CONSUMER_TIMEOUT = 5000;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   protected void tearDown() throws Exception
   {
   }

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   public void testMessageProperties() throws Exception
   {
      final String QUEUE_NAME = "A1";
      HornetQServer server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(QUEUE_NAME, QUEUE_NAME);

      ClientProducer producer = session.createProducer(QUEUE_NAME);

      StringBuilder international = new StringBuilder();
      for (char x = 800; x < 1200; x++)
      {
         international.append(x);
      }

      String special = "\"<>'&";

      for (int i = 0; i < 5; i++)
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

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearData();
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      for (int i = 0; i < 5; i++)
      {
         ClientMessage msg = consumer.receive(CONSUMER_TIMEOUT);
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
         Assert.assertEquals(i * 1.6, msg.getDoubleProperty("myDoubleProperty"));
         Assert.assertEquals(i * 2.5F, msg.getFloatProperty("myFloatProperty"));
         Assert.assertEquals(i, msg.getIntProperty("myIntProperty").intValue());
         Assert.assertEquals(Long.MAX_VALUE - i, msg.getLongProperty("myLongProperty").longValue());
         Assert.assertEquals(i, msg.getObjectProperty("myObjectProperty"));
         Assert.assertEquals(new Integer(i).shortValue(), msg.getShortProperty("myShortProperty").shortValue());
         Assert.assertEquals("myStringPropertyValue_" + i, msg.getStringProperty("myStringProperty"));
         Assert.assertEquals(international.toString(), msg.getStringProperty("myNonAsciiStringProperty"));
         Assert.assertEquals(special, msg.getStringProperty("mySpecialCharacters"));
      }

      session.close();
      locator.close();
      server.stop();
   }

   public void testMessageTypes() throws Exception
   {
      final String QUEUE_NAME = "A1";
      HornetQServer server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(QUEUE_NAME, QUEUE_NAME);

      ClientProducer producer = session.createProducer(QUEUE_NAME);


      ClientMessage msg = session.createMessage(Message.BYTES_TYPE, true);
      producer.send(msg);
      msg = session.createMessage(Message.DEFAULT_TYPE, true);
      producer.send(msg);
      msg = session.createMessage(Message.MAP_TYPE, true);
      producer.send(msg);
      msg = session.createMessage(Message.OBJECT_TYPE, true);
      producer.send(msg);
      msg = session.createMessage(Message.STREAM_TYPE, true);
      producer.send(msg);
      msg = session.createMessage(Message.TEXT_TYPE, true);
      producer.send(msg);
      msg = session.createMessage(true);
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearData();
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      msg = consumer.receive(CONSUMER_TIMEOUT);
      Assert.assertEquals(Message.BYTES_TYPE, msg.getType());
      msg = consumer.receive(CONSUMER_TIMEOUT);
      Assert.assertEquals(Message.DEFAULT_TYPE, msg.getType());
      msg = consumer.receive(CONSUMER_TIMEOUT);
      Assert.assertEquals(Message.MAP_TYPE, msg.getType());
      msg = consumer.receive(CONSUMER_TIMEOUT);
      Assert.assertEquals(Message.OBJECT_TYPE, msg.getType());
      msg = consumer.receive(CONSUMER_TIMEOUT);
      Assert.assertEquals(Message.STREAM_TYPE, msg.getType());
      msg = consumer.receive(CONSUMER_TIMEOUT);
      Assert.assertEquals(Message.TEXT_TYPE, msg.getType());
      msg = consumer.receive(CONSUMER_TIMEOUT);
      Assert.assertEquals(Message.DEFAULT_TYPE, msg.getType());

      session.close();
      locator.close();
      server.stop();
   }

   public void testMessageAttributes() throws Exception
   {
      final String QUEUE_NAME = "A1";
      HornetQServer server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(QUEUE_NAME, QUEUE_NAME);

      ClientProducer producer = session.createProducer(QUEUE_NAME);

      ClientMessage msg = session.createMessage(Message.BYTES_TYPE, true);
      msg.setExpiration(Long.MAX_VALUE);
      msg.setPriority((byte) 0);
      msg.setTimestamp(Long.MAX_VALUE - 1);
      msg.setUserID(UUIDGenerator.getInstance().generateUUID());
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearData();
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      msg = consumer.receive(CONSUMER_TIMEOUT);
      Assert.assertEquals(Long.MAX_VALUE, msg.getExpiration());
      Assert.assertEquals((byte) 0, msg.getPriority());
      Assert.assertEquals(Long.MAX_VALUE - 1, msg.getTimestamp());
      Assert.assertNotNull(msg.getUserID());

      session.close();
      locator.close();
      server.stop();
   }

   public void testBindingAttributes() throws Exception
   {
      HornetQServer server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue("addressName1", "queueName1");
      session.createQueue("addressName1", "queueName2", "bob", true);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearData();
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();

      ClientSession.QueueQuery queueQuery = session.queueQuery(new SimpleString("queueName1"));

      Assert.assertEquals("addressName1", queueQuery.getAddress().toString());
      Assert.assertNull(queueQuery.getFilterString());

      queueQuery = session.queueQuery(new SimpleString("queueName2"));

      Assert.assertEquals("addressName1", queueQuery.getAddress().toString());
      Assert.assertEquals("bob", queueQuery.getFilterString().toString());
      Assert.assertEquals(Boolean.TRUE.booleanValue(), queueQuery.isDurable());

      session.close();
      locator.close();
      server.stop();
   }

   public void testLargeMessage() throws Exception
   {
      HornetQServer server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, false);

      LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) server.getStorageManager());

      fileMessage.setMessageID(1005);
      fileMessage.setDurable(true);

      for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
      {
         fileMessage.addBytes(new byte[]{UnitTestCase.getSamplebyte(i)});
      }

      fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

      fileMessage.releaseResources();

      session.createQueue("A", "A");

      ClientProducer prod = session.createProducer("A");

      prod.send(fileMessage);

      fileMessage.deleteFile();

      session.commit();

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearData();
      server.start();
      locator = createFactory(false);
      factory = locator.createSessionFactory();
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();
      session.close();
      session = factory.createSession(false, false);
      session.start();

      ClientConsumer cons = session.createConsumer("A");

      ClientMessage msg = cons.receive(CONSUMER_TIMEOUT);

      Assert.assertNotNull(msg);

      Assert.assertEquals(2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, msg.getBodySize());

      for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
      {
         Assert.assertEquals(UnitTestCase.getSamplebyte(i), msg.getBodyBuffer().readByte());
      }

      msg.acknowledge();
      session.commit();

      session.close();
      factory.close();
      locator.close();
      server.stop();
   }

   public void testPartialQueue() throws Exception
   {
      HornetQServer server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue("myAddress", "myQueue1");
      session.createQueue("myAddress", "myQueue2");

      ClientProducer producer = session.createProducer("myAddress");

      ClientMessage msg = session.createMessage(true);
      producer.send(msg);

      ClientConsumer consumer = session.createConsumer("myQueue1");
      session.start();
      msg = consumer.receive(CONSUMER_TIMEOUT);
      Assert.assertNotNull(msg);
      msg.acknowledge();
      consumer.close();

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearData();
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();
      consumer = session.createConsumer("myQueue1");
      session.start();
      msg = consumer.receive(CONSUMER_TIMEOUT);
      Assert.assertNull(msg);
      consumer.close();

      consumer = session.createConsumer("myQueue2");
      msg = consumer.receive(CONSUMER_TIMEOUT);
      Assert.assertNotNull(msg);

      session.close();
      locator.close();
      server.stop();
   }

   public void testPaging() throws Exception
   {
      final String MY_ADDRESS = "myAddress";
      final String MY_QUEUE = "myQueue";

      HornetQServer server = createServer(true);

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(20 * 1024);
      server.getAddressSettingsRepository().addMatch("#", defaultSetting);
      server.start();

      ServerLocator locator = createInVMNonHALocator();
      // Making it synchronous, just because we want to stop sending messages as soon as the page-store becomes in
      // page mode and we could only guarantee that by setting it to synchronous
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(MY_ADDRESS, MY_QUEUE, true);

      ClientProducer producer = session.createProducer(MY_ADDRESS);

      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeBytes(new byte[1024]);

      for (int i = 0; i < 200; i++)
      {
         producer.send(message);
      }

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearData();
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session);
      xmlDataImporter.processXml();

      ClientConsumer consumer = session.createConsumer(MY_QUEUE);

      session.start();

      for (int i = 0; i < 200; i++)
      {
         message = consumer.receive(CONSUMER_TIMEOUT);

         Assert.assertNotNull(message);
      }

      session.close();
      locator.close();
      server.stop();
   }

   public void testTransactional() throws Exception
   {
      final String QUEUE_NAME = "A1";
      HornetQServer server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(QUEUE_NAME, QUEUE_NAME);

      ClientProducer producer = session.createProducer(QUEUE_NAME);


      ClientMessage msg = session.createMessage(true);
      producer.send(msg);

      session.close();
      locator.close();
      server.stop();

      ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      System.out.print(new String(xmlOutputStream.toByteArray()));

      clearData();
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, false, true);
      ClientSession managementSession = factory.createSession(false, true, true);

      ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(xmlOutputStream.toByteArray());
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session, managementSession);
      xmlDataImporter.processXml();
      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      msg = consumer.receive(CONSUMER_TIMEOUT);
      Assert.assertNotNull(msg);

      session.close();
      locator.close();
      server.stop();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
