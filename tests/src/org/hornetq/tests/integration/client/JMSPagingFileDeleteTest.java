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

package org.hornetq.tests.integration.client;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.JMSTestBase;

public class JMSPagingFileDeleteTest extends JMSTestBase
{
   static Logger log = Logger.getLogger(JMSPagingFileDeleteTest.class);
   
   Topic topic1;

   Connection connection;

   Session session;

   MessageConsumer subscriber1;

   MessageConsumer subscriber2;

   PagingStore pagingStore;

   private static final int MESSAGE_SIZE = 1024;

   private static final int PAGE_SIZE = 10 * 1024;

   private static final int PAGE_MAX = 20 * 1024;

   private static final int RECEIVE_TIMEOUT = 500;

   private static final int MESSAGE_NUM = 50;

   @Override
   protected boolean usePersistence()
   {
      return true;
   }

   @Override
   protected void setUp() throws Exception
   {
      clearData();
      super.setUp();

      topic1 = createTopic("topic1");

      // Paging Setting
      AddressSettings setting = new AddressSettings();
      setting.setPageSizeBytes(JMSPagingFileDeleteTest.PAGE_SIZE);
      setting.setMaxSizeBytes(JMSPagingFileDeleteTest.PAGE_MAX);
      server.getAddressSettingsRepository().addMatch("#", setting);
   }

   @Override
   protected void tearDown() throws Exception
   {
      topic1 = null;
      super.tearDown();
   }
   
   public void testTopics() throws Exception
   {
      connection = null;

      try
      {
         connection = cf.createConnection();
         connection.setClientID("cid");

         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(topic1);
         subscriber1 = session.createDurableSubscriber(topic1, "subscriber-1");
         subscriber2 = session.createDurableSubscriber(topic1, "subscriber-2");

         // -----------------(Step1) Publish Messages to make Paging Files. --------------------
         System.out.println("---------- Send messages. ----------");
         BytesMessage bytesMessage = session.createBytesMessage();
         bytesMessage.writeBytes(new byte[JMSPagingFileDeleteTest.MESSAGE_SIZE]);
         for (int i = 0; i < JMSPagingFileDeleteTest.MESSAGE_NUM; i++)
         {
            producer.send(bytesMessage);
         }
         System.out.println("Sent " + JMSPagingFileDeleteTest.MESSAGE_NUM + " messages.");

         pagingStore = server.getPagingManager().getPageStore(new SimpleString("jms.topic.topic1"));
         printPageStoreInfo(pagingStore);

         assertTrue(pagingStore.isPaging());

         connection.start();

         // -----------------(Step2) Restart the server. --------------------------------------
         stopAndStartServer(); // If try this test without restarting server, please comment out this line;

         // -----------------(Step3) Subscribe to all the messages from the topic.--------------
         System.out.println("---------- Receive all messages. ----------");
         for (int i = 0; i < JMSPagingFileDeleteTest.MESSAGE_NUM; i++)
         {
            Message message1 = subscriber1.receive(JMSPagingFileDeleteTest.RECEIVE_TIMEOUT);
            assertNotNull(message1);
            Message message2 = subscriber2.receive(JMSPagingFileDeleteTest.RECEIVE_TIMEOUT);
            assertNotNull(message2);
         }

         pagingStore = server.getPagingManager().getPageStore(new SimpleString("jms.topic.topic1"));
         long timeout = System.currentTimeMillis() + 5000;
         while (timeout > System.currentTimeMillis() && pagingStore.isPaging())
         {
            Thread.sleep(100);
         }
         assertFalse(pagingStore.isPaging());
         
         printPageStoreInfo(pagingStore);

         assertEquals(0, pagingStore.getAddressSize());
         // assertEquals(1, pagingStore.getNumberOfPages()); //I expected number of the page is 1, but It was not.
         assertFalse(pagingStore.isPaging()); // I expected IsPaging is false, but It was true.
         // If the server is not restart, this test pass.

         // -----------------(Step4) Publish a message. the message is stored in the paging file.
         producer = session.createProducer(topic1);
         bytesMessage = session.createBytesMessage();
         bytesMessage.writeBytes(new byte[JMSPagingFileDeleteTest.MESSAGE_SIZE]);
         producer.send(bytesMessage);

         printPageStoreInfo(pagingStore);

         assertEquals(1, pagingStore.getNumberOfPages()); //I expected number of the page is 1, but It was not.
      }
      finally
      {
         if (connection != null)
         {
            connection.close();
         }
      }
   }

   private void stopAndStartServer() throws Exception
   {
      System.out.println("---------- Restart server. ----------");
      connection.close();

      jmsServer.stop();

      jmsServer.start();
      jmsServer.activated();
      registerConnectionFactory();

      printPageStoreInfo(pagingStore);
      reconnect();
   }

   private void reconnect() throws Exception
   {
      connection = cf.createConnection();
      connection.setClientID("cid");
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      subscriber1 = session.createDurableSubscriber(topic1, "subscriber-1");
      subscriber2 = session.createDurableSubscriber(topic1, "subscriber-2");
      connection.start();
   }

   private void printPageStoreInfo(PagingStore pagingStore) throws Exception
   {
      System.out.println("---------- Paging Store Info ----------");
      System.out.println(" CurrentPage = " + pagingStore.getCurrentPage());
      System.out.println(" FirstPage = " + pagingStore.getFirstPage());
      System.out.println(" Number of Pages = " + pagingStore.getNumberOfPages());
      System.out.println(" Address Size = " + pagingStore.getAddressSize());
      System.out.println(" Is Paging = " + pagingStore.isPaging());
   }
}