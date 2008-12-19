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
package org.jboss.messaging.tests.integration.scheduling;

import java.util.Calendar;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.UUIDGenerator;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ScheduledMessageTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ScheduledMessageTest.class);
  
   private SimpleString atestq = new SimpleString("ascheduledtestq");

   private SimpleString atestq2 = new SimpleString("ascheduledtestq2");

   private Configuration configuration;
   
   private MessagingService messagingService;

   protected void setUp() throws Exception
   {
      super.clearData();
      configuration = createDefaultConfig();
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setPagingMaxGlobalSizeBytes(-1);
      messagingService = createService(true, configuration);
      messagingService.start();
   }

   protected void tearDown() throws Exception
   {
      if (messagingService != null)
      {
         try
         {
            messagingService.stop();
            messagingService = null;
         }
         catch (Exception e)
         {
            // ignore
         }
      }
      super.tearDown();
   }

   public void testRecoveredMessageDeliveredCorrectly() throws Exception
   {
      testMessageDeliveredCorrectly(true);
   }

   public void testMessageDeliveredCorrectly() throws Exception
   {
      testMessageDeliveredCorrectly(false);
   }

   public void testScheduledMessagesDeliveredCorrectly() throws Exception
   {
      testScheduledMessagesDeliveredCorrectly(false);
   }

   public void testRecoveredScheduledMessagesDeliveredCorrectly() throws Exception
   {
      testScheduledMessagesDeliveredCorrectly(true);
   }

   public void testScheduledMessagesDeliveredCorrectlyDifferentOrder() throws Exception
   {
      testScheduledMessagesDeliveredCorrectlyDifferentOrder(false);
   }

   public void testRecoveredScheduledMessagesDeliveredCorrectlyDifferentOrder() throws Exception
   {
      testScheduledMessagesDeliveredCorrectlyDifferentOrder(true);
   }

   public void testScheduledAndNormalMessagesDeliveredCorrectly() throws Exception
   {
      testScheduledAndNormalMessagesDeliveredCorrectly(false);
   }

   public void testRecoveredScheduledAndNormalMessagesDeliveredCorrectly() throws Exception
   {
      testScheduledAndNormalMessagesDeliveredCorrectly(true);
   }

   public void testTxMessageDeliveredCorrectly() throws Exception
   {
      testTxMessageDeliveredCorrectly(false);
   }

   public void testRecoveredTxMessageDeliveredCorrectly() throws Exception
   {
      testTxMessageDeliveredCorrectly(true);
   }

   public void testPagedMessageDeliveredCorrectly() throws Exception
   {
      // then we create a client as normal
      ClientSessionFactory sessionFactory = createInVMFactory(); 
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = createMessage(session, "m1");
      long time = System.currentTimeMillis();
      time += 10000;
      message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(message);

      producer.close();

      ClientConsumer consumer = session.createConsumer(atestq);

      session.start();

      ClientMessage message2 = consumer.receive(10250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m1", message2.getBody().getString());

      message2.acknowledge();

      // Make sure no more messages
      consumer.close();
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));

      session.close();
   }

   public void testPagedMessageDeliveredMultipleConsumersCorrectly() throws Exception
   {
      QueueSettings qs = new QueueSettings();
      qs.setRedeliveryDelay(5000l);
      messagingService.getServer().getQueueSettingsRepository().addMatch(atestq2.toString(), qs);
      // then we create a client as normal
      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true, true);
      session.createQueue(atestq, atestq2, null, true, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = createMessage(session, "m1");
      producer.send(message);

      producer.close();

      ClientConsumer consumer = session.createConsumer(atestq);
      ClientConsumer consumer2 = session.createConsumer(atestq2);

      session.start();
      ClientMessage message3 = consumer.receive(1000);
      ClientMessage message2 = consumer2.receive(1000);
      assertEquals("m1", message3.getBody().getString());
      assertEquals("m1", message2.getBody().getString());
      long time = System.currentTimeMillis();
      // force redelivery
      consumer.close();
      consumer2.close();
      consumer = session.createConsumer(atestq);
      consumer2 = session.createConsumer(atestq2);
      message3 = consumer.receive(1000);
      message2 = consumer2.receive(5250);
      time += 5000;
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m1", message3.getBody().getString());
      assertEquals("m1", message2.getBody().getString());
      message2.acknowledge();
      message3.acknowledge();

      // Make sure no more messages
      consumer.close();
      consumer2.close();
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));

      session.close();
   }

   public void testPagedMessageDeliveredMultipleConsumersAfterRecoverCorrectly() throws Exception
   {

      QueueSettings qs = new QueueSettings();
      qs.setRedeliveryDelay(5000l);
      messagingService.getServer().getQueueSettingsRepository().addMatch(atestq2.toString(), qs);
      // then we create a client as normal
      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true, true);
      session.createQueue(atestq, atestq2, null, true, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = createMessage(session, "m1");
      producer.send(message);

      producer.close();

      ClientConsumer consumer = session.createConsumer(atestq);
      ClientConsumer consumer2 = session.createConsumer(atestq2);

      session.start();
      ClientMessage message3 = consumer.receive(1000);
      ClientMessage message2 = consumer2.receive(1000);
      assertEquals("m1", message3.getBody().getString());
      assertEquals("m1", message2.getBody().getString());
      long time = System.currentTimeMillis();
      // force redelivery
      consumer.close();
      consumer2.close();
      producer.close();
      session.close();
      messagingService.stop();
      messagingService = null;
      messagingService = createService(true, configuration);
      messagingService.start();
      sessionFactory = createInVMFactory();
      session = sessionFactory.createSession(false, true, true);
      consumer = session.createConsumer(atestq);
      consumer2 = session.createConsumer(atestq2);
      session.start();
      message3 = consumer.receive(1000);
      message2 = consumer2.receive(5250);
      time += 5000;
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m1", message3.getBody().getString());
      assertEquals("m1", message2.getBody().getString());
      message2.acknowledge();
      message3.acknowledge();
      
      // Make sure no more messages
      consumer.close();
      consumer2.close();
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));
      
      session.close();
   }

   public void testMessageDeliveredCorrectly(boolean recover) throws Exception
   {

       // then we create a client as normal
      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                          false,
                                                          0,
                                                          System.currentTimeMillis(),
                                                          (byte)1);
      message.getBody().putString("testINVMCoreClient");
      message.getBody().flip();
      message.setDurable(true);
      long time = System.currentTimeMillis();
      time += 10000;
      message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(message);

      log.info("Recover is " + recover);
      if (recover)
      {
         producer.close();
         session.close();
         messagingService.stop();
         messagingService = null;
         messagingService = createService(true, configuration);
         messagingService.start();
         sessionFactory = createInVMFactory();
         session = sessionFactory.createSession(false, true, true);
      }
      ClientConsumer consumer = session.createConsumer(atestq);

      session.start();

      ClientMessage message2 = consumer.receive(11000);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("testINVMCoreClient", message2.getBody().getString());

      message2.acknowledge();
      
      // Make sure no more messages
      consumer.close();   
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));
      
      session.close();
   }

   public void testScheduledMessagesDeliveredCorrectly(boolean recover) throws Exception
   {

      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage m1 = createMessage(session, "m1");
      ClientMessage m2 = createMessage(session, "m2");
      ClientMessage m3 = createMessage(session, "m3");
      ClientMessage m4 = createMessage(session, "m4");
      ClientMessage m5 = createMessage(session, "m5");
      long time = System.currentTimeMillis();
      time += 10000;
      m1.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m1);
      time += 1000;
      m2.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m2);
      time += 1000;
      m3.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m3);
      time += 1000;
      m4.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m4);
      time += 1000;
      m5.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m5);
      time -= 4000;
      if (recover)
      {
         producer.close();
         session.close();
         messagingService.stop();
         messagingService = null;
         messagingService = createService(true, configuration);
         messagingService.start();

         sessionFactory = createInVMFactory();

         session = sessionFactory.createSession(false, true, true);
      }

      ClientConsumer consumer = session.createConsumer(atestq);

      session.start();

      ClientMessage message = consumer.receive(11000);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m1", message.getBody().getString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m2", message.getBody().getString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m3", message.getBody().getString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m4", message.getBody().getString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m5", message.getBody().getString());
      message.acknowledge();
      
      // Make sure no more messages
      consumer.close();
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));
      
      session.close();
   }

   public void testScheduledMessagesDeliveredCorrectlyDifferentOrder(boolean recover) throws Exception
   {

      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage m1 = createMessage(session, "m1");
      ClientMessage m2 = createMessage(session, "m2");
      ClientMessage m3 = createMessage(session, "m3");
      ClientMessage m4 = createMessage(session, "m4");
      ClientMessage m5 = createMessage(session, "m5");
      long time = System.currentTimeMillis();
      time += 10000;
      m1.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m1);
      time += 3000;
      m2.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m2);
      time -= 2000;
      m3.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m3);
      time += 3000;
      m4.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m4);
      time -= 2000;
      m5.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m5);
      time -= 2000;
      ClientConsumer consumer = null;
      if (recover)
      {
         producer.close();
         session.close();
         messagingService.stop();
         messagingService = null;
         messagingService = createService(true, configuration);
         messagingService.start();

         sessionFactory = createInVMFactory();

         session = sessionFactory.createSession(false, true, true);

      }
      consumer = session.createConsumer(atestq);

      session.start();

      ClientMessage message = consumer.receive(10250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m1", message.getBody().getString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m3", message.getBody().getString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m5", message.getBody().getString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m2", message.getBody().getString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m4", message.getBody().getString());
      message.acknowledge();
      
      // Make sure no more messages
      consumer.close();
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));
      
      session.close();
   }

   public void testScheduledAndNormalMessagesDeliveredCorrectly(boolean recover) throws Exception
   {

      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage m1 = createMessage(session, "m1");
      ClientMessage m2 = createMessage(session, "m2");
      ClientMessage m3 = createMessage(session, "m3");
      ClientMessage m4 = createMessage(session, "m4");
      ClientMessage m5 = createMessage(session, "m5");
      long time = System.currentTimeMillis();
      time += 10000;
      m1.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m1);
      producer.send(m2);
      time += 1000;
      m3.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m3);
      producer.send(m4);
      time += 1000;
      m5.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(m5);
      time -= 2000;
      ClientConsumer consumer = null;
      if (recover)
      {
         producer.close();
         session.close();
         messagingService.stop();
         messagingService = null;
         messagingService = createService(true, configuration);
         messagingService.start();

         sessionFactory = createInVMFactory();

         session = sessionFactory.createSession(false, true, true);
      }

      consumer = session.createConsumer(atestq);
      session.start();

      ClientMessage message = consumer.receive(1000);
      assertEquals("m2", message.getBody().getString());
      message.acknowledge();
      message = consumer.receive(1000);
      assertEquals("m4", message.getBody().getString());
      message.acknowledge();
      message = consumer.receive(10250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m1", message.getBody().getString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m3", message.getBody().getString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m5", message.getBody().getString());
      message.acknowledge();
      
      // Make sure no more messages
      consumer.close();
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));
      
      session.close();
   }

   public void testTxMessageDeliveredCorrectly(boolean recover) throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      Xid xid2 = new XidImpl("xa2".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      
      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(true, false, false);
      session.createQueue(atestq, atestq, null, true, false);
      session.start(xid, XAResource.TMNOFLAGS);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                          false,
                                                          0,
                                                          System.currentTimeMillis(),
                                                          (byte)1);
      message.getBody().putString("testINVMCoreClient");
      message.getBody().flip();
      message.setDurable(true);
      Calendar cal = Calendar.getInstance();
      cal.roll(Calendar.SECOND, 10);
      message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, cal.getTimeInMillis());
      producer.send(message);
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      if (recover)
      {
         producer.close();
         session.close();
         messagingService.stop();
         messagingService = null;
         messagingService = createService(true, configuration);
         messagingService.start();

         sessionFactory = createInVMFactory();

         session = sessionFactory.createSession(true, false, false);
      }
      session.commit(xid, true);
      ClientConsumer consumer = session.createConsumer(atestq);

      session.start();
      session.start(xid2, XAResource.TMNOFLAGS);
      ClientMessage message2 = consumer.receive(10000);
      assertTrue(System.currentTimeMillis() >= cal.getTimeInMillis());
      assertNotNull(message2);
      assertEquals("testINVMCoreClient", message2.getBody().getString());

      message2.acknowledge();
      session.end(xid2, XAResource.TMSUCCESS);
      session.prepare(xid2);
      session.commit(xid2, true);
      consumer.close();
      // Make sure no more messages
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));
      session.close();
   }

   private ClientMessage createMessage(ClientSession session, String body)
   {
      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                          false,
                                                          0,
                                                          System.currentTimeMillis(),
                                                          (byte)1);
      message.getBody().putString(body);
      message.getBody().flip();
      message.setDurable(true);
      return message;
   }
}
