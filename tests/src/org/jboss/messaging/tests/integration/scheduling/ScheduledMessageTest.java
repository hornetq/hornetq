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
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.UUIDGenerator;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ScheduledMessageTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ScheduledMessageTest.class);

   private final SimpleString atestq = new SimpleString("ascheduledtestq");

   private final SimpleString atestq2 = new SimpleString("ascheduledtestq2");

   private Configuration configuration;

   private MessagingService messagingService;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      clearData();
      configuration = createDefaultConfig();
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setPagingMaxGlobalSizeBytes(-1);
      messagingService = createService(true, configuration);
      messagingService.start();
   }

   @Override
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
      session.createQueue(atestq, atestq, null, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = createDurableMessage(session, "m1");
      long time = System.currentTimeMillis();
      time += 10000;
      message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time);
      producer.send(message);

      producer.close();

      ClientConsumer consumer = session.createConsumer(atestq);

      session.start();

      ClientMessage message2 = consumer.receive(10250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m1", message2.getBody().readString());

      message2.acknowledge();

      // Make sure no more messages
      consumer.close();
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));

      session.close();
   }

   public void testPagedMessageDeliveredMultipleConsumersCorrectly() throws Exception
   {
      AddressSettings qs = new AddressSettings();
      qs.setRedeliveryDelay(5000l);
      messagingService.getServer().getAddressSettingsRepository().addMatch(atestq.toString(), qs);
      // then we create a client as normal
      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true);
      session.createQueue(atestq, atestq2, null, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = createDurableMessage(session, "m1");
      producer.send(message);

      producer.close();

      ClientConsumer consumer = session.createConsumer(atestq);
      ClientConsumer consumer2 = session.createConsumer(atestq2);

      session.start();
      ClientMessage message3 = consumer.receive(1000);
      ClientMessage message2 = consumer2.receive(1000);
      assertEquals("m1", message3.getBody().readString());
      assertEquals("m1", message2.getBody().readString());
      long time = System.currentTimeMillis();
      // force redelivery
      consumer.close();
      consumer2.close();
      consumer = session.createConsumer(atestq);
      consumer2 = session.createConsumer(atestq2);
      message3 = consumer.receive(5250);
      message2 = consumer2.receive(1000);
      time += 5000;
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m1", message3.getBody().readString());
      assertEquals("m1", message2.getBody().readString());
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

      AddressSettings qs = new AddressSettings();
      qs.setRedeliveryDelay(5000l);
      messagingService.getServer().getAddressSettingsRepository().addMatch(atestq.toString(), qs);
      // then we create a client as normal
      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true);
      session.createQueue(atestq, atestq2, null, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = createDurableMessage(session, "m1");
      producer.send(message);

      producer.close();

      ClientConsumer consumer = session.createConsumer(atestq);
      ClientConsumer consumer2 = session.createConsumer(atestq2);

      session.start();
      ClientMessage message3 = consumer.receive(1000);
      assertNotNull(message3);
      ClientMessage message2 = consumer2.receive(1000);
      assertNotNull(message2);
      assertEquals("m1", message3.getBody().readString());
      assertEquals("m1", message2.getBody().readString());
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
      message3 = consumer.receive(5250);
      assertNotNull(message3);
      message2 = consumer2.receive(1000);
      assertNotNull(message2);
      time += 5000;
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m1", message3.getBody().readString());
      assertEquals("m1", message2.getBody().readString());
      message2.acknowledge();
      message3.acknowledge();

      // Make sure no more messages
      consumer.close();
      consumer2.close();
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));

      session.close();
   }

   public void testMessageDeliveredCorrectly(final boolean recover) throws Exception
   {

      // then we create a client as normal
      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                          false,
                                                          0,
                                                          System.currentTimeMillis(),
                                                          (byte)1);
      message.getBody().writeString("testINVMCoreClient");
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
      assertEquals("testINVMCoreClient", message2.getBody().readString());

      message2.acknowledge();

      // Make sure no more messages
      consumer.close();
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));

      session.close();
   }

   public void testScheduledMessagesDeliveredCorrectly(final boolean recover) throws Exception
   {

      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage m1 = createDurableMessage(session, "m1");
      ClientMessage m2 = createDurableMessage(session, "m2");
      ClientMessage m3 = createDurableMessage(session, "m3");
      ClientMessage m4 = createDurableMessage(session, "m4");
      ClientMessage m5 = createDurableMessage(session, "m5");
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
      assertEquals("m1", message.getBody().readString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m2", message.getBody().readString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m3", message.getBody().readString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m4", message.getBody().readString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m5", message.getBody().readString());
      message.acknowledge();

      // Make sure no more messages
      consumer.close();
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));

      session.close();
   }

   public void testScheduledMessagesDeliveredCorrectlyDifferentOrder(final boolean recover) throws Exception
   {

      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage m1 = createDurableMessage(session, "m1");
      ClientMessage m2 = createDurableMessage(session, "m2");
      ClientMessage m3 = createDurableMessage(session, "m3");
      ClientMessage m4 = createDurableMessage(session, "m4");
      ClientMessage m5 = createDurableMessage(session, "m5");
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
      assertEquals("m1", message.getBody().readString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m3", message.getBody().readString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m5", message.getBody().readString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m2", message.getBody().readString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m4", message.getBody().readString());
      message.acknowledge();

      // Make sure no more messages
      consumer.close();
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));

      session.close();
   }

   public void testScheduledAndNormalMessagesDeliveredCorrectly(final boolean recover) throws Exception
   {

      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(false, true, false);
      session.createQueue(atestq, atestq, null, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage m1 = createDurableMessage(session, "m1");
      ClientMessage m2 = createDurableMessage(session, "m2");
      ClientMessage m3 = createDurableMessage(session, "m3");
      ClientMessage m4 = createDurableMessage(session, "m4");
      ClientMessage m5 = createDurableMessage(session, "m5");
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
      assertEquals("m2", message.getBody().readString());
      message.acknowledge();
      message = consumer.receive(1000);
      assertEquals("m4", message.getBody().readString());
      message.acknowledge();
      message = consumer.receive(10250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m1", message.getBody().readString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m3", message.getBody().readString());
      message.acknowledge();
      time += 1000;
      message = consumer.receive(1250);
      assertTrue(System.currentTimeMillis() >= time);
      assertEquals("m5", message.getBody().readString());
      message.acknowledge();

      // Make sure no more messages
      consumer.close();
      consumer = session.createConsumer(atestq);
      assertNull(consumer.receive(1000));

      session.close();
   }

   public void testTxMessageDeliveredCorrectly(final boolean recover) throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      Xid xid2 = new XidImpl("xa2".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(true, false, false);
      session.createQueue(atestq, atestq, null, true);
      session.start(xid, XAResource.TMNOFLAGS);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = createDurableMessage(session, "testINVMCoreClient");
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
      assertEquals("testINVMCoreClient", message2.getBody().readString());

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
   
   public void testScheduledDeliveryTX() throws Exception
   {
      scheduledDelivery(true);
   }

   public void testScheduledDeliveryNoTX() throws Exception
   {
      scheduledDelivery(false);
   }
   
   // Private -------------------------------------------------------

   private void scheduledDelivery(boolean tx) throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      ClientSessionFactory sessionFactory = createInVMFactory();
      ClientSession session = sessionFactory.createSession(tx, false, false);
      session.createQueue(atestq, atestq, null, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientConsumer consumer = session.createConsumer(atestq);

      session.start();
      if (tx)
      {
         session.start(xid, XAResource.TMNOFLAGS);
      }
      
      //Send one scheduled
      long now = System.currentTimeMillis();

      ClientMessage tm1 = createDurableMessage(session, "testScheduled1");
      tm1.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, now + 7000);
      producer.send(tm1);

      //First send some non scheduled messages

      ClientMessage tm2 = createDurableMessage(session, "testScheduled2");
      producer.send(tm2);

      ClientMessage tm3 = createDurableMessage(session, "testScheduled3");
      producer.send(tm3);

      ClientMessage tm4 = createDurableMessage(session, "testScheduled4");
      producer.send(tm4);


      //Now send some more scheduled messages

      ClientMessage tm5 = createDurableMessage(session, "testScheduled5");
      tm5.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, now + 5000);
      producer.send(tm5);

      ClientMessage tm6 = createDurableMessage(session, "testScheduled6");
      tm6.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, now + 4000);
      producer.send(tm6);

      ClientMessage tm7 = createDurableMessage(session, "testScheduled7");
      tm7.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, now + 3000);
      producer.send(tm7);

      ClientMessage tm8 = createDurableMessage(session, "testScheduled8");
      tm8.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, now + 6000);
      producer.send(tm8);

      //And one scheduled with a -ve number

      ClientMessage tm9 = createDurableMessage(session, "testScheduled9");
      tm9.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, -3);
      producer.send(tm9);

      if (tx)
      {
         session.end(xid, XAResource.TMSUCCESS);
         session.prepare(xid);
         session.commit(xid, true);
      } else
      {
         session.commit();
      }

      //First the non scheduled messages should be received
      forceGC();

      if (tx)
      {
         session.start(xid, XAResource.TMNOFLAGS);
      }

      ClientMessage rm1 = consumer.receive(250);
      assertNotNull(rm1);
      assertEquals("testScheduled2", rm1.getBody().readString());

      ClientMessage rm2 = consumer.receive(250);
      assertNotNull(rm2);
      assertEquals("testScheduled3", rm2.getBody().readString());

      ClientMessage rm3 = consumer.receive(250);
      assertNotNull(rm3);
      assertEquals("testScheduled4", rm3.getBody().readString());

      //Now the one with a scheduled with a -ve number
      ClientMessage rm5 = consumer.receive(250);
      assertNotNull(rm5);
      assertEquals("testScheduled9", rm5.getBody().readString());

      //Now the scheduled
      ClientMessage rm6 = consumer.receive(3250);
      assertNotNull(rm6);
      assertEquals("testScheduled7", rm6.getBody().readString());

      long now2 = System.currentTimeMillis();

      assertTrue(now2 - now >= 3000);

      ClientMessage rm7 = consumer.receive(1250);
      assertNotNull(rm7);
      assertEquals("testScheduled6", rm7.getBody().readString());

      now2 = System.currentTimeMillis();

      assertTrue(now2 - now >= 4000);

      ClientMessage rm8 = consumer.receive(1250);
      assertNotNull(rm8);
      assertEquals("testScheduled5", rm8.getBody().readString());

      now2 = System.currentTimeMillis();

      assertTrue(now2 - now >= 5000);

      ClientMessage rm9 = consumer.receive(1250);
      assertNotNull(rm9);
      assertEquals("testScheduled8", rm9.getBody().readString());

      now2 = System.currentTimeMillis();

      assertTrue(now2 - now >= 6000);

      ClientMessage rm10 = consumer.receive(1250);
      assertNotNull(rm10);
      assertEquals("testScheduled1", rm10.getBody().readString());

      now2 = System.currentTimeMillis();

      assertTrue(now2 - now >= 7000);

      if (tx)
      {
         session.end(xid, XAResource.TMSUCCESS);
         session.prepare(xid);
         session.commit(xid, true);
      }
      
      session.close();
      sessionFactory.close();
   }

   private ClientMessage createDurableMessage(final ClientSession session, final String body)
   {
      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                          true,
                                                          0,
                                                          System.currentTimeMillis(),
                                                          (byte)1);
      message.getBody().writeString(body);
      return message;
   }
}
