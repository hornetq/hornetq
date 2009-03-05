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

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.ServiceTestBase;

/**
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class DelayedMessageTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(DelayedMessageTest.class);

   private Configuration configuration;

   private MessagingService messagingService;

   private static final long DELAY = 3000;
   
   private final String qName = "DelayedMessageTestQueue";

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
      
      AddressSettings qs = messagingService.getServer().getAddressSettingsRepository().getMatch("*");
      AddressSettings newSets = new AddressSettings();
      newSets.setRedeliveryDelay(DELAY);
      newSets.merge(qs);
      messagingService.getServer().getAddressSettingsRepository().addMatch(qName, newSets);

   }

   @Override
   protected void tearDown() throws Exception
   {
      if (messagingService != null)
      {
         try
         {
            messagingService.getServer().getAddressSettingsRepository().removeMatch(qName);

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
   
   public void testDelayedRedeliveryDefaultOnClose() throws Exception
   {
         ClientSessionFactory sessionFactory = createInVMFactory();
         ClientSession session = sessionFactory.createSession(false, false, false);

         session.createQueue(qName, qName, null, true, false);
         session.close();
         
         ClientSession session1 = sessionFactory.createSession(false, true, true);
         ClientProducer producer = session1.createProducer(qName);
         
         final int NUM_MESSAGES = 5;
         
         forceGC();
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            ClientMessage tm = this.createDurableMessage(session1, "message" + i);
            producer.send(tm);
         }
         
         session1.close();
         
         ClientSession session2 = sessionFactory.createSession(false, false, false);
         
         ClientConsumer consumer2 = session2.createConsumer(qName);
         session2.start();
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            ClientMessage tm = consumer2.receive(500);
         
            assertNotNull(tm);
         
            assertEquals("message" + i, tm.getBody().readString());
         }
         
         //Now close the session
         //This should cancel back to the queue with a delayed redelivery
         
         long now = System.currentTimeMillis();
         
         session2.close();
         
         ClientSession session3 = sessionFactory.createSession(false, false, false);
         
         ClientConsumer consumer3 = session3.createConsumer(qName);
         session3.start();
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            ClientMessage tm = consumer3.receive(DELAY + 1000);
         
            assertNotNull(tm);
         
            long time = System.currentTimeMillis();
         
            assertTrue(time - now >= DELAY);
         
            //Hudson can introduce a large degree of indeterminism
            assertTrue((time - now)  + ">" + (DELAY + 1000), time - now < DELAY + 1000);
         }
         
         session3.commit();
         session3.close();
         
         sessionFactory.close();
   }
   
   public void testDelayedRedeliveryDefaultOnRollback() throws Exception
   {
         ClientSessionFactory sessionFactory = createInVMFactory();
         ClientSession session = sessionFactory.createSession(false, false, false);


         session.createQueue(qName, qName, null, true, false);
         session.close();
         
         ClientSession session1 = sessionFactory.createSession(false, true, true);         
         ClientProducer producer = session1.createProducer(qName);
         
         final int NUM_MESSAGES = 5;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            ClientMessage tm = this.createDurableMessage(session1, "message" + i);
            producer.send(tm);
         }
         session1.close();
         
         ClientSession session2 = sessionFactory.createSession(false, false, false);
         ClientConsumer consumer2 = session2.createConsumer(qName);
         
         session2.start();
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            ClientMessage tm = consumer2.receive(500);
            assertNotNull(tm);
            assertEquals("message" + i, tm.getBody().readString());
         }
         
         //Now rollback
         long now = System.currentTimeMillis();
         
         session2.rollback();
         
         //This should redeliver with a delayed redelivery
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            ClientMessage tm = consumer2.receive(DELAY + 1000);
            assertNotNull(tm);
         
            long time = System.currentTimeMillis();
         
            assertTrue(time - now >= DELAY);
         
            //Hudson can introduce a large degree of indeterminism
            assertTrue((time - now)  + ">" + (DELAY + 1000), time - now < DELAY + 1000);
         }
         
         session2.commit();
         session2.close();     
         
         sessionFactory.close();
   }
   
   // Private -------------------------------------------------------

 
   private void delayedRedeliveryDefaultOnRollback(ClientSessionFactory sessionFactory, String queueName, long delay) throws Exception
   {
      ClientSession session = sessionFactory.createSession(false, true, true);         
      ClientProducer producer = session.createProducer(queueName);

      final int NUM_MESSAGES = 5;

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage tm = createDurableMessage(session, "message" + i);
         producer.send(tm);
      }
      session.close();

      ClientSession session2 = sessionFactory.createSession(false, false, false);
      ClientConsumer consumer2 = session2.createConsumer(queueName);

      session2.start();

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage tm = consumer2.receive(500);
         assertNotNull(tm);
         assertEquals("message" + i, tm.getBody().readString());
      }

      //Now rollback
      long now = System.currentTimeMillis();

      session2.rollback();

      //This should redeliver with a delayed redelivery

      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         ClientMessage tm = consumer2.receive(delay + 1000);
         assertNotNull(tm);

         long time = System.currentTimeMillis();

         assertTrue(time - now >= delay);

         //Hudson can introduce a large degree of indeterminism
         assertTrue((time - now)  + ">" + (delay + 1000), time - now < delay + 1000);
      }

      session2.commit();
      session2.close();
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
