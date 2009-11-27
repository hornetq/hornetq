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
package org.hornetq.tests.integration.scheduling;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class DelayedMessageTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(DelayedMessageTest.class);

   private Configuration configuration;

   private HornetQServer server;

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
      server = createServer(true, configuration);
      server.start();
      
      AddressSettings qs = server.getAddressSettingsRepository().getMatch("*");
      AddressSettings newSets = new AddressSettings();
      newSets.setRedeliveryDelay(DELAY);
      newSets.merge(qs);
      server.getAddressSettingsRepository().addMatch(qName, newSets);

   }

   @Override
   protected void tearDown() throws Exception
   {
      if (server != null)
      {
         try
         {
            server.getAddressSettingsRepository().removeMatch(qName);

            server.stop();
            server = null;
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

         session.createQueue(qName, qName, null, true);
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
         
            assertEquals("message" + i, tm.getBodyBuffer().readString());
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


         session.createQueue(qName, qName, null, true);
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
            assertEquals("message" + i, tm.getBodyBuffer().readString());
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
         assertEquals("message" + i, tm.getBodyBuffer().readString());
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
      ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                          true,
                                                          0,
                                                          System.currentTimeMillis(),
                                                          (byte)1);
      message.getBodyBuffer().writeString(body);
      return message;
   }
}
