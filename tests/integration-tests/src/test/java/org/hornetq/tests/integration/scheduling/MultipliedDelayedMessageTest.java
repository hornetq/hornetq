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
import org.junit.Before;

import org.junit.Test;

import org.junit.Assert;

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="jbertram@redhat.com">Justin Bertram</a>
 */
public class MultipliedDelayedMessageTest extends ServiceTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private Configuration configuration;

   private HornetQServer server;

   private static final long DELAY = 1000;

   private static final double MULTIPLIER = 2.0;

   private static final long  MAX_DELAY = 17000;

   private final String queueName = "MultipliedDelayedMessageTestQueue";

   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      initServer();
   }

   /**
    * @throws Exception
    */
   protected void initServer() throws Exception
   {
      configuration = createDefaultConfig();
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      server = createServer(true, configuration);
      server.start();

      // Create settings to enable multiplied redelivery delay
      AddressSettings addressSettings = server.getAddressSettingsRepository().getMatch("*");
      AddressSettings newAddressSettings = new AddressSettings();
      newAddressSettings.setRedeliveryDelay(DELAY);
      newAddressSettings.setRedeliveryMultiplier(MULTIPLIER);
      newAddressSettings.setMaxRedeliveryDelay(MAX_DELAY);
      newAddressSettings.merge(addressSettings);
      server.getAddressSettingsRepository().addMatch(queueName, newAddressSettings);
      locator = createInVMNonHALocator();
   }

   @Test
   public void testMultipliedDelayedRedeliveryOnClose() throws Exception
   {
      ClientSessionFactory sessionFactory = createSessionFactory(locator);

      // Session for creating the queue
      ClientSession session = sessionFactory.createSession(false, false, false);
      session.createQueue(queueName, queueName, null, true);
      session.close();

      // Session for sending the message
      session = sessionFactory.createSession(false, true, true);
      ClientProducer producer = session.createProducer(queueName);
      UnitTestCase.forceGC();
      ClientMessage tm = createDurableMessage(session, "message");
      producer.send(tm);
      session.close();

      // Session for consuming the message
      session = sessionFactory.createSession(false, false, false);
      ClientConsumer consumer = session.createConsumer(queueName);
      session.start();
      tm = consumer.receive(500);
      Assert.assertNotNull(tm);

      for (int i = 1; i <= 6; i++)
      {
         // Ack the message, but rollback the session to trigger redelivery with increasing delivery count
         long start = System.currentTimeMillis();
         tm.acknowledge();
         session.rollback();

         long expectedDelay = calculateExpectedDelay(DELAY, MAX_DELAY, MULTIPLIER, i);
         log.info("\nExpected delay: " + expectedDelay);
         tm = consumer.receive(expectedDelay + 500);
         long stop = System.currentTimeMillis();
         Assert.assertNotNull(tm);
         log.info("Actual delay: " + (stop - start));
         Assert.assertTrue(stop - start >= expectedDelay);
      }

      tm.acknowledge();
      session.commit();
      session.close();
   }

   // Private -------------------------------------------------------

   private ClientMessage createDurableMessage(final ClientSession session, final String body)
   {
      ClientMessage message = session.createMessage(HornetQTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)1);
      message.getBodyBuffer().writeString(body);
      return message;
   }

   // This is based on org.hornetq.core.server.impl.QueueImpl.calculateRedeliveryDelay()
   private long calculateExpectedDelay(final long redeliveryDelay, final long maxRedeliveryDelay, final double redeliveryMultiplier, final int deliveryCount) {
      int tmpDeliveryCount = deliveryCount > 0 ? deliveryCount - 1 : 0;
      long delay = (long) (redeliveryDelay * (Math.pow(redeliveryMultiplier, tmpDeliveryCount)));

      if (delay > maxRedeliveryDelay)
      {
         delay = maxRedeliveryDelay;
      }

      return delay;
   }
}