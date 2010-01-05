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

package org.hornetq.jms.tests;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.hornetq.Pair;
import org.hornetq.core.client.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.jms.HornetQConnectionFactory;

/**
 * Safeguards for previously detected TCK failures.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.org">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CTSMiscellaneousTest extends HornetQServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
   protected static HornetQConnectionFactory cf;

   private static final String ORG_JBOSS_MESSAGING_SERVICE_LBCONNECTION_FACTORY = "StrictTCKConnectionFactory";

   // Constructors --------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      try
      {
         super.setUp();
         // Deploy a connection factory with load balancing but no failover on node0
         List<String> bindings = new ArrayList<String>();
         bindings.add("StrictTCKConnectionFactory");

         List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();

         connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(new TransportConfiguration("org.hornetq.integration.transports.netty.NettyConnectorFactory"),
                                                                                       null));

         List<String> jndiBindings = new ArrayList<String>();
         jndiBindings.add("/StrictTCKConnectionFactory");

         getJmsServerManager().createConnectionFactory("StrictTCKConnectionFactory",
                                                       connectorConfigs,
                                                       null,
                                                       ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                       ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL,
                                                       ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                       ClientSessionFactoryImpl.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                                       ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                       ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                       ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                                       ClientSessionFactoryImpl.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                       ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE,
                                                       ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                                       true,
                                                       true,
                                                       true,
                                                       ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                                       ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE,
                                                       ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                       ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                       ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                       ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS,
                                                       ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                                       ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE,
                                                       ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL,
                                                       ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                       ClientSessionFactoryImpl.DEFAULT_MAX_RETRY_INTERVAL,
                                                       ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS,
                                                       ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN,
                                                       null,
                                                       jndiBindings);

         CTSMiscellaneousTest.cf = (HornetQConnectionFactory)getInitialContext().lookup("/StrictTCKConnectionFactory");
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

   }

   // Public --------------------------------------------------------

   /* By default we send non persistent messages asynchronously for performance reasons
    * when running with strictTCK we send them synchronously
    */
   public void testNonPersistentMessagesSentSynchronously() throws Exception
   {
      Connection c = null;

      try
      {
         c = CTSMiscellaneousTest.cf.createConnection();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer p = s.createProducer(HornetQServerTestCase.queue1);

         p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         final int numMessages = 100;

         assertRemainingMessages(0);

         for (int i = 0; i < numMessages; i++)
         {
            p.send(s.createMessage());
         }

         assertRemainingMessages(numMessages);
      }
      finally
      {
         if (c != null)
         {
            c.close();
         }

         removeAllMessages(HornetQServerTestCase.queue1.getQueueName(), true);
      }
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
      HornetQServerTestCase.undeployConnectionFactory(CTSMiscellaneousTest.ORG_JBOSS_MESSAGING_SERVICE_LBCONNECTION_FACTORY);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
