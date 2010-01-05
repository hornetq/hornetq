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

import javax.naming.InitialContext;

import org.hornetq.api.Pair;
import org.hornetq.api.core.client.ClientSessionFactoryImpl;
import org.hornetq.api.core.config.TransportConfiguration;
import org.hornetq.api.jms.HornetQConnectionFactory;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>23 Jul 2007
 *          <p/>
 *          $Id: $
 */
public class JMSTestCase extends HornetQServerTestCase
{

   protected static HornetQConnectionFactory cf;

   protected static InitialContext ic;

   protected static final String defaultConf = "all";

   protected static String conf;

   protected String overrideConf;

   protected boolean startHornetQServer = true;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      JMSTestCase.ic = getInitialContext();

      // All jms tests should use a specific cg which has blockOnAcknowledge = true and
      // both np and p messages are sent synchronously

      List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();

      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(new TransportConfiguration("org.hornetq.integration.transports.netty.NettyConnectorFactory"),
                                                                                    null));

      List<String> jndiBindings = new ArrayList<String>();
      jndiBindings.add("/testsuitecf");

      getJmsServerManager().createConnectionFactory("testsuitecf",
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

      JMSTestCase.cf = (HornetQConnectionFactory)getInitialContext().lookup("/testsuitecf");

      assertRemainingMessages(0);
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();

      getJmsServerManager().destroyConnectionFactory("testsuitecf");

      JMSTestCase.cf = null;

      assertRemainingMessages(0);
   }
}
