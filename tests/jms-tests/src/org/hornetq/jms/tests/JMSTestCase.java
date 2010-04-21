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

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.jms.client.HornetQConnectionFactory;

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

      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory"),
                                                                                    null));


      getJmsServerManager().createConnectionFactory("testsuitecf",
                                                    connectorConfigs,
                                                    null,
                                                    HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                    HornetQClient.DEFAULT_CONNECTION_TTL,
                                                    HornetQClient.DEFAULT_CALL_TIMEOUT,
                                                    HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                                    HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                    HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                                                    HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                                                    true,
                                                    true,
                                                    true,
                                                    HornetQClient.DEFAULT_AUTO_GROUP,
                                                    HornetQClient.DEFAULT_PRE_ACKNOWLEDGE,
                                                    HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                    HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                                    HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                                    HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                                                    HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                                    HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                                                    HornetQClient.DEFAULT_RETRY_INTERVAL,
                                                    HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                    HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                    HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                                                    HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
                                                    HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN,
                                                    null,
                                                    "/testsuitecf");

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
