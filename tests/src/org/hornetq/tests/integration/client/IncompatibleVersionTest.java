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

package org.hornetq.tests.integration.client;

import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.client.impl.FailoverManagerImpl;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.VersionLoader;

/**
 * A IncompatibleVersionTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class IncompatibleVersionTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private CoreRemotingConnection connection;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      server = createServer(false, false);
      server.getConfiguration().setConnectionTTLOverride(500);
      server.start();

      TransportConfiguration config = new TransportConfiguration(InVMConnectorFactory.class.getName());
      ClientSessionFactory csf = HornetQClient.createClientSessionFactory(config);
      ExecutorService executorService = Executors.newFixedThreadPool(1);
      ScheduledExecutorService scheduledexecutorService = Executors.newScheduledThreadPool(1);
      FailoverManagerImpl failoverManager = new FailoverManagerImpl(csf,
                                                                    config,
                                                                    null,
                                                                    HornetQClient.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN,
                                                                    HornetQClient.DEFAULT_CALL_TIMEOUT,
                                                                    HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                                    500,
                                                                    HornetQClient.DEFAULT_RETRY_INTERVAL,
                                                                    HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                                    HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                                    HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                                                                    executorService,
                                                                    scheduledexecutorService,
                                                                    null);
      connection = failoverManager.getConnection();
   }

   @Override
   protected void tearDown() throws Exception
   {
      connection.destroy();
      
      server.stop();
   }

   public void testCompatibleClientVersion() throws Exception
   {
      doTestClientVersionCompatibility(true);
   }

   public void testIncompatibleClientVersion() throws Exception
   {
      doTestClientVersionCompatibility(false);
   }

   private void doTestClientVersionCompatibility(boolean compatible) throws Exception
   {
      Channel channel1 = connection.getChannel(1, -1);
      long sessionChannelID = connection.generateChannelID();
      int version = VersionLoader.getVersion().getIncrementingVersion();
      if (!compatible)
      {
         version = -1;
      }
      Packet request = new CreateSessionMessage(randomString(),
                                                sessionChannelID,
                                                version,
                                                null,
                                                null,
                                                HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                false,
                                                true,
                                                true,
                                                false,
                                                HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE);

      if (compatible)
      {
         Packet packet = channel1.sendBlocking(request);
         assertNotNull(packet);
         assertTrue(packet instanceof CreateSessionResponseMessage);
         // 1 connection on the server
         assertEquals(1, server.getConnectionCount());
      }
      else
      {
         try
         {
            channel1.sendBlocking(request);
            fail();
         }
         catch (HornetQException e)
         {
            assertEquals(HornetQException.INCOMPATIBLE_CLIENT_SERVER_VERSIONS, e.getCode());
         }
         long start = System.currentTimeMillis();
         while (System.currentTimeMillis() < start + 3 * RemotingServiceImpl.CONNECTION_TTL_CHECK_INTERVAL)
         {
            if (server.getConnectionCount() == 0)
            {
               break;
            }
         }
         // no connection on the server
         assertEquals(0, server.getConnectionCount());
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
