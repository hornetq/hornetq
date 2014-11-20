/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.tests.integration.discovery;

import java.net.InetAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.BroadcastEndpoint;
import org.hornetq.api.core.BroadcastEndpointFactory;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.server.cluster.impl.BroadcastGroupImpl;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.utils.HornetQThreadFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class DiscoveryStayAliveTest extends DiscoveryBaseTest
{


   ScheduledExecutorService scheduledExecutorService;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
                                                                 new HornetQThreadFactory("HornetQ-scheduled-threads",
                                                                                          false,
                                                                                          Thread.currentThread().getContextClassLoader()));

   }

   public void tearDown() throws Exception
   {
      scheduledExecutorService.shutdown();
      super.tearDown();
   }

   @Test
   public void testDiscoveryRunning() throws Throwable
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;


      final DiscoveryGroup dg = newDiscoveryGroup(RandomUtil.randomString(),
                                                  RandomUtil.randomString(),
                                                  null,
                                                  groupAddress,
                                                  groupPort,
                                                  timeout);

      final AtomicInteger errors = new AtomicInteger(0);
      Thread t = new Thread()
      {
         public void run()
         {
            try
            {
               dg.internalRunning();
            }
            catch (Throwable e)
            {
               e.printStackTrace();
               errors.incrementAndGet();
            }

         }
      };
      t.start();


      BroadcastGroupImpl bg = new BroadcastGroupImpl(new FakeNodeManager("test-nodeID"),
                                  RandomUtil.randomString(),
                                  1, scheduledExecutorService, new UDPBroadcastGroupConfiguration(address1, groupPort, null, -1).createBroadcastEndpointFactory());

      bg.start();

      bg.addConnector(generateTC());


      for (int i = 0; i < 10; i++)
      {
         BroadcastEndpointFactory factoryEndpoint = new UDPBroadcastGroupConfiguration(address1, groupPort, null, -1).createBroadcastEndpointFactory();
         sendBadData(factoryEndpoint);

         Thread.sleep(100);
         assertTrue(t.isAlive());
         assertEquals(0, errors.get());
      }

      bg.stop();
      dg.stop();

      t.join(5000);

      Assert.assertFalse(t.isAlive());

   }


   private static void sendBadData(BroadcastEndpointFactory factoryEndpoint) throws Exception
   {
      BroadcastEndpoint endpoint = factoryEndpoint.createBroadcastEndpoint();


      HornetQBuffer buffer = HornetQBuffers.dynamicBuffer(500);

      buffer.writeString("This is a test1!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
      buffer.writeString("This is a test2!!!!!!!!!!!!!!!!!!!!!!!!!!!!");


      byte[] bytes = new byte[buffer.writerIndex()];

      buffer.readBytes(bytes);

      // messing up with the string!!!
      for (int i = bytes.length - 10; i < bytes.length; i++)
      {
         bytes[i] = 0;
      }


      endpoint.openBroadcaster();

      endpoint.broadcast(bytes);
   }
}
