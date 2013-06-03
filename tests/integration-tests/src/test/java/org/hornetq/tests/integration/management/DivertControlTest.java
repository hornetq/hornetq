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

package org.hornetq.tests.integration.management;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import org.junit.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.DivertControl;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.RandomUtil;

/**
 * A BridgeControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * Created 11 dec. 2008 17:38:58
 *
 */
public class DivertControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer service;

   private DivertConfiguration divertConfig;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testAttributes() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(divertConfig.getName()));

      DivertControl divertControl = createManagementControl(divertConfig.getName());

      Assert.assertEquals(divertConfig.getFilterString(), divertControl.getFilter());

      Assert.assertEquals(divertConfig.isExclusive(), divertControl.isExclusive());

      Assert.assertEquals(divertConfig.getName(), divertControl.getUniqueName());

      Assert.assertEquals(divertConfig.getRoutingName(), divertControl.getRoutingName());

      Assert.assertEquals(divertConfig.getAddress(), divertControl.getAddress());

      Assert.assertEquals(divertConfig.getForwardingAddress(), divertControl.getForwardingAddress());

      Assert.assertEquals(divertConfig.getTransformerClassName(), divertControl.getTransformerClassName());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName());

      CoreQueueConfiguration queueConfig = new CoreQueueConfiguration(RandomUtil.randomString(),
                                                              RandomUtil.randomString(),
                                                              null,
                                                              false);
      CoreQueueConfiguration fowardQueueConfig = new CoreQueueConfiguration(RandomUtil.randomString(),
                                                                    RandomUtil.randomString(),
                                                                    null,
                                                                    false);

      divertConfig = new DivertConfiguration(RandomUtil.randomString(),
                                             RandomUtil.randomString(),
                                             queueConfig.getAddress(),
                                             fowardQueueConfig.getAddress(),
                                             RandomUtil.randomBoolean(),
                                             null,
                                             null);
      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getQueueConfigurations().add(queueConfig);
      conf.getQueueConfigurations().add(fowardQueueConfig);
      conf.getDivertConfigurations().add(divertConfig);

      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      conf.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);

      service = HornetQServers.newHornetQServer(conf, mbeanServer, false);
      service.start();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      service.stop();

      checkNoResource(ObjectNameBuilder.DEFAULT.getDivertObjectName(divertConfig.getName()));

      service = null;

      divertConfig = null;

      super.tearDown();
   }

   protected DivertControl createManagementControl(final String name) throws Exception
   {
      return ManagementControlHelper.createDivertControl(name, mbeanServer);
   }
}
