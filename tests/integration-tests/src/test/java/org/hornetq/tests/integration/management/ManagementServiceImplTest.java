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

import org.junit.Test;

import org.junit.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.AddressControl;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.QueueControl;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.persistence.impl.nullpm.NullStorageManager;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.server.management.impl.ManagementServiceImpl;
import org.hornetq.tests.integration.server.FakeStorageManager;
import org.hornetq.tests.unit.core.postoffice.impl.FakeQueue;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/*
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ManagementServiceImplTest extends UnitTestCase
{
   @Test
   public void testHandleManagementMessageWithOperation() throws Exception
   {
      String queue = RandomUtil.randomString();
      String address = RandomUtil.randomString();

      Configuration conf = createBasicConfig();
      conf.setJMXManagementEnabled(false);

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);
      ManagementHelper.putOperationInvocation(message, ResourceNames.CORE_SERVER, "createQueue", queue, address);

      ServerMessage reply = server.getManagementService().handleMessage(message);

      Assert.assertTrue(ManagementHelper.hasOperationSucceeded(reply));

      server.stop();
   }

   @Test
   public void testHandleManagementMessageWithOperationWhichFails() throws Exception
   {
      Configuration conf = createBasicConfig();
      conf.setJMXManagementEnabled(false);

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);
      ManagementHelper.putOperationInvocation(message, ResourceNames.CORE_SERVER, "thereIsNoSuchOperation");

      ServerMessage reply = server.getManagementService().handleMessage(message);

      Assert.assertFalse(ManagementHelper.hasOperationSucceeded(reply));
      Assert.assertNotNull(ManagementHelper.getResult(reply));
      server.stop();
   }

   @Test
   public void testHandleManagementMessageWithUnknowResource() throws Exception
   {
      Configuration conf = createBasicConfig();
      conf.setJMXManagementEnabled(false);

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);
      ManagementHelper.putOperationInvocation(message, "Resouce.Does.Not.Exist", "toString");

      ServerMessage reply = server.getManagementService().handleMessage(message);

      Assert.assertFalse(ManagementHelper.hasOperationSucceeded(reply));
      Assert.assertNotNull(ManagementHelper.getResult(reply));
      server.stop();
   }

   @Test
   public void testHandleManagementMessageWithUnknownAttribute() throws Exception
   {
      Configuration conf = createBasicConfig();
      conf.setJMXManagementEnabled(false);

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);

      ManagementHelper.putAttribute(message, ResourceNames.CORE_SERVER, "started");

      ServerMessage reply = server.getManagementService().handleMessage(message);

      Assert.assertTrue(ManagementHelper.hasOperationSucceeded(reply));
      Assert.assertTrue((Boolean)ManagementHelper.getResult(reply));
      server.stop();
   }

   @Test
   public void testHandleManagementMessageWithKnownAttribute() throws Exception
   {
      Configuration conf = createBasicConfig();
      conf.setJMXManagementEnabled(false);

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);

      ManagementHelper.putAttribute(message, ResourceNames.CORE_SERVER, "attribute.Does.Not.Exist");

      ServerMessage reply = server.getManagementService().handleMessage(message);

      Assert.assertFalse(ManagementHelper.hasOperationSucceeded(reply));
      Assert.assertNotNull(ManagementHelper.getResult(reply));
      server.stop();
   }

   @Test
   public void testGetResources() throws Exception
   {
      Configuration conf = createBasicConfig();
      conf.setJMXManagementEnabled(false);
      ManagementServiceImpl managementService = new ManagementServiceImpl(null, conf);
      managementService.setStorageManager(new NullStorageManager());

      SimpleString address = RandomUtil.randomSimpleString();
      managementService.registerAddress(address);
      Queue queue = new FakeQueue(RandomUtil.randomSimpleString());
      managementService.registerQueue(queue, RandomUtil.randomSimpleString(), new FakeStorageManager());

      Object[] addresses = managementService.getResources(AddressControl.class);
      Assert.assertEquals(1, addresses.length);
      Assert.assertTrue(addresses[0] instanceof AddressControl);
      AddressControl addressControl = (AddressControl)addresses[0];
      Assert.assertEquals(address.toString(), addressControl.getAddress());

      Object[] queues = managementService.getResources(QueueControl.class);
      Assert.assertEquals(1, queues.length);
      Assert.assertTrue(queues[0] instanceof QueueControl);
      QueueControl queueControl = (QueueControl)queues[0];
      Assert.assertEquals(queue.getName().toString(), queueControl.getName());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
