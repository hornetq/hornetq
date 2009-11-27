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

import static org.hornetq.tests.util.RandomUtil.randomSimpleString;
import static org.hornetq.tests.util.RandomUtil.randomString;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.buffers.HornetQBuffers;
import org.hornetq.core.client.management.impl.ManagementHelper;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.AddressControl;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.management.impl.ManagementServiceImpl;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.tests.integration.server.FakeStorageManager;
import org.hornetq.tests.unit.core.postoffice.impl.FakeQueue;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/*
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ManagementServiceImplTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   private final Logger log = Logger.getLogger(ManagementServiceImplTest.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testHandleManagementMessageWithOperation() throws Exception
   {
      String queue = randomString();
      String address = randomString();
      
      Configuration conf = new ConfigurationImpl();
      conf.setJMXManagementEnabled(false);
      
      HornetQServer server = HornetQ.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);
      ManagementHelper.putOperationInvocation(message,
                                              ResourceNames.CORE_SERVER,
                                              "createQueue",
                                              queue,
                                              address);
      
      ServerMessage reply = server.getManagementService().handleMessage(message);
      
      assertTrue(ManagementHelper.hasOperationSucceeded(reply));

      server.stop();
   }

   public void testHandleManagementMessageWithOperationWhichFails() throws Exception
   {
      Configuration conf = new ConfigurationImpl();
      conf.setJMXManagementEnabled(false);
      
      HornetQServer server = HornetQ.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);
      ManagementHelper.putOperationInvocation(message,
                                              ResourceNames.CORE_SERVER,
                                              "thereIsNoSuchOperation");
      
      ServerMessage reply = server.getManagementService().handleMessage(message);

      
      assertFalse(ManagementHelper.hasOperationSucceeded(reply));
      assertNotNull(ManagementHelper.getResult(reply));
      server.stop();
   }
   
   public void testHandleManagementMessageWithUnknowResource() throws Exception
   {
      Configuration conf = new ConfigurationImpl();
      conf.setJMXManagementEnabled(false);
      
      HornetQServer server = HornetQ.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);
      ManagementHelper.putOperationInvocation(message,
                                              "Resouce.Does.Not.Exist",
                                              "toString");
      
      ServerMessage reply = server.getManagementService().handleMessage(message);

      
      assertFalse(ManagementHelper.hasOperationSucceeded(reply));
      assertNotNull(ManagementHelper.getResult(reply));
      server.stop();
   }

   public void testHandleManagementMessageWithUnknowAttribute() throws Exception
   {
      Configuration conf = new ConfigurationImpl();
      conf.setJMXManagementEnabled(false);
      
      HornetQServer server = HornetQ.newHornetQServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl(1, 100);

      ManagementHelper.putAttribute(message, ResourceNames.CORE_SERVER, "attribute.Does.Not.Exist");
      
      ServerMessage reply = server.getManagementService().handleMessage(message);

      
      assertFalse(ManagementHelper.hasOperationSucceeded(reply));
      assertNotNull(ManagementHelper.getResult(reply));
      server.stop();
   }
   
   public void testGetResources() throws Exception
   {
      Configuration conf  = new ConfigurationImpl();
      conf.setJMXManagementEnabled(false);
      ManagementServiceImpl managementService = new ManagementServiceImpl(null, conf);
      
      SimpleString address = randomSimpleString();
      managementService.registerAddress(address);
      Queue queue = new FakeQueue(randomSimpleString());
      managementService.registerQueue(queue, randomSimpleString(), new FakeStorageManager());

      Object[] addresses = managementService.getResources(AddressControl.class);
      assertEquals(1, addresses.length);
      assertTrue(addresses[0] instanceof AddressControl);
      AddressControl addressControl = (AddressControl)addresses[0];
      assertEquals(address.toString(), addressControl.getAddress());

      Object[] queues = managementService.getResources(QueueControl.class);
      assertEquals(1, queues.length);
      assertTrue(queues[0] instanceof QueueControl);
      QueueControl queueControl = (QueueControl)queues[0];
      assertEquals(queue.getName().toString(), queueControl.getName());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
