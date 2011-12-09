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

import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.management.AddressControl;
import org.hornetq.api.core.management.RoleInfo;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A QueueControlTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class AddressControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   protected ClientSession session;
   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetAddress() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, false);

      AddressControl addressControl = createManagementControl(address);

      Assert.assertEquals(address.toString(), addressControl.getAddress());

      session.deleteQueue(queue);
   }

   public void testGetQueueNames() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString anotherQueue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
      String[] queueNames = addressControl.getQueueNames();
      Assert.assertEquals(1, queueNames.length);
      Assert.assertEquals(queue.toString(), queueNames[0]);

      session.createQueue(address, anotherQueue, false);
      queueNames = addressControl.getQueueNames();
      Assert.assertEquals(2, queueNames.length);

      session.deleteQueue(queue);

      queueNames = addressControl.getQueueNames();
      Assert.assertEquals(1, queueNames.length);
      Assert.assertEquals(anotherQueue.toString(), queueNames[0]);

      session.deleteQueue(anotherQueue);
   }
   
   public void testGetBindingNames() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      String divertName = RandomUtil.randomString();
      
      session.createQueue(address, queue, false);

      AddressControl addressControl = createManagementControl(address);
      String[] bindingNames = addressControl.getBindingNames();
      assertEquals(1, bindingNames.length);
      assertEquals(queue.toString(), bindingNames[0]);

      server.getHornetQServerControl().createDivert(divertName, randomString(), address.toString(), RandomUtil.randomString(), false, null, null);

      bindingNames = addressControl.getBindingNames();
      Assert.assertEquals(2, bindingNames.length);
      
      session.deleteQueue(queue);
      
      bindingNames = addressControl.getBindingNames();
      assertEquals(1, bindingNames.length);
      assertEquals(divertName.toString(), bindingNames[0]);
   }

   public void testGetRoles() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      Role role = new Role(RandomUtil.randomString(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean());

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
      Object[] roles = addressControl.getRoles();
      Assert.assertEquals(0, roles.length);

      Set<Role> newRoles = new HashSet<Role>();
      newRoles.add(role);
      server.getSecurityRepository().addMatch(address.toString(), newRoles);

      roles = addressControl.getRoles();
      Assert.assertEquals(1, roles.length);
      Object[] r = (Object[])roles[0];
      Assert.assertEquals(role.getName(), r[0]);
      Assert.assertEquals(CheckType.SEND.hasRole(role), r[1]);
      Assert.assertEquals(CheckType.CONSUME.hasRole(role), r[2]);
      Assert.assertEquals(CheckType.CREATE_DURABLE_QUEUE.hasRole(role), r[3]);
      Assert.assertEquals(CheckType.DELETE_DURABLE_QUEUE.hasRole(role), r[4]);
      Assert.assertEquals(CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role), r[5]);
      Assert.assertEquals(CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role), r[6]);
      Assert.assertEquals(CheckType.MANAGE.hasRole(role), r[7]);

      session.deleteQueue(queue);
   }

   public void testGetRolesAsJSON() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      Role role = new Role(RandomUtil.randomString(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean(),
                           RandomUtil.randomBoolean());

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
      String jsonString = addressControl.getRolesAsJSON();
      Assert.assertNotNull(jsonString);
      RoleInfo[] roles = RoleInfo.from(jsonString);
      Assert.assertEquals(0, roles.length);

      Set<Role> newRoles = new HashSet<Role>();
      newRoles.add(role);
      server.getSecurityRepository().addMatch(address.toString(), newRoles);

      jsonString = addressControl.getRolesAsJSON();
      Assert.assertNotNull(jsonString);
      roles = RoleInfo.from(jsonString);
      Assert.assertEquals(1, roles.length);
      RoleInfo r = roles[0];
      Assert.assertEquals(role.getName(), roles[0].getName());
      Assert.assertEquals(role.isSend(), r.isSend());
      Assert.assertEquals(role.isConsume(), r.isConsume());
      Assert.assertEquals(role.isCreateDurableQueue(), r.isCreateDurableQueue());
      Assert.assertEquals(role.isDeleteDurableQueue(), r.isDeleteDurableQueue());
      Assert.assertEquals(role.isCreateNonDurableQueue(), r.isCreateNonDurableQueue());
      Assert.assertEquals(role.isDeleteNonDurableQueue(), r.isDeleteNonDurableQueue());
      Assert.assertEquals(role.isManage(), r.isManage());

      session.deleteQueue(queue);
   }

   public void testGetNumberOfPages() throws Exception
   {
      session.close();
      server.stop();

      SimpleString address = RandomUtil.randomSimpleString();

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setPageSizeBytes(1024);
      addressSettings.setMaxSizeBytes(10 * 1024);
      int NUMBER_MESSAGES_BEFORE_PAGING = 5;

      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);
      server.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory sf = locator.createSessionFactory();
      session = sf.createSession(false, true, false);
      session.start();
      session.createQueue(address, address, true);

      ClientProducer producer = session.createProducer(address);

      for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(new byte[512]);
         producer.send(msg);
      }
      session.commit();

      AddressControl addressControl = createManagementControl(address);
      Assert.assertEquals(0, addressControl.getNumberOfPages());

      ClientMessage msg = session.createMessage(true);
      msg.getBodyBuffer().writeBytes(new byte[512]);
      producer.send(msg);

      session.commit();
      Assert.assertEquals(1, addressControl.getNumberOfPages());

      msg = session.createMessage(true);
      msg.getBodyBuffer().writeBytes(new byte[512]);
      producer.send(msg);

      session.commit();
      Assert.assertEquals(1, addressControl.getNumberOfPages());

      msg = session.createMessage(true);
      msg.getBodyBuffer().writeBytes(new byte[512]);
      producer.send(msg);

      session.commit();
      Assert.assertEquals(2, addressControl.getNumberOfPages());
   }

   public void testGetNumberOfBytesPerPage() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      session.createQueue(address, address, true);

      AddressControl addressControl = createManagementControl(address);
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE, addressControl.getNumberOfBytesPerPage());

      session.close();
      server.stop();

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setPageSizeBytes(1024);

      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);
      server.start();
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      ClientSessionFactory sf = locator.createSessionFactory();
      session = sf.createSession(false, true, false);
      session.createQueue(address, address, true);
      Assert.assertEquals(1024, addressControl.getNumberOfBytesPerPage());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      server = HornetQServers.newHornetQServer(conf, mbeanServer, false);
      server.start();

      locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnNonDurableSend(true);
      ClientSessionFactory sf = locator.createSessionFactory();
      session = sf.createSession(false, true, false);
      session.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();

      locator.close();

      server.stop();

      server = null;

      session = null;

      super.tearDown();
   }

   protected AddressControl createManagementControl(final SimpleString address) throws Exception
   {
      return ManagementControlHelper.createAddressControl(address, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
