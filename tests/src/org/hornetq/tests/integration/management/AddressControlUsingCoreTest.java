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

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.hornetq.api.SimpleString;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClientSessionFactoryImpl;
import org.hornetq.api.core.config.Configuration;
import org.hornetq.api.core.config.ConfigurationImpl;
import org.hornetq.api.core.config.TransportConfiguration;
import org.hornetq.api.core.management.AddressControl;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.api.core.server.HornetQServers;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.RandomUtil;

/**
 * 
 * A AddressControlUsingCoreTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class AddressControlUsingCoreTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   protected ClientSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetAddress() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, false);

      CoreMessagingProxy proxy = createProxy(address);

      Assert.assertEquals(address.toString(), proxy.retrieveAttributeValue("address"));

      session.deleteQueue(queue);
   }

   public void testGetQueueNames() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString anotherQueue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, true);

      CoreMessagingProxy proxy = createProxy(address);
      Object[] queueNames = (Object[])proxy.retrieveAttributeValue("queueNames");
      Assert.assertEquals(1, queueNames.length);
      Assert.assertEquals(queue.toString(), queueNames[0]);

      session.createQueue(address, anotherQueue, false);
      queueNames = (Object[])proxy.retrieveAttributeValue("queueNames");
      Assert.assertEquals(2, queueNames.length);

      session.deleteQueue(queue);

      queueNames = (Object[])proxy.retrieveAttributeValue("queueNames");
      Assert.assertEquals(1, queueNames.length);
      Assert.assertEquals(anotherQueue.toString(), queueNames[0]);

      session.deleteQueue(anotherQueue);
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

      CoreMessagingProxy proxy = createProxy(address);
      Object[] roles = (Object[])proxy.retrieveAttributeValue("roles");
      for (Object role2 : roles)
      {
         System.out.println(((Object[])role2)[0]);
      }
      Assert.assertEquals(0, roles.length);

      Set<Role> newRoles = new HashSet<Role>();
      newRoles.add(role);
      server.getSecurityRepository().addMatch(address.toString(), newRoles);

      roles = (Object[])proxy.retrieveAttributeValue("roles");
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

   public void testAddRole() throws Exception
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

      CoreMessagingProxy proxy = createProxy(address);
      Object[] roles = (Object[])proxy.retrieveAttributeValue("roles");
      Assert.assertEquals(0, roles.length);

      proxy.invokeOperation("addRole",
                            role.getName(),
                            CheckType.SEND.hasRole(role),
                            CheckType.CONSUME.hasRole(role),
                            CheckType.CREATE_DURABLE_QUEUE.hasRole(role),
                            CheckType.DELETE_DURABLE_QUEUE.hasRole(role),
                            CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role),
                            CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role),
                            CheckType.MANAGE.hasRole(role));

      roles = (Object[])proxy.retrieveAttributeValue("roles");
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

   public void testAddRoleWhichAlreadyExists() throws Exception
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

      CoreMessagingProxy proxy = createProxy(address);
      Object[] data = (Object[])proxy.retrieveAttributeValue("roles");
      Assert.assertEquals(0, data.length);

      proxy.invokeOperation("addRole",
                            role.getName(),
                            CheckType.SEND.hasRole(role),
                            CheckType.CONSUME.hasRole(role),
                            CheckType.CREATE_DURABLE_QUEUE.hasRole(role),
                            CheckType.DELETE_DURABLE_QUEUE.hasRole(role),
                            CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role),
                            CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role),
                            CheckType.MANAGE.hasRole(role));

      data = (Object[])proxy.retrieveAttributeValue("roles");
      Assert.assertEquals(1, data.length);

      try
      {
         proxy.invokeOperation("addRole",
                               role.getName(),
                               CheckType.SEND.hasRole(role),
                               CheckType.CONSUME.hasRole(role),
                               CheckType.CREATE_DURABLE_QUEUE.hasRole(role),
                               CheckType.DELETE_DURABLE_QUEUE.hasRole(role),
                               CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role),
                               CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role),
                               CheckType.MANAGE.hasRole(role));
         Assert.fail("can not add a role which already exists");
      }
      catch (Exception e)
      {
      }

      data = (Object[])proxy.retrieveAttributeValue("roles");
      Assert.assertEquals(1, data.length);

      session.deleteQueue(queue);
   }

   public void testRemoveRole() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      String roleName = RandomUtil.randomString();

      session.createQueue(address, queue, true);

      CoreMessagingProxy proxy = createProxy(address);
      Object[] data = (Object[])proxy.retrieveAttributeValue("roles");
      Assert.assertEquals(0, data.length);

      proxy.invokeOperation("addRole",
                            roleName,
                            RandomUtil.randomBoolean(),
                            RandomUtil.randomBoolean(),
                            RandomUtil.randomBoolean(),
                            RandomUtil.randomBoolean(),
                            RandomUtil.randomBoolean(),
                            RandomUtil.randomBoolean(),
                            RandomUtil.randomBoolean());

      data = (Object[])proxy.retrieveAttributeValue("roles");
      Assert.assertEquals(1, data.length);

      proxy.invokeOperation("removeRole", roleName);

      data = (Object[])proxy.retrieveAttributeValue("roles");
      Assert.assertEquals(0, data.length);

      session.deleteQueue(queue);
   }

   public void testRemoveRoleWhichDoesNotExist() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      String roleName = RandomUtil.randomString();

      session.createQueue(address, queue, true);

      CoreMessagingProxy proxy = createProxy(address);
      Object[] data = (Object[])proxy.retrieveAttributeValue("roles");
      Assert.assertEquals(0, data.length);

      try
      {
         proxy.invokeOperation("removeRole", roleName);
         Assert.fail("can not remove a role which does not exist");
      }
      catch (Exception e)
      {
      }

      session.deleteQueue(queue);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      server = HornetQServers.newHornetQServer(conf, mbeanServer, false);
      server.start();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      sf.setBlockOnNonDurableSend(true);
      sf.setBlockOnNonDurableSend(true);
      session = sf.createSession(false, true, false);
      session.start();
   }

   protected CoreMessagingProxy createProxy(final SimpleString address) throws Exception
   {
      CoreMessagingProxy proxy = new CoreMessagingProxy(session, ResourceNames.CORE_ADDRESS + address);

      return proxy;
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();

      server.stop();

      session = null;

      server = null;

      super.tearDown();
   }

   protected AddressControl createManagementControl(final SimpleString address) throws Exception
   {
      return ManagementControlHelper.createAddressControl(address, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
