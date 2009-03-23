/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.management;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.management.RoleInfo;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import org.jboss.messaging.utils.SimpleString;

import javax.management.openmbean.TabularData;
import java.util.HashSet;
import java.util.Set;

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

   private MessagingService service;

   protected ClientSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, false);

      AddressControlMBean addressControl = createManagementControl(address);

      assertEquals(address.toString(), addressControl.getAddress());

      session.deleteQueue(queue);
   }

   public void testGetQueueNames() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString anotherQueue = randomSimpleString();

      session.createQueue(address, queue, true);

      AddressControlMBean addressControl = createManagementControl(address);
      String[] queueNames = addressControl.getQueueNames();
      assertEquals(1, queueNames.length);
      assertEquals(queue.toString(), queueNames[0]);

      session.createQueue(address, anotherQueue, false);
      queueNames = addressControl.getQueueNames();
      assertEquals(2, queueNames.length);

      session.deleteQueue(queue);

      queueNames = addressControl.getQueueNames();
      assertEquals(1, queueNames.length);
      assertEquals(anotherQueue.toString(), queueNames[0]);

      session.deleteQueue(anotherQueue);
   }

   public void testGetRoles() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      Role role = new Role(randomString(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

      session.createQueue(address, queue, true);

      AddressControlMBean addressControl = createManagementControl(address);
      TabularData tabularData = addressControl.getRoles();
      assertEquals(0, tabularData.size());

      Set<Role> newRoles = new HashSet<Role>();
      newRoles.add(role);
      service.getServer().getSecurityRepository().addMatch(address.toString(), newRoles);

      tabularData = addressControl.getRoles();
      assertEquals(1, tabularData.size());
      RoleInfo[] roleInfos = RoleInfo.from(tabularData);
      assertEquals(role.getName(), roleInfos[0].getName());
      assertEquals(CheckType.CONSUME.hasRole(role), roleInfos[0].isConsume());
      assertEquals(CheckType.CREATE_DURABLE_QUEUE.hasRole(role), roleInfos[0].isCreateDurableQueue());
      assertEquals(CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role), roleInfos[0].isCreateNonDurableQueue());
      assertEquals(CheckType.DELETE_DURABLE_QUEUE.hasRole(role), roleInfos[0].isDeleteDurableQueue());
      assertEquals(CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role), roleInfos[0].isDeleteNonDurableQueue());
      assertEquals(CheckType.MANAGE.hasRole(role), roleInfos[0].isManage());
      assertEquals(CheckType.SEND.hasRole(role), roleInfos[0].isSend());

      session.deleteQueue(queue);
   }

   public void testAddRole() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      Role role = new Role(randomString(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

      session.createQueue(address, queue, true);

      AddressControlMBean addressControl = createManagementControl(address);
      TabularData tabularData = addressControl.getRoles();
      assertEquals(0, tabularData.size());

      addressControl.addRole(role.getName(),
                             CheckType.SEND.hasRole(role),
                             CheckType.CONSUME.hasRole(role),
                             CheckType.CREATE_DURABLE_QUEUE.hasRole(role),
                             CheckType.DELETE_DURABLE_QUEUE.hasRole(role),
                             CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role),
                             CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role),
                             CheckType.MANAGE.hasRole(role));

      tabularData = addressControl.getRoles();
      assertEquals(1, tabularData.size());
      RoleInfo[] roleInfos = RoleInfo.from(tabularData);
      assertEquals(role.getName(), roleInfos[0].getName());
      assertEquals(CheckType.CONSUME.hasRole(role), roleInfos[0].isConsume());
      assertEquals(CheckType.CREATE_DURABLE_QUEUE.hasRole(role), roleInfos[0].isCreateDurableQueue());
      assertEquals(CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role), roleInfos[0].isCreateNonDurableQueue());
      assertEquals(CheckType.DELETE_DURABLE_QUEUE.hasRole(role), roleInfos[0].isDeleteDurableQueue());
      assertEquals(CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role), roleInfos[0].isDeleteNonDurableQueue());
      assertEquals(CheckType.MANAGE.hasRole(role), roleInfos[0].isManage());
      assertEquals(CheckType.SEND.hasRole(role), roleInfos[0].isSend());


      session.deleteQueue(queue);
   }

   public void testAddRoleWhichAlreadyExists() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      Role role = new Role(randomString(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

      session.createQueue(address, queue, true);

      AddressControlMBean addressControl = createManagementControl(address);
      TabularData tabularData = addressControl.getRoles();
      assertEquals(0, tabularData.size());

      addressControl.addRole(role.getName(),
                             CheckType.SEND.hasRole(role),
                             CheckType.CONSUME.hasRole(role),
                             CheckType.CREATE_DURABLE_QUEUE.hasRole(role),
                             CheckType.DELETE_DURABLE_QUEUE.hasRole(role),
                             CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role),
                             CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role),
                             CheckType.MANAGE.hasRole(role));

      tabularData = addressControl.getRoles();
      assertEquals(1, tabularData.size());

      try
      {
         addressControl.addRole(role.getName(),
                             CheckType.SEND.hasRole(role),
                             CheckType.CONSUME.hasRole(role),
                             CheckType.CREATE_DURABLE_QUEUE.hasRole(role),
                             CheckType.DELETE_DURABLE_QUEUE.hasRole(role),
                             CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role),
                             CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role),
                             CheckType.MANAGE.hasRole(role));
         fail("can not add a role which already exists");
      }
      catch (Exception e)
      {
      }

      tabularData = addressControl.getRoles();
      assertEquals(1, tabularData.size());

      session.deleteQueue(queue);
   }

   public void testRemoveRole() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      String roleName = randomString();

      session.createQueue(address, queue, true);

      AddressControlMBean addressControl = createManagementControl(address);
      TabularData tabularData = addressControl.getRoles();
      assertEquals(0, tabularData.size());

      addressControl.addRole(roleName, randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

      tabularData = addressControl.getRoles();
      assertEquals(1, tabularData.size());

      addressControl.removeRole(roleName);

      tabularData = addressControl.getRoles();
      assertEquals(0, tabularData.size());

      session.deleteQueue(queue);
   }

   public void testRemoveRoleWhichDoesNotExist() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      String roleName = randomString();

      session.createQueue(address, queue, true);

      AddressControlMBean addressControl = createManagementControl(address);
      TabularData tabularData = addressControl.getRoles();
      assertEquals(0, tabularData.size());

      try
      {
         addressControl.removeRole(roleName);
         fail("can not remove a role which does not exist");
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
      service = Messaging.newNullStorageMessagingService(conf, mbeanServer);
      service.start();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      sf.setBlockOnNonPersistentSend(true);
      sf.setBlockOnNonPersistentSend(true);
      session = sf.createSession(false, true, false);
      session.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();

      service.stop();

      super.tearDown();
   }

   protected AddressControlMBean createManagementControl(SimpleString address) throws Exception
   {
      return ManagementControlHelper.createAddressControl(address, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
