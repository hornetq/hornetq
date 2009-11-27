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

import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomSimpleString;
import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.HashSet;
import java.util.Set;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.management.AddressControl;
import org.hornetq.core.management.RoleInfo;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.utils.SimpleString;

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

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, false);

      AddressControl addressControl = createManagementControl(address);

      assertEquals(address.toString(), addressControl.getAddress());

      session.deleteQueue(queue);
   }

   public void testGetQueueNames() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      SimpleString anotherQueue = randomSimpleString();

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
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
      Role role = new Role(randomString(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean());

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
      Object[] roles = addressControl.getRoles();
      assertEquals(0, roles.length);

      Set<Role> newRoles = new HashSet<Role>();
      newRoles.add(role);
      server.getSecurityRepository().addMatch(address.toString(), newRoles);

      roles = addressControl.getRoles();
      assertEquals(1, roles.length);
      Object[] r = (Object[])roles[0];
      assertEquals(role.getName(), r[0]);
      assertEquals(CheckType.SEND.hasRole(role), r[1]);
      assertEquals(CheckType.CONSUME.hasRole(role), r[2]);
      assertEquals(CheckType.CREATE_DURABLE_QUEUE.hasRole(role), r[3]);
      assertEquals(CheckType.DELETE_DURABLE_QUEUE.hasRole(role), r[4]);
      assertEquals(CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role), r[5]);
      assertEquals(CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role), r[6]);
      assertEquals(CheckType.MANAGE.hasRole(role), r[7]);

      session.deleteQueue(queue);
   }

   public void testGetRolesAsJSON() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      Role role = new Role(randomString(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean());

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
      String jsonString = addressControl.getRolesAsJSON();
      assertNotNull(jsonString);     
      RoleInfo[] roles = RoleInfo.from(jsonString);
      assertEquals(0, roles.length);

      Set<Role> newRoles = new HashSet<Role>();
      newRoles.add(role);
      server.getSecurityRepository().addMatch(address.toString(), newRoles);

      jsonString = addressControl.getRolesAsJSON();
      assertNotNull(jsonString);     
      roles = RoleInfo.from(jsonString);
      assertEquals(1, roles.length);
      RoleInfo r = roles[0];
      assertEquals(role.getName(), roles[0].getName());
      assertEquals(role.isSend(), r.isSend());
      assertEquals(role.isConsume(), r.isConsume());
      assertEquals(role.isCreateDurableQueue(), r.isCreateDurableQueue());
      assertEquals(role.isDeleteDurableQueue(), r.isDeleteDurableQueue());
      assertEquals(role.isCreateNonDurableQueue(), r.isCreateNonDurableQueue());
      assertEquals(role.isDeleteNonDurableQueue(), r.isDeleteNonDurableQueue());
      assertEquals(role.isManage(), r.isManage());

      session.deleteQueue(queue);
   }
   
   public void testAddRole() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      Role role = new Role(randomString(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean());

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
      Object[] roles = addressControl.getRoles();
      assertEquals(0, roles.length);

      addressControl.addRole(role.getName(),
                             CheckType.SEND.hasRole(role),
                             CheckType.CONSUME.hasRole(role),
                             CheckType.CREATE_DURABLE_QUEUE.hasRole(role),
                             CheckType.DELETE_DURABLE_QUEUE.hasRole(role),
                             CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role),
                             CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role),
                             CheckType.MANAGE.hasRole(role));

      roles = addressControl.getRoles();
      assertEquals(1, roles.length);
      Object[] r = (Object[])roles[0];
      assertEquals(role.getName(), r[0]);
      assertEquals(CheckType.SEND.hasRole(role), r[1]);
      assertEquals(CheckType.CONSUME.hasRole(role), r[2]);
      assertEquals(CheckType.CREATE_DURABLE_QUEUE.hasRole(role), r[3]);
      assertEquals(CheckType.DELETE_DURABLE_QUEUE.hasRole(role), r[4]);
      assertEquals(CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role), r[5]);
      assertEquals(CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role), r[6]);
      assertEquals(CheckType.MANAGE.hasRole(role), r[7]);

      session.deleteQueue(queue);
   }

   public void testAddRoleWhichAlreadyExists() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      Role role = new Role(randomString(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean(),
                           randomBoolean());

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
      Object[] tabularData = addressControl.getRoles();
      assertEquals(0, tabularData.length);

      addressControl.addRole(role.getName(),
                             CheckType.SEND.hasRole(role),
                             CheckType.CONSUME.hasRole(role),
                             CheckType.CREATE_DURABLE_QUEUE.hasRole(role),
                             CheckType.DELETE_DURABLE_QUEUE.hasRole(role),
                             CheckType.CREATE_NON_DURABLE_QUEUE.hasRole(role),
                             CheckType.DELETE_NON_DURABLE_QUEUE.hasRole(role),
                             CheckType.MANAGE.hasRole(role));

      tabularData = addressControl.getRoles();
      assertEquals(1, tabularData.length);

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
      assertEquals(1, tabularData.length);

      session.deleteQueue(queue);
   }

   public void testRemoveRole() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      String roleName = randomString();

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
      Object[] tabularData = addressControl.getRoles();
      assertEquals(0, tabularData.length);

      addressControl.addRole(roleName,
                             randomBoolean(),
                             randomBoolean(),
                             randomBoolean(),
                             randomBoolean(),
                             randomBoolean(),
                             randomBoolean(),
                             randomBoolean());

      tabularData = addressControl.getRoles();
      assertEquals(1, tabularData.length);

      addressControl.removeRole(roleName);

      tabularData = addressControl.getRoles();
      assertEquals(0, tabularData.length);

      session.deleteQueue(queue);
   }

   public void testRemoveRoleWhichDoesNotExist() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      String roleName = randomString();

      session.createQueue(address, queue, true);

      AddressControl addressControl = createManagementControl(address);
      Object[] tabularData = addressControl.getRoles();
      assertEquals(0, tabularData.length);

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
   
   public void testGetNumberOfPages() throws Exception
   {
      session.close();
      server.stop();
      
      SimpleString address = randomSimpleString();
      
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setPageSizeBytes(1024);
      addressSettings.setMaxSizeBytes(10 * 1024);    
      int NUMBER_MESSAGES_BEFORE_PAGING = 14;
      
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);
      server.start();  
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));      
      session = sf.createSession(false, true, false);
      session.start();      
      session.createQueue(address, address, true);
      
      ClientProducer producer = session.createProducer(address);
      
      for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++)
      {
         ClientMessage msg = session.createClientMessage(true);
         msg.getBodyBuffer().writeBytes(new byte[512]);
         producer.send(msg);
      }
      session.commit();
      
      AddressControl addressControl = createManagementControl(address);
      assertEquals(0, addressControl.getNumberOfPages());
      
      ClientMessage msg = session.createClientMessage(true);
      msg.getBodyBuffer().writeBytes(new byte[512]);
      producer.send(msg);

      session.commit();
      assertEquals(1, addressControl.getNumberOfPages());
      
      msg = session.createClientMessage(true);
      msg.getBodyBuffer().writeBytes(new byte[512]);
      producer.send(msg);

      session.commit();
      assertEquals(1, addressControl.getNumberOfPages());
      
      msg = session.createClientMessage(true);
      msg.getBodyBuffer().writeBytes(new byte[512]);
      producer.send(msg);

      session.commit();
      assertEquals(2, addressControl.getNumberOfPages());
   }
   
   public void testGetNumberOfBytesPerPage() throws Exception
   {
      SimpleString address = randomSimpleString();
      session.createQueue(address, address, true);      
      
      AddressControl addressControl = createManagementControl(address);      
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE, addressControl.getNumberOfBytesPerPage());
      
      session.close();
      server.stop();     
      
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setPageSizeBytes(1024);
      
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);
      server.start();
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, false);
      session.createQueue(address, address, true);  
      assertEquals(1024, addressControl.getNumberOfBytesPerPage());
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
      server = HornetQ.newHornetQServer(conf, mbeanServer, false);
      server.start();

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

      server.stop();
      
      server = null;
      
      session = null;
      

      super.tearDown();
   }

   protected AddressControl createManagementControl(SimpleString address) throws Exception
   {
      return ManagementControlHelper.createAddressControl(address, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
