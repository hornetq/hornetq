/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.core.management.impl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import junit.framework.TestCase;

import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.management.RoleInfo;
import org.jboss.messaging.core.management.impl.AddressControl;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class AddressControlTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static void assertRoleEquals(Role expected, RoleInfo actual)
   {
      assertEquals(expected.getName(), actual.getName());
      assertEquals(expected.isCheckType(CheckType.CREATE), actual.isCreate());
      assertEquals(expected.isCheckType(CheckType.READ), actual.isRead());
      assertEquals(expected.isCheckType(CheckType.WRITE), actual.isWrite());
   }

   private static void assertRoleEquals(Role expected, CompositeData actual)
   {
      assertTrue(actual.getCompositeType().equals(RoleInfo.TYPE));

      assertEquals(expected.getName(), actual.get("name"));
      assertEquals(expected.isCheckType(CheckType.CREATE), actual.get("create"));
      assertEquals(expected.isCheckType(CheckType.READ), actual.get("read"));
      assertEquals(expected.isCheckType(CheckType.WRITE), actual.get("write"));
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetAddress() throws Exception
   {
      SimpleString address = randomSimpleString();
      MessagingServerManagement server = createMock(MessagingServerManagement.class);

      replay(server);

      AddressControl control = new AddressControl(address, server);
      assertEquals(address.toString(), control.getAddress());

      verify(server);
   }

   public void testGetQueueNames() throws Exception
   {
      SimpleString address = randomSimpleString();
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      List<Queue> queues = new ArrayList<Queue>();
      Queue queue_1 = createMock(Queue.class);
      expect(queue_1.getName()).andStubReturn(randomSimpleString());
      Queue queue_2 = createMock(Queue.class);
      expect(queue_2.getName()).andStubReturn(randomSimpleString());
      queues.add(queue_1);
      queues.add(queue_2);
      expect(server.getQueuesForAddress(address)).andReturn(queues);

      replay(server, queue_1, queue_2);

      AddressControl control = new AddressControl(address, server);
      String[] queueNames = control.getQueueNames();
      assertEquals(2, queueNames.length);
      assertEquals(queue_1.getName().toString(), queueNames[0]);
      assertEquals(queue_2.getName().toString(), queueNames[1]);

      verify(server, queue_1, queue_2);
   }

   public void testGetRoleInfos() throws Exception
   {
      SimpleString address = randomSimpleString();
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Set<Role> roles = new HashSet<Role>();
      Role role_1 = new Role(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());
      Role role_2 = new Role(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());
      roles.add(role_1);
      roles.add(role_2);
      expect(server.getSecurityForAddress(address)).andReturn(roles);

      replay(server);

      AddressControl control = new AddressControl(address, server);
      RoleInfo[] infos = control.getRoleInfos();
      assertEquals(2, infos.length);
      if (infos[0].getName().equals(role_1.getName()))
      {
         assertRoleEquals(role_1, infos[0]);
         assertRoleEquals(role_2, infos[1]);
      } else
      {
         assertRoleEquals(role_2, infos[0]);
         assertRoleEquals(role_1, infos[1]);
      }

      verify(server);
   }

   public void testGetRoles() throws Exception
   {
      SimpleString address = randomSimpleString();
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Set<Role> roles = new HashSet<Role>();
      Role role_1 = new Role(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());
      Role role_2 = new Role(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());
      roles.add(role_1);
      roles.add(role_2);
      expect(server.getSecurityForAddress(address)).andReturn(roles);

      replay(server);

      AddressControl control = new AddressControl(address, server);
      TabularData data = control.getRoles();
      assertEquals(2, data.size());
      CompositeData roleData_1 = data.get(new Object[] { role_1.getName() });
      CompositeData roleData_2 = data.get(new Object[] { role_2.getName() });
      assertRoleEquals(role_1, roleData_1);
      assertRoleEquals(role_2, roleData_2);

      verify(server);
   }

   public void testAddRole() throws Exception
   {
      SimpleString address = randomSimpleString();
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      expect(server.getSecurityForAddress(address)).andReturn(
            new HashSet<Role>());
      server.setSecurityForAddress(eq(address), isA(Set.class));
      replay(server);

      AddressControl control = new AddressControl(address, server);
      control.addRole(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());

      verify(server);
   }

   public void testAddRoleWhichAlreadExists() throws Exception
   {
      SimpleString address = randomSimpleString();
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Set<Role> roles = new HashSet<Role>();
      Role role = new Role(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());
      roles.add(role);
      expect(server.getSecurityForAddress(address)).andReturn(roles);

      replay(server);

      AddressControl control = new AddressControl(address, server);
      try
      {
         control.addRole(role.getName(), role.isCheckType(CheckType.CREATE),
               role.isCheckType(CheckType.READ), role
                     .isCheckType(CheckType.WRITE));
         fail("role already exists");
      } catch (IllegalArgumentException e)
      {
      }

      verify(server);
   }

   public void testRemoveRole() throws Exception
   {
      SimpleString address = randomSimpleString();
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      Set<Role> roles = new HashSet<Role>();
      Role role = new Role(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());
      roles.add(role);
      expect(server.getSecurityForAddress(address)).andReturn(roles);
      server.setSecurityForAddress(eq(address), isA(Set.class));
      replay(server);

      AddressControl control = new AddressControl(address, server);
      control.removeRole(role.getName());

      verify(server);
   }

   public void testRemoveRoleFromEmptySet() throws Exception
   {
      SimpleString address = randomSimpleString();
      MessagingServerManagement server = createMock(MessagingServerManagement.class);
      expect(server.getSecurityForAddress(address)).andReturn(
            new HashSet<Role>());
      replay(server);

      AddressControl control = new AddressControl(address, server);
      try
      {
         control.removeRole(randomString());
         fail("role does not exisits");
      } catch (IllegalArgumentException e)
      {
      }

      verify(server);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
