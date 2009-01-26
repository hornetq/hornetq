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

import java.util.HashSet;
import java.util.Set;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import junit.framework.TestCase;

import org.jboss.messaging.core.management.RoleInfo;
import org.jboss.messaging.core.management.impl.AddressControl;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.settings.HierarchicalRepository;
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
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<Set<Role>> securityRepository = createMock(HierarchicalRepository.class);
      replay(postOffice, securityRepository);

      AddressControl control = new AddressControl(address, postOffice,
            securityRepository);
      assertEquals(address.toString(), control.getAddress());

      verify(postOffice, securityRepository);
   }

//   public void testGetQueueNames() throws Exception
//   {
//      SimpleString address = randomSimpleString();
//      PostOffice postOffice = createMock(PostOffice.class);
//      HierarchicalRepository<Set<Role>> securityRepository = createMock(HierarchicalRepository.class);
//      Bindings bindings = new BindingsImpl();
//      Queue queue_1 = createMock(Queue.class);
//      expect(queue_1.getName()).andStubReturn(randomSimpleString());
//      Binding binding_1 = createMock(Binding.class);
//      
//      expect(binding_1.getQueue()).andReturn(queue_1);      
//      Queue queue_2 = createMock(Queue.class);
//      expect(queue_2.getName()).andStubReturn(randomSimpleString());
//      expect(binding_1.isExclusive()).andStubReturn(false);
//      Binding binding_2 = createMock(Binding.class);
//      
//      expect(binding_2.getQueue()).andReturn(queue_2);      
//      bindings.addBinding(binding_1);
//      bindings.addBinding(binding_2);
//      expect(postOffice.getBindingsForAddress(address)).andReturn(bindings);
//      expect(binding_2.isExclusive()).andStubReturn(false);
//
//      replay(binding_1, queue_1, binding_2, queue_2);
//      replay(postOffice, securityRepository);
//
//      AddressControl control = new AddressControl(address, postOffice,
//            securityRepository);
//      String[] queueNames = control.getQueueNames();
//      assertEquals(2, queueNames.length);
//      assertEquals(queue_1.getName().toString(), queueNames[0]);
//      assertEquals(queue_2.getName().toString(), queueNames[1]);
//
//      verify(binding_1, queue_1, binding_2, queue_2);
//      verify(postOffice, securityRepository);
//   }

   public void testGetRoleInfos() throws Exception
   {
      SimpleString address = randomSimpleString();
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<Set<Role>> securityRepository = createMock(HierarchicalRepository.class);

      Set<Role> roles = new HashSet<Role>();
      Role role_1 = new Role(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());
      Role role_2 = new Role(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());
      roles.add(role_1);
      roles.add(role_2);
      expect(securityRepository.getMatch(address.toString())).andReturn(roles);

      replay(postOffice, securityRepository);

      AddressControl control = new AddressControl(address, postOffice,
            securityRepository);
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

      verify(postOffice, securityRepository);
   }

   public void testGetRoles() throws Exception
   {
      SimpleString address = randomSimpleString();
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<Set<Role>> securityRepository = createMock(HierarchicalRepository.class);

      Set<Role> roles = new HashSet<Role>();
      Role role_1 = new Role(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());
      Role role_2 = new Role(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());
      roles.add(role_1);
      roles.add(role_2);
      expect(securityRepository.getMatch(address.toString())).andReturn(roles);

      replay(postOffice, securityRepository);

      AddressControl control = new AddressControl(address, postOffice,
            securityRepository);
      TabularData data = control.getRoles();
      assertEquals(2, data.size());
      CompositeData roleData_1 = data.get(new Object[] { role_1.getName() });
      CompositeData roleData_2 = data.get(new Object[] { role_2.getName() });
      assertRoleEquals(role_1, roleData_1);
      assertRoleEquals(role_2, roleData_2);

      verify(postOffice, securityRepository);
   }

   public void testAddRole() throws Exception
   {
      SimpleString address = randomSimpleString();
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<Set<Role>> securityRepository = createMock(HierarchicalRepository.class);

      expect(securityRepository.getMatch(address.toString())).andReturn(
            new HashSet<Role>());
      securityRepository.addMatch(eq(address.toString()), isA(Set.class));
      replay(postOffice, securityRepository);

      AddressControl control = new AddressControl(address, postOffice,
            securityRepository);
      control.addRole(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());

      verify(postOffice, securityRepository);
   }

   public void testAddRoleWhichAlreadExists() throws Exception
   {
      SimpleString address = randomSimpleString();
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<Set<Role>> securityRepository = createMock(HierarchicalRepository.class);

      Set<Role> roles = new HashSet<Role>();
      Role role = new Role(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());
      roles.add(role);
      expect(securityRepository.getMatch(address.toString())).andReturn(roles);

      replay(postOffice, securityRepository);

      AddressControl control = new AddressControl(address, postOffice,
            securityRepository);
      try
      {
         control.addRole(role.getName(), role.isCheckType(CheckType.CREATE),
               role.isCheckType(CheckType.READ), role
                     .isCheckType(CheckType.WRITE));
         fail("role already exists");
      } catch (IllegalArgumentException e)
      {
      }

      verify(postOffice, securityRepository);
   }

   public void testRemoveRole() throws Exception
   {
      SimpleString address = randomSimpleString();
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<Set<Role>> securityRepository = createMock(HierarchicalRepository.class);

      Set<Role> roles = new HashSet<Role>();
      Role role = new Role(randomString(), randomBoolean(), randomBoolean(),
            randomBoolean());
      roles.add(role);
      expect(securityRepository.getMatch(address.toString())).andReturn(roles);
      securityRepository.addMatch(eq(address.toString()), isA(Set.class));
      replay(postOffice, securityRepository);

      AddressControl control = new AddressControl(address, postOffice,
            securityRepository);
      control.removeRole(role.getName());

      verify(postOffice, securityRepository);
   }

   public void testRemoveRoleFromEmptySet() throws Exception
   {
      SimpleString address = randomSimpleString();
      PostOffice postOffice = createMock(PostOffice.class);
      HierarchicalRepository<Set<Role>> securityRepository = createMock(HierarchicalRepository.class);

      expect(securityRepository.getMatch(address.toString())).andReturn(
            new HashSet<Role>());
      replay(postOffice, securityRepository);

      AddressControl control = new AddressControl(address, postOffice,
            securityRepository);
      try
      {
         control.removeRole(randomString());
         fail("role does not exisits");
      } catch (IllegalArgumentException e)
      {
      }

      verify(postOffice, securityRepository);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
