/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.tests.unit.core.security.impl;

import junit.framework.TestCase;
import org.jboss.messaging.core.security.impl.JBMSecurityManagerImpl;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.Role;

import java.util.HashSet;

/**
 * tests JBMSecurityManagerImpl 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JBMSecurityManagerImplTest  extends TestCase
{
   private JBMSecurityManagerImpl securityManager;

   protected void setUp() throws Exception
   {
      securityManager = new JBMSecurityManagerImpl(true);
   }

   protected void tearDown() throws Exception
   {
      securityManager = null;
   }

   public void testDefaultSecurity()
   {
      assertTrue(securityManager.validateUser(null, null));
      assertTrue(securityManager.validateUser("guest", "guest"));
      HashSet<Role> roles = new HashSet<Role>();
      roles.add(new Role("guest", true, true, true));
      assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.CREATE));
      assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.WRITE));
      assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.READ));
      roles = new HashSet<Role>();
      roles.add(new Role("guest", true, true, false));
      assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CREATE));
      assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.WRITE));
      assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.READ));
      roles = new HashSet<Role>();
      roles.add(new Role("guest", true, false, false));
      assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CREATE));
      assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.WRITE));
      assertTrue(securityManager.validateUserAndRole(null, null, roles, CheckType.READ));
      roles = new HashSet<Role>();
      roles.add(new Role("guest", false, false, false));
      assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.CREATE));
      assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.WRITE));
      assertFalse(securityManager.validateUserAndRole(null, null, roles, CheckType.READ));
   }

   public void testAddingUsers()
   {
      securityManager.addUser("newuser1", "newpassword1");
      assertTrue(securityManager.validateUser("newuser1", "newpassword1"));
      assertFalse(securityManager.validateUser("newuser1", "guest"));
      assertFalse(securityManager.validateUser("newuser1", null));
      try
      {
         securityManager.addUser("newuser2", null);
         fail("password cannot be null");
      }
      catch (IllegalArgumentException e)
      {
         //pass
      }
      try
      {
         securityManager.addUser(null, "newpassword2");
         fail("password cannot be null");
      }
      catch (IllegalArgumentException e)
      {
         //pass
      }
   }

   public void testRemovingUsers()
   {
      securityManager.addUser("newuser1", "newpassword1");
      assertTrue(securityManager.validateUser("newuser1", "newpassword1"));
      securityManager.removeUser("newuser1");
      assertFalse(securityManager.validateUser("newuser1", "newpassword1"));
   }

   public void testAddingRoles()
   {
      securityManager.addUser("newuser1", "newpassword1");
      securityManager.addRole("newuser1", "role1");
      securityManager.addRole("newuser1", "role2");
      securityManager.addRole("newuser1", "role3");
      securityManager.addRole("newuser1", "role4");
      HashSet<Role> roles = new HashSet<Role>();
      roles.add(new Role("role1", true, true, true));
      assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.WRITE));
      roles = new HashSet<Role>();
      roles.add(new Role("role2", true, true, true));
      assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.WRITE));
      roles = new HashSet<Role>();
      roles.add(new Role("role3", true, true, true));
      assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.WRITE));
      roles = new HashSet<Role>();
      roles.add(new Role("role4", true, true, true));
      assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.WRITE));
      roles = new HashSet<Role>();
      roles.add(new Role("role5", true, true, true));
      assertFalse(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.WRITE));
   }

   public void testRemovingRoles()
   {
      securityManager.addUser("newuser1", "newpassword1");
      securityManager.addRole("newuser1", "role1");
      securityManager.addRole("newuser1", "role2");
      securityManager.addRole("newuser1", "role3");
      securityManager.addRole("newuser1", "role4");
      securityManager.removeRole("newuser1", "role2");
      securityManager.removeRole("newuser1", "role4");
      HashSet<Role> roles = new HashSet<Role>();
      roles.add(new Role("role1", true, true, true));
      assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.WRITE));
      roles = new HashSet<Role>();
      roles.add(new Role("role2", true, true, true));
      assertFalse(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.WRITE));
      roles = new HashSet<Role>();
      roles.add(new Role("role3", true, true, true));
      assertTrue(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.WRITE));
      roles = new HashSet<Role>();
      roles.add(new Role("role4", true, true, true));
      assertFalse(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.WRITE));
      roles = new HashSet<Role>();
      roles.add(new Role("role5", true, true, true));
      assertFalse(securityManager.validateUserAndRole("newuser1", "newpassword1", roles, CheckType.WRITE));
   }
}
