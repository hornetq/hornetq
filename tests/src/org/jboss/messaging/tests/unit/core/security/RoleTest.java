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

package org.jboss.messaging.tests.unit.core.security;

import static org.jboss.messaging.core.security.CheckType.CREATE;
import static org.jboss.messaging.core.security.CheckType.READ;
import static org.jboss.messaging.core.security.CheckType.WRITE;

import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class RoleTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testDefaultRole() throws Exception
   {
      Role role = new Role("testDefaultRole");
      assertEquals("testDefaultRole", role.getName());
      assertFalse(role.isCheckType(READ));
      assertFalse(role.isCheckType(WRITE));
      assertFalse(role.isCheckType(CREATE));      
   }
   
   public void testReadRole() throws Exception
   {
      Role role = new Role("testReadRole", true, false, false);
      assertTrue(role.isCheckType(READ));
      assertFalse(role.isCheckType(WRITE));
      assertFalse(role.isCheckType(CREATE));      
   }
   
   public void testWriteRole() throws Exception
   {
      Role role = new Role("testWriteRole", false, true, false);
      assertFalse(role.isCheckType(READ));
      assertTrue(role.isCheckType(WRITE));
      assertFalse(role.isCheckType(CREATE));      
   }

   public void testCreateRole() throws Exception
   {
      Role role = new Role("testWriteRole", false, false, true);
      assertFalse(role.isCheckType(READ));
      assertFalse(role.isCheckType(WRITE));
      assertTrue(role.isCheckType(CREATE));      
   }
   
   public void testEqualsAndHashcode() throws Exception
   {
      Role role = new Role("testEquals", true, true, true);
      Role sameRole = new Role("testEquals", true, true, true);
      Role roleWithDifferentName = new Role("notEquals", true, true, true);
      Role roleWithDifferentRead = new Role("testEquals", false, true, true);
      Role roleWithDifferentWrite = new Role("testEquals", true, false, true);
      Role roleWithDifferentCreate = new Role("testEquals", true, true, false);

      assertTrue(role.equals(role));

      assertTrue(role.equals(sameRole));
      assertTrue(role.hashCode() == sameRole.hashCode());
      
      assertFalse(role.equals(roleWithDifferentName));
      assertFalse(role.hashCode() == roleWithDifferentName.hashCode());
      
      assertFalse(role.equals(roleWithDifferentRead));
      assertFalse(role.hashCode() == roleWithDifferentRead.hashCode());
      
      assertFalse(role.equals(roleWithDifferentWrite));
      assertFalse(role.hashCode() == roleWithDifferentWrite.hashCode());
      
      assertFalse(role.equals(roleWithDifferentCreate));
      assertFalse(role.hashCode() == roleWithDifferentCreate.hashCode());
      
      assertFalse(role.equals(null));
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
