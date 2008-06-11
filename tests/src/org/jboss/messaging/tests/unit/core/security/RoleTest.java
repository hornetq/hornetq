/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.security;

import static org.jboss.messaging.core.security.CheckType.CREATE;
import static org.jboss.messaging.core.security.CheckType.READ;
import static org.jboss.messaging.core.security.CheckType.WRITE;
import junit.framework.TestCase;

import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.Role;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class RoleTest extends TestCase
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
