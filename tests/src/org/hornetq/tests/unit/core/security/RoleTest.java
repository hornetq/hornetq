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

package org.hornetq.tests.unit.core.security;

import static org.hornetq.core.security.CheckType.CONSUME;
import static org.hornetq.core.security.CheckType.CREATE_DURABLE_QUEUE;
import static org.hornetq.core.security.CheckType.CREATE_NON_DURABLE_QUEUE;
import static org.hornetq.core.security.CheckType.DELETE_DURABLE_QUEUE;
import static org.hornetq.core.security.CheckType.DELETE_NON_DURABLE_QUEUE;
import static org.hornetq.core.security.CheckType.SEND;

import org.hornetq.core.security.Role;
import org.hornetq.tests.util.UnitTestCase;

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

   
   public void testReadRole() throws Exception
   {
      Role role = new Role("testReadRole", true, false, false, false, false, false, false);
      assertTrue(SEND.hasRole(role));
      assertFalse(CONSUME.hasRole(role));
      assertFalse(CREATE_DURABLE_QUEUE.hasRole(role));
      assertFalse(CREATE_NON_DURABLE_QUEUE.hasRole(role));
      assertFalse(DELETE_DURABLE_QUEUE.hasRole(role));
      assertFalse(DELETE_NON_DURABLE_QUEUE.hasRole(role));
   }
   
   public void testWriteRole() throws Exception
   {
      Role role = new Role("testWriteRole", false, true, false, false, false, false, false);
      assertFalse(SEND.hasRole(role));
      assertTrue(CONSUME.hasRole(role));
      assertFalse(CREATE_DURABLE_QUEUE.hasRole(role));
      assertFalse(CREATE_NON_DURABLE_QUEUE.hasRole(role));
      assertFalse(DELETE_DURABLE_QUEUE.hasRole(role));
      assertFalse(DELETE_NON_DURABLE_QUEUE.hasRole(role));
   }

   public void testCreateRole() throws Exception
   {
      Role role = new Role("testWriteRole", false, false, true, false, false, false, false);
      assertFalse(SEND.hasRole(role));
      assertFalse(CONSUME.hasRole(role));
      assertTrue(CREATE_DURABLE_QUEUE.hasRole(role));
      assertFalse(CREATE_NON_DURABLE_QUEUE.hasRole(role));
      assertFalse(DELETE_DURABLE_QUEUE.hasRole(role));
      assertFalse(DELETE_NON_DURABLE_QUEUE.hasRole(role));
   }
   
   public void testEqualsAndHashcode() throws Exception
   {
      Role role = new Role("testEquals", true, true, true, false, false, false, false);
      Role sameRole = new Role("testEquals", true, true, true, false, false, false, false);
      Role roleWithDifferentName = new Role("notEquals", true, true, true, false, false, false, false);
      Role roleWithDifferentRead = new Role("testEquals", false, true, true, false, false, false, false);
      Role roleWithDifferentWrite = new Role("testEquals", true, false, true, false, false, false, false);
      Role roleWithDifferentCreate = new Role("testEquals", true, true, false, false, false, false, false);

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
