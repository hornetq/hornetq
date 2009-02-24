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

package org.jboss.messaging.tests.unit.util;

import java.util.HashSet;
import java.util.Set;

import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.UUIDGenerator;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class UUIDTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testManyUUIDs() throws Exception
   {
      final int MANY_TIMES = getTimes();
      Set<String> uuidsSet = new HashSet<String>();
      
      UUIDGenerator gen = UUIDGenerator.getInstance();
      for (int i = 0; i < MANY_TIMES; i++)
      {
         uuidsSet.add(gen.generateStringUUID());
      }
      
      // we put them in a set to check duplicates
      assertEquals(MANY_TIMES, uuidsSet.size());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected int getTimes()
   {
      return 100000;
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
