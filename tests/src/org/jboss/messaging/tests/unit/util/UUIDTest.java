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

import junit.framework.TestCase;
import org.jboss.messaging.util.UUIDGenerator;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class UUIDTest extends TestCase
{
   // Constants -----------------------------------------------------

   private static final int MANY_TIMES = 100000;
   
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testManyUUIDs() throws Exception
   {
      List<String> uuids = new ArrayList<String>(MANY_TIMES);
      
      UUIDGenerator gen = UUIDGenerator.getInstance();
      InetAddress addr = InetAddress.getLocalHost();
      
      for (int i = 0; i < MANY_TIMES; i++)
      {
         uuids.add(gen.generateTimeBasedUUID(addr).toString());
      }
      
      // we put them in a set to check duplicates
      Set<String> uuidsSet = new HashSet<String>(uuids);
      assertEquals(MANY_TIMES, uuidsSet.size());
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
