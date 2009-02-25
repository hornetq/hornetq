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

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.Iterator;

import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.ConcurrentHashSet;
import org.jboss.messaging.utils.ConcurrentSet;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ConcurrentHashSetTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ConcurrentSet<String> set;
   private String element;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testAdd() throws Exception
   {
      assertTrue(set.add(element));
      assertFalse(set.add(element));
   }

   public void testAddIfAbsent() throws Exception
   {
      assertTrue(set.addIfAbsent(element));
      assertFalse(set.addIfAbsent(element));
   }
   
   public void testRemove() throws Exception
   {
      assertTrue(set.add(element));
      
      assertTrue(set.remove(element));
      assertFalse(set.remove(element));
   }
   
   public void testContains() throws Exception
   {
      assertFalse(set.contains(element));
      
      assertTrue(set.add(element));      
      assertTrue(set.contains(element));

      assertTrue(set.remove(element));      
      assertFalse(set.contains(element));
   }
   
   public void testSize() throws Exception
   {
      assertEquals(0, set.size());
      
      assertTrue(set.add(element));      
      assertEquals(1, set.size());

      assertTrue(set.remove(element));      
      assertEquals(0, set.size());
   }
   
   public void testClear() throws Exception
   {
      assertTrue(set.add(element));      

      assertTrue(set.contains(element));
      set.clear();
      assertFalse(set.contains(element));
   }
   
   public void testIsEmpty() throws Exception
   {
      assertTrue(set.isEmpty());
      
      assertTrue(set.add(element));      
      assertFalse(set.isEmpty());

      set.clear();
      assertTrue(set.isEmpty());
   }

   public void testIterator() throws Exception
   {
      set.add(element);
      
      Iterator<String> iterator = set.iterator();
      while (iterator.hasNext())
      {
         String e = (String) iterator.next();
         assertEquals(element, e);
      }
   }
   
   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      set = new ConcurrentHashSet<String>();
      element = randomString();
   }

   @Override
   protected void tearDown() throws Exception
   {
      set = null;
      element = null;

      super.tearDown();
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
