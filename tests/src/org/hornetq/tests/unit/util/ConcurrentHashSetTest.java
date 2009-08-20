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

package org.hornetq.tests.unit.util;

import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.Iterator;

import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ConcurrentSet;

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
