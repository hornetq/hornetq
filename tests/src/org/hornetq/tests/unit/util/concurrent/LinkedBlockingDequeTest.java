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

package org.hornetq.tests.unit.util.concurrent;

import java.util.Iterator;

import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.concurrent.LinkedBlockingDeque;

/**
 * A LinkedBlockingDequeTest
 *
 * @author <a href="jmesnil@redhat.com>Jeff Mesnil</a>
 */
public class LinkedBlockingDequeTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAddFirstWhileIterating() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addFirst("a");
      deque.addFirst("b");

      Iterator<String> iter = deque.iterator();

      System.out.println(deque);

      assertTrue(iter.hasNext());
      assertEquals("b", iter.next());

      deque.addFirst("c");

      System.out.println(deque);

      assertTrue(iter.hasNext());
      assertEquals("a", iter.next());

      assertFalse(iter.hasNext());
   }

   public void testAddLastWhileIterating() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");
      deque.addLast("b");

      System.out.println(deque);

      Iterator<String> iter = deque.iterator();

      assertTrue(iter.hasNext());
      assertEquals("a", iter.next());

      deque.addLast("c");

      System.out.println(deque);

      assertTrue(iter.hasNext());
      assertEquals("b", iter.next());

      assertTrue(iter.hasNext());
      assertEquals("c", iter.next());

      assertFalse(iter.hasNext());
   }

   public void testRemoveFromHeadWhileIterating() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");
      deque.addLast("b");
      deque.addLast("c");

      System.out.println(deque);

      Iterator<String> iter = deque.iterator();

      assertTrue(iter.hasNext());
      assertEquals("a", iter.next());

      assertEquals("a", deque.removeFirst());
      assertEquals("b", deque.removeFirst());
      
      System.out.println(deque);
      
      assertTrue(iter.hasNext());
      assertEquals("b", iter.next());

      assertTrue(iter.hasNext());
      assertEquals("c", iter.next());

      assertFalse(iter.hasNext());
   }
   
   public void testRemoveFromHeadWhileIterating_2() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");
      deque.addLast("b");
      deque.addLast("c");

      System.out.println(deque);

      Iterator<String> iter = deque.iterator();

      assertTrue(iter.hasNext());
      assertEquals("a", iter.next());

      assertEquals("a", deque.removeFirst());
      assertEquals("b", deque.removeFirst());
      assertEquals("c", deque.removeFirst());
      
      assertEquals(0, deque.size());
      
      assertTrue(iter.hasNext());
      assertEquals("b", iter.next());

      assertTrue(iter.hasNext());
      assertEquals("c", iter.next());

      assertFalse(iter.hasNext());
   }
   
   public void testRemoveFromHeadAndAddLastWhileIterating_2() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");
      deque.addLast("b");
      deque.addLast("c");

      System.out.println(deque);

      Iterator<String> iter = deque.iterator();

      assertTrue(iter.hasNext());
      assertEquals("a", iter.next());

      assertEquals("a", deque.removeFirst());
      assertEquals("b", deque.removeFirst());
      
      assertEquals(1, deque.size());
      
      assertTrue(iter.hasNext());
      assertEquals("b", iter.next());

      assertTrue(iter.hasNext());
      assertEquals("c", iter.next());

      deque.addLast("d");
      
      assertFalse(iter.hasNext());
   }
   
   public void testRemoveFromHeadAndAddFirstWhileIterating() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");
      deque.addLast("b");
      deque.addLast("c");

      System.out.println(deque);

      Iterator<String> iter = deque.iterator();

      assertTrue(iter.hasNext());
      assertEquals("a", iter.next());

      assertEquals("a", deque.removeFirst());
      assertEquals("b", deque.removeFirst());
     
      deque.addFirst("d");
      
      System.out.println(deque);
      
      assertEquals(2, deque.size());
      
      assertTrue(iter.hasNext());
      assertEquals("b", iter.next());

      assertTrue(iter.hasNext());
      assertEquals("c", iter.next());

      assertFalse(iter.hasNext());
   }
   
   public void test3() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");
      deque.addLast("b");
      deque.addLast("c");
      deque.addLast("d");

      Iterator<String> iter = deque.iterator();

      assertEquals("a", deque.removeFirst());
      assertEquals("b", deque.removeFirst());
      assertEquals("c", deque.removeFirst());
      assertEquals("d", deque.removeFirst());
      
      deque.addFirst("e");

      assertTrue(iter.hasNext());
      assertEquals("a", iter.next());

      assertTrue(iter.hasNext());
      assertEquals("b", iter.next());

      assertTrue(iter.hasNext());
      assertEquals("c", iter.next());

      assertTrue(iter.hasNext());
      assertEquals("d", iter.next());

      assertFalse(iter.hasNext());
   }
   
   public void test4() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");

      Iterator<String> iter = deque.iterator();

      assertEquals("a", deque.removeFirst());
      deque.addLast("b");

      assertTrue(iter.hasNext());
      assertEquals("a", iter.next());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
