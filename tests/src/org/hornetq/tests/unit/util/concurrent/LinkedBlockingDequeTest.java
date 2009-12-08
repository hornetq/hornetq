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

import junit.framework.Assert;

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

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("b", iter.next());

      deque.addFirst("c");

      System.out.println(deque);

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("a", iter.next());

      Assert.assertFalse(iter.hasNext());
   }

   public void testAddLastWhileIterating() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");
      deque.addLast("b");

      System.out.println(deque);

      Iterator<String> iter = deque.iterator();

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("a", iter.next());

      deque.addLast("c");

      System.out.println(deque);

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("b", iter.next());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("c", iter.next());

      Assert.assertFalse(iter.hasNext());
   }

   public void testRemoveFromHeadWhileIterating() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");
      deque.addLast("b");
      deque.addLast("c");

      System.out.println(deque);

      Iterator<String> iter = deque.iterator();

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("a", iter.next());

      Assert.assertEquals("a", deque.removeFirst());
      Assert.assertEquals("b", deque.removeFirst());

      System.out.println(deque);

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("b", iter.next());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("c", iter.next());

      Assert.assertFalse(iter.hasNext());
   }

   public void testRemoveFromHeadWhileIterating_2() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");
      deque.addLast("b");
      deque.addLast("c");

      System.out.println(deque);

      Iterator<String> iter = deque.iterator();

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("a", iter.next());

      Assert.assertEquals("a", deque.removeFirst());
      Assert.assertEquals("b", deque.removeFirst());
      Assert.assertEquals("c", deque.removeFirst());

      Assert.assertEquals(0, deque.size());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("b", iter.next());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("c", iter.next());

      Assert.assertFalse(iter.hasNext());
   }

   public void testRemoveFromHeadAndAddLastWhileIterating_2() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");
      deque.addLast("b");
      deque.addLast("c");

      System.out.println(deque);

      Iterator<String> iter = deque.iterator();

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("a", iter.next());

      Assert.assertEquals("a", deque.removeFirst());
      Assert.assertEquals("b", deque.removeFirst());

      Assert.assertEquals(1, deque.size());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("b", iter.next());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("c", iter.next());

      deque.addLast("d");

      Assert.assertFalse(iter.hasNext());
   }

   public void testRemoveFromHeadAndAddFirstWhileIterating() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");
      deque.addLast("b");
      deque.addLast("c");

      System.out.println(deque);

      Iterator<String> iter = deque.iterator();

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("a", iter.next());

      Assert.assertEquals("a", deque.removeFirst());
      Assert.assertEquals("b", deque.removeFirst());

      deque.addFirst("d");

      System.out.println(deque);

      Assert.assertEquals(2, deque.size());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("b", iter.next());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("c", iter.next());

      Assert.assertFalse(iter.hasNext());
   }

   public void test3() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");
      deque.addLast("b");
      deque.addLast("c");
      deque.addLast("d");

      Iterator<String> iter = deque.iterator();

      Assert.assertEquals("a", deque.removeFirst());
      Assert.assertEquals("b", deque.removeFirst());
      Assert.assertEquals("c", deque.removeFirst());
      Assert.assertEquals("d", deque.removeFirst());

      deque.addFirst("e");

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("a", iter.next());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("b", iter.next());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("c", iter.next());

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("d", iter.next());

      Assert.assertFalse(iter.hasNext());
   }

   public void test4() throws Exception
   {
      LinkedBlockingDeque<String> deque = new LinkedBlockingDeque<String>();

      deque.addLast("a");

      Iterator<String> iter = deque.iterator();

      Assert.assertEquals("a", deque.removeFirst());
      deque.addLast("b");

      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("a", iter.next());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
