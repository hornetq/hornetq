/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.util.concurrent;

import java.util.Iterator;

import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.concurrent.LinkedBlockingDeque;

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
