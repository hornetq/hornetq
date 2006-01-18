/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.test.messaging.core.refqueue;

import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.refqueue.SingleLinkedDeque;
import org.jboss.test.messaging.MessagingTestCase;

/**
 * A DoubleLinkedDequeTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.1</tt>
 *
 * SingleLinkedDequeTest.java,v 1.1 2006/01/18 12:52:39 timfox Exp
 */
public class SingleLinkedDequeTest extends MessagingTestCase
{
   protected SingleLinkedDeque deque;
   
   protected Object a;
   protected Object b;
   protected Object c;
   protected Object d;
   protected Object e;  
   
   public SingleLinkedDequeTest(String name)
   {
      super(name);
   }
   
   public void setUp() throws Exception
   {
      super.setUp();
      
      deque = new SingleLinkedDeque();
      a = new Object();
      b = new Object();
      c = new Object();
      d = new Object();
      e = new Object();
   }
   
   
   public void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   public void testAddFirst() throws Exception
   {
      deque.addFirst(a);      
      
      deque.addFirst(b);
            
      deque.addFirst(c);
      
      Iterator iter = deque.getAll().iterator();
      int count = 0;
      while (iter.hasNext())
      {
         Object o = iter.next();
         if (count == 0)
         {
            assertEquals(c, o);            
         }
         if (count == 1)
         {
            assertEquals(b, o);            
         }
         if (count == 2)
         {
            assertEquals(a, o);            
         }
         count++;
      }
      assertEquals(3, count);
   }
   
   public void testPeekFirst() throws Exception
   {
      deque.addFirst(a);
      deque.addFirst(b);
      deque.addFirst(c);
      
      Object o = deque.peekFirst();
      
      assertEquals(c, o);
      
      o = deque.peekFirst();
      
      assertEquals(c, o);
      
      o = deque.peekFirst();
      
      assertEquals(c, o);
        
   }
   
   public void testAddLast() throws Exception
   {
      deque.addLast(a);
     
      deque.addLast(b);

      deque.addLast(c);

      Iterator iter = deque.getAll().iterator();
      int count = 0;
      while (iter.hasNext())
      {
         Object o = iter.next();
         if (count == 0)
         {
            assertEquals(a, o);            
         }
         if (count == 1)
         {
            assertEquals(b, o);            
         }
         if (count == 2)
         {
            assertEquals(c, o);            
         }
         count++;
      }
      assertEquals(3, count);
   }
   
   public void testRemoveFirst() throws Exception
   {
      deque.addLast(a);
      deque.addLast(b);
      deque.addLast(c);
      deque.addLast(d);
      deque.addLast(e);
      
      assertEquals(a, deque.removeFirst());
      assertEquals(b, deque.removeFirst());
      assertEquals(c, deque.removeFirst());
      assertEquals(d, deque.removeFirst());
      assertEquals(e, deque.removeFirst());      
      
      List all = deque.getAll();
      assertTrue(all.isEmpty());
   }
   
   public void testSize() throws Exception
   {
      int size = deque.size();
      assertEquals(0, size);
      deque.addLast(a);
      size = deque.size();
      assertEquals(1, size);
      deque.addLast(b);
      size = deque.size();
      assertEquals(2, size);
      deque.addLast(c);
      size = deque.size();
      assertEquals(3, size);
      deque.addLast(d);
      size = deque.size();
      assertEquals(4, size);
      deque.addLast(e);
      size = deque.size();
      assertEquals(5, size);
      
      deque.removeFirst();
      size = deque.size();
      assertEquals(4, size);
      deque.removeFirst();
      size = deque.size();
      assertEquals(3, size);
      deque.removeFirst();
      size = deque.size();
      assertEquals(2, size);
      deque.removeFirst();
      size = deque.size();
      assertEquals(1, size);
      deque.removeFirst();
      size = deque.size();
      assertEquals(0, size);
      
      deque.removeFirst();
      
      deque.removeFirst();
      
      deque.removeFirst();
      
      size = deque.size();
      assertEquals(0, size);
      
      
      size = deque.size();
      assertEquals(0, size);
      deque.addFirst(a);
      size = deque.size();
      assertEquals(1, size);
      deque.addFirst(b);
      size = deque.size();
      assertEquals(2, size);
      deque.addFirst(c);
      size = deque.size();
      assertEquals(3, size);
      deque.addFirst(d);
      size = deque.size();
      assertEquals(4, size);
      deque.addFirst(e);
      size = deque.size();
      assertEquals(5, size);
      
      deque.removeFirst();
      size = deque.size();
      assertEquals(4, size);
      deque.removeFirst();
      size = deque.size();
      assertEquals(3, size);
      deque.removeFirst();
      size = deque.size();
      assertEquals(2, size);
      deque.removeFirst();
      size = deque.size();
      assertEquals(1, size);
      deque.removeFirst();
      size = deque.size();
      assertEquals(0, size);
      
  
      
   }
   
   
}
