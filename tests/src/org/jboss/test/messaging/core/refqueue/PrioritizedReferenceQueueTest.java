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
import java.util.ListIterator;

import org.jboss.messaging.core.refqueue.BasicPrioritizedDeque;
import org.jboss.test.messaging.MessagingTestCase;

/**
 * @author <a href="tim.fox@jboss.com>Tim Fox</a>
 *
 * $Id$
 */
public class PrioritizedReferenceQueueTest extends MessagingTestCase
{
   protected BasicPrioritizedDeque deque;
   
   protected Wibble a;
   protected Wibble b;
   protected Wibble c;
   protected Wibble d;
   protected Wibble e;   
   protected Wibble f;
   protected Wibble g;
   protected Wibble h;
   protected Wibble i;
   protected Wibble j;
   protected Wibble k;
   protected Wibble l;
   protected Wibble m;
   protected Wibble n;
   protected Wibble o;   
   protected Wibble p;
   protected Wibble q;
   protected Wibble r;
   protected Wibble s;
   protected Wibble t;
   protected Wibble u;
   protected Wibble v;
   protected Wibble w;
   protected Wibble x;
   protected Wibble y;   
   protected Wibble z;
   
   public PrioritizedReferenceQueueTest(String name)
   {
      super(name);
   }
   
   public void setUp() throws Exception
   {
      super.setUp();
      
      deque = new BasicPrioritizedDeque(10);
      
      a = new Wibble("a");
      b = new Wibble("b");
      c = new Wibble("c");
      d = new Wibble("d");
      e = new Wibble("e");
      f = new Wibble("f");
      g = new Wibble("g");
      h = new Wibble("h");
      i = new Wibble("i");
      j = new Wibble("j");
      k = new Wibble("k");
      l = new Wibble("l");
      m = new Wibble("m");
      n = new Wibble("n");
      o = new Wibble("o");
      p = new Wibble("p");
      q = new Wibble("q");
      r = new Wibble("r");
      s = new Wibble("s");
      t = new Wibble("t");
      u = new Wibble("u");
      v = new Wibble("v");
      w = new Wibble("w");
      x = new Wibble("x");
      y = new Wibble("y");
      z = new Wibble("z");
   }
   
   
   public void tearDown() throws Exception
   {
      super.tearDown();
   }
    
   public void testAddFirst() throws Exception
   {
      deque.addFirst(a, 0);
      deque.addFirst(b, 0);
      deque.addFirst(c, 0);
      deque.addFirst(d, 0);
      deque.addFirst(e, 0);


      assertEquals(e, deque.removeFirst());
      assertEquals(d, deque.removeFirst());
      assertEquals(c, deque.removeFirst());
      assertEquals(b, deque.removeFirst());
      assertEquals(a, deque.removeFirst());
      assertNull(deque.removeFirst());
   }
   
   public void testAddLast() throws Exception
   {
      deque.addLast(a, 0);
      deque.addLast(b, 0);
      deque.addLast(c, 0);
      deque.addLast(d, 0);
      deque.addLast(e, 0);
      
      assertEquals(a, deque.removeFirst());
      assertEquals(b, deque.removeFirst());
      assertEquals(c, deque.removeFirst());
      assertEquals(d, deque.removeFirst());
      assertEquals(e, deque.removeFirst());
      assertNull(deque.removeFirst());

   }
   
   
   public void testRemoveFirst() throws Exception
   {
      deque.addLast(a, 0);
      deque.addLast(b, 1);
      deque.addLast(c, 2);
      deque.addLast(d, 3);
      deque.addLast(e, 4);
      deque.addLast(f, 5);
      deque.addLast(g, 6);
      deque.addLast(h, 7);
      deque.addLast(i, 8);
      deque.addLast(j, 9);
      
      assertEquals(j, deque.removeFirst());
      assertEquals(i, deque.removeFirst());
      assertEquals(h, deque.removeFirst());
      assertEquals(g, deque.removeFirst());
      assertEquals(f, deque.removeFirst());
      assertEquals(e, deque.removeFirst());
      assertEquals(d, deque.removeFirst());
      assertEquals(c, deque.removeFirst());
      assertEquals(b, deque.removeFirst());
      assertEquals(a, deque.removeFirst());
    
      assertNull(deque.removeFirst());
      
      deque.addLast(a, 9);
      deque.addLast(b, 8);
      deque.addLast(c, 7);
      deque.addLast(d, 6);
      deque.addLast(e, 5);
      deque.addLast(f, 4);
      deque.addLast(g, 3);
      deque.addLast(h, 2);
      deque.addLast(i, 1);
      deque.addLast(j, 0);
      
      assertEquals(a, deque.removeFirst());
      assertEquals(b, deque.removeFirst());
      assertEquals(c, deque.removeFirst());
      assertEquals(d, deque.removeFirst());
      assertEquals(e, deque.removeFirst());
      assertEquals(f, deque.removeFirst());
      assertEquals(g, deque.removeFirst());
      assertEquals(h, deque.removeFirst());
      assertEquals(i, deque.removeFirst());
      assertEquals(j, deque.removeFirst());
    
      assertNull(deque.removeFirst());
      
      deque.addLast(a, 9);
      deque.addLast(b, 0);
      deque.addLast(c, 8);
      deque.addLast(d, 1);
      deque.addLast(e, 7);
      deque.addLast(f, 2);
      deque.addLast(g, 6);
      deque.addLast(h, 3);
      deque.addLast(i, 5);
      deque.addLast(j, 4);
      
      assertEquals(a, deque.removeFirst());
      assertEquals(c, deque.removeFirst());
      assertEquals(e, deque.removeFirst());
      assertEquals(g, deque.removeFirst());
      assertEquals(i, deque.removeFirst());
      assertEquals(j, deque.removeFirst());
      assertEquals(h, deque.removeFirst());
      assertEquals(f, deque.removeFirst());
      assertEquals(d, deque.removeFirst());
      assertEquals(b, deque.removeFirst());
      
      assertNull(deque.removeFirst());
      
      deque.addLast(a, 0);
      deque.addLast(b, 3);
      deque.addLast(c, 3);
      deque.addLast(d, 3);
      deque.addLast(e, 6);
      deque.addLast(f, 6);
      deque.addLast(g, 6);
      deque.addLast(h, 9);
      deque.addLast(i, 9);
      deque.addLast(j, 9);
      
      assertEquals(h, deque.removeFirst());
      assertEquals(i, deque.removeFirst());
      assertEquals(j, deque.removeFirst());
      assertEquals(e, deque.removeFirst());
      assertEquals(f, deque.removeFirst());
      assertEquals(g, deque.removeFirst());
      assertEquals(b, deque.removeFirst());
      assertEquals(c, deque.removeFirst());
      assertEquals(d, deque.removeFirst());
      assertEquals(a, deque.removeFirst());
      
      assertNull(deque.removeFirst());
      
      deque.addLast(a, 5);
      deque.addLast(b, 5);
      deque.addLast(c, 5);
      deque.addLast(d, 5);
      deque.addLast(e, 5);
      deque.addLast(f, 5);
      deque.addLast(g, 5);
      deque.addLast(h, 5);
      deque.addLast(i, 5);
      deque.addLast(j, 5);
      
      assertEquals(a, deque.removeFirst());
      assertEquals(b, deque.removeFirst());
      assertEquals(c, deque.removeFirst());
      assertEquals(d, deque.removeFirst());
      assertEquals(e, deque.removeFirst());
      assertEquals(f, deque.removeFirst());
      assertEquals(g, deque.removeFirst());
      assertEquals(h, deque.removeFirst());
      assertEquals(i, deque.removeFirst());
      assertEquals(j, deque.removeFirst());
      
      assertNull(deque.removeFirst());
      
      deque.addLast(j, 5);
      deque.addLast(i, 5);
      deque.addLast(h, 5);
      deque.addLast(g, 5);
      deque.addLast(f, 5);
      deque.addLast(e, 5);
      deque.addLast(d, 5);
      deque.addLast(c, 5);
      deque.addLast(b, 5);
      deque.addLast(a, 5);
      
      assertEquals(j, deque.removeFirst());
      assertEquals(i, deque.removeFirst());
      assertEquals(h, deque.removeFirst());
      assertEquals(g, deque.removeFirst());
      assertEquals(f, deque.removeFirst());
      assertEquals(e, deque.removeFirst());
      assertEquals(d, deque.removeFirst());
      assertEquals(c, deque.removeFirst());
      assertEquals(b, deque.removeFirst());
      assertEquals(a, deque.removeFirst());
      
      assertNull(deque.removeFirst());
      
   }
   
   public void testGetAll() throws Exception
   {
      deque.addLast(a, 0);
      deque.addLast(b, 3);
      deque.addLast(c, 3);
      deque.addLast(d, 3);
      deque.addLast(e, 6);
      deque.addLast(f, 6);
      deque.addLast(g, 6);
      deque.addLast(h, 9);
      deque.addLast(i, 9);
      deque.addLast(j, 9);
      
      
      Iterator iter = deque.getAll().iterator();
      int count = 0;
      while (iter.hasNext())
      {
         Object o = iter.next();
         if (count == 0)
         {
            assertEquals(h, o);
         }
         if (count == 1)
         {
            assertEquals(i, o);
         }
         if (count == 2)
         {
            assertEquals(j, o);
         }
         if (count == 3)
         {
            assertEquals(e, o);
         }
         if (count == 4)
         {
            assertEquals(f, o);
         }
         if (count == 5)
         {
            assertEquals(g, o);
         }
         if (count == 6)
         {
            assertEquals(b, o);
         }
         if (count == 7)
         {
            assertEquals(c, o);
         }
         if (count == 8)
         {
            assertEquals(d, o);
         }
         if (count == 9)
         {
            assertEquals(a, o);
         }
         count++;
      }
      assertEquals(10, count);
   }
   
   public void testIterator()
   {
      deque.addLast(a, 9);
      deque.addLast(b, 9);
      deque.addLast(c, 8);
      deque.addLast(d, 8);
      deque.addLast(e, 7);
      deque.addLast(f, 7);
      deque.addLast(g, 7);
      deque.addLast(h, 6);
      deque.addLast(i, 6);
      deque.addLast(j, 6);
      deque.addLast(k, 5);
      deque.addLast(l, 5);
      deque.addLast(m, 4);
      deque.addLast(n, 4);
      deque.addLast(o, 4);
      deque.addLast(p, 3);
      deque.addLast(q, 3);
      deque.addLast(r, 3);
      deque.addLast(s, 2);
      deque.addLast(t, 2);
      deque.addLast(u, 2);
      deque.addLast(v, 1);
      deque.addLast(w, 1);
      deque.addLast(x, 1);
      deque.addLast(y, 0);
      deque.addLast(z, 0);
      
      ListIterator iter = deque.iterator();
      
      int c = 0;
      while (iter.hasNext())
      {
         Wibble w = (Wibble)iter.next();
         c++;
      }      
      assertEquals(c, 26);
      
      iter = deque.iterator();
      assertTrue(iter.hasNext());
      Wibble w = (Wibble)iter.next();
      assertEquals("a", w.s);      
      w = (Wibble)iter.next();
      assertEquals("b", w.s);
      w = (Wibble)iter.next();
      assertEquals("c", w.s);
      w = (Wibble)iter.next();
      assertEquals("d", w.s);
      w = (Wibble)iter.next();
      assertEquals("e", w.s);
      w = (Wibble)iter.next();
      assertEquals("f", w.s);
      w = (Wibble)iter.next();
      assertEquals("g", w.s);
      w = (Wibble)iter.next();
      assertEquals("h", w.s);
      w = (Wibble)iter.next();
      assertEquals("i", w.s);
      w = (Wibble)iter.next();
      assertEquals("j", w.s);
      w = (Wibble)iter.next();
      assertEquals("k", w.s);
      w = (Wibble)iter.next();
      assertEquals("l", w.s);
      w = (Wibble)iter.next();
      assertEquals("m", w.s);
      w = (Wibble)iter.next();
      assertEquals("n", w.s);
      w = (Wibble)iter.next();
      assertEquals("o", w.s);
      w = (Wibble)iter.next();
      assertEquals("p", w.s);
      w = (Wibble)iter.next();
      assertEquals("q", w.s);
      w = (Wibble)iter.next();
      assertEquals("r", w.s);
      w = (Wibble)iter.next();
      assertEquals("s", w.s);
      w = (Wibble)iter.next();
      assertEquals("t", w.s);
      w = (Wibble)iter.next();
      assertEquals("u", w.s);
      w = (Wibble)iter.next();
      assertEquals("v", w.s);
      w = (Wibble)iter.next();
      assertEquals("w", w.s);
      w = (Wibble)iter.next();
      assertEquals("x", w.s);
      w = (Wibble)iter.next();
      assertEquals("y", w.s);
      w = (Wibble)iter.next();
      assertEquals("z", w.s);
      assertFalse(iter.hasNext());
      
      iter = deque.iterator();
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      assertEquals("a", w.s);   
      
      iter.remove();
      
      w = (Wibble)iter.next();
      assertEquals("b", w.s);
      w = (Wibble)iter.next();
      assertEquals("c", w.s);
      w = (Wibble)iter.next();
      assertEquals("d", w.s);
      
      iter.remove();
      
      w = (Wibble)iter.next();
      assertEquals("e", w.s);
      w = (Wibble)iter.next();
      assertEquals("f", w.s);
      w = (Wibble)iter.next();
      assertEquals("g", w.s);
      w = (Wibble)iter.next();
      assertEquals("h", w.s);
      w = (Wibble)iter.next();
      assertEquals("i", w.s);
      w = (Wibble)iter.next();
      assertEquals("j", w.s);
      
      iter.remove();
      
      w = (Wibble)iter.next();
      assertEquals("k", w.s);
      w = (Wibble)iter.next();
      assertEquals("l", w.s);
      w = (Wibble)iter.next();
      assertEquals("m", w.s);
      w = (Wibble)iter.next();
      assertEquals("n", w.s);
      w = (Wibble)iter.next();
      assertEquals("o", w.s);
      w = (Wibble)iter.next();
      assertEquals("p", w.s);
      w = (Wibble)iter.next();
      assertEquals("q", w.s);
      w = (Wibble)iter.next();
      assertEquals("r", w.s);
      w = (Wibble)iter.next();
      assertEquals("s", w.s);
      w = (Wibble)iter.next();
      assertEquals("t", w.s);
      w = (Wibble)iter.next();
      assertEquals("u", w.s);
      w = (Wibble)iter.next();
      assertEquals("v", w.s);
      w = (Wibble)iter.next();
      assertEquals("w", w.s);
      w = (Wibble)iter.next();
      assertEquals("x", w.s);
      w = (Wibble)iter.next();
      assertEquals("y", w.s);
      w = (Wibble)iter.next();
      assertEquals("z", w.s);
      iter.remove();
      assertFalse(iter.hasNext());
      
      iter = deque.iterator();
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      assertEquals("b", w.s);   
      w = (Wibble)iter.next();
      assertEquals("c", w.s);
      w = (Wibble)iter.next();
      assertEquals("e", w.s);
      w = (Wibble)iter.next();
      assertEquals("f", w.s);
      w = (Wibble)iter.next();
      assertEquals("g", w.s);
      w = (Wibble)iter.next();
      assertEquals("h", w.s);
      w = (Wibble)iter.next();
      assertEquals("i", w.s);
      w = (Wibble)iter.next();
      assertEquals("k", w.s);
      w = (Wibble)iter.next();
      assertEquals("l", w.s);
      w = (Wibble)iter.next();
      assertEquals("m", w.s);
      w = (Wibble)iter.next();
      assertEquals("n", w.s);
      w = (Wibble)iter.next();
      assertEquals("o", w.s);
      w = (Wibble)iter.next();
      assertEquals("p", w.s);
      w = (Wibble)iter.next();
      assertEquals("q", w.s);
      w = (Wibble)iter.next();
      assertEquals("r", w.s);
      w = (Wibble)iter.next();
      assertEquals("s", w.s);
      w = (Wibble)iter.next();
      assertEquals("t", w.s);
      w = (Wibble)iter.next();
      assertEquals("u", w.s);
      w = (Wibble)iter.next();
      assertEquals("v", w.s);
      w = (Wibble)iter.next();
      assertEquals("w", w.s);
      w = (Wibble)iter.next();
      assertEquals("x", w.s);
      w = (Wibble)iter.next();
      assertEquals("y", w.s);     
      assertFalse(iter.hasNext());
      
   }
      
     
   public void testClear()
   {
      deque.addLast(a, 0);
      deque.addLast(b, 3);
      deque.addLast(c, 3);
      deque.addLast(d, 3);
      deque.addLast(e, 6);
      deque.addLast(f, 6);
      deque.addLast(g, 6);
      deque.addLast(h, 9);
      deque.addLast(i, 9);
      deque.addLast(j, 9);
      
      deque.clear();
      
      assertNull(deque.removeFirst());
      
      assertTrue(deque.getAll().isEmpty());
   }
   
   class Wibble
   {
      String s;
      Wibble(String s)
      {
         this.s = s;
      }
      public String toString()
      {
         return s;
      }
   }
   
}

