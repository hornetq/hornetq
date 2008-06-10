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
package org.jboss.messaging.tests.unit.core.list.impl;

import java.util.Iterator;

import org.jboss.messaging.core.list.PriorityHeadInsertableQueue;
import org.jboss.messaging.core.list.impl.PriorityHeadInsertableQueueImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="tim.fox@jboss.com>Tim Fox</a>
 *
 * $Id: PriorityHeadInsertableQueueTest.java 4055 2008-04-15 09:24:10Z ataylor $
 */
public class PriorityHeadInsertableQueueTest extends UnitTestCase
{
   protected PriorityHeadInsertableQueue<Wibble> queue;
   
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
   
   public void setUp() throws Exception
   {
      super.setUp();
      
      queue = new PriorityHeadInsertableQueueImpl<Wibble>(10);
      
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
         
   public void testofferFirst() throws Exception
   {
      //for now will throw UnsupportedOperationException
      
      try
      {
         queue.offerFirst(a, 0);
         
         fail("Should throw exception");
      }
      catch (UnsupportedOperationException e)
      {
         //Ok
      }
      
//      queue.offerFirst(a, 0);
//      queue.offerFirst(b, 0);
//      queue.offerFirst(c, 0);
//      queue.offerFirst(d, 0);
//      queue.offerFirst(e, 0);
//
//
//      assertEquals(e, queue.poll());
//      assertEquals(d, queue.poll());
//      assertEquals(c, queue.poll());
//      assertEquals(b, queue.poll());
//      assertEquals(a, queue.poll());
//      assertNull(queue.poll());
   }
   
   public void testofferLast() throws Exception
   {
      queue.offerLast(a, 0);
      queue.offerLast(b, 0);
      queue.offerLast(c, 0);
      queue.offerLast(d, 0);
      queue.offerLast(e, 0);
      
      assertEquals(a, queue.poll());
      assertEquals(b, queue.poll());
      assertEquals(c, queue.poll());
      assertEquals(d, queue.poll());
      assertEquals(e, queue.poll());
      assertNull(queue.poll());

   }
   
   
   public void testpoll() throws Exception
   {
      queue.offerLast(a, 0);
      queue.offerLast(b, 1);
      queue.offerLast(c, 2);
      queue.offerLast(d, 3);
      queue.offerLast(e, 4);
      queue.offerLast(f, 5);
      queue.offerLast(g, 6);
      queue.offerLast(h, 7);
      queue.offerLast(i, 8);
      queue.offerLast(j, 9);
      
      assertEquals(j, queue.poll());
      assertEquals(i, queue.poll());
      assertEquals(h, queue.poll());
      assertEquals(g, queue.poll());
      assertEquals(f, queue.poll());
      assertEquals(e, queue.poll());
      assertEquals(d, queue.poll());
      assertEquals(c, queue.poll());
      assertEquals(b, queue.poll());
      assertEquals(a, queue.poll());
    
      assertNull(queue.poll());
      
      queue.offerLast(a, 9);
      queue.offerLast(b, 8);
      queue.offerLast(c, 7);
      queue.offerLast(d, 6);
      queue.offerLast(e, 5);
      queue.offerLast(f, 4);
      queue.offerLast(g, 3);
      queue.offerLast(h, 2);
      queue.offerLast(i, 1);
      queue.offerLast(j, 0);
      
      assertEquals(a, queue.poll());
      assertEquals(b, queue.poll());
      assertEquals(c, queue.poll());
      assertEquals(d, queue.poll());
      assertEquals(e, queue.poll());
      assertEquals(f, queue.poll());
      assertEquals(g, queue.poll());
      assertEquals(h, queue.poll());
      assertEquals(i, queue.poll());
      assertEquals(j, queue.poll());
    
      assertNull(queue.poll());
      
      queue.offerLast(a, 9);
      queue.offerLast(b, 0);
      queue.offerLast(c, 8);
      queue.offerLast(d, 1);
      queue.offerLast(e, 7);
      queue.offerLast(f, 2);
      queue.offerLast(g, 6);
      queue.offerLast(h, 3);
      queue.offerLast(i, 5);
      queue.offerLast(j, 4);
      
      assertEquals(a, queue.poll());
      assertEquals(c, queue.poll());
      assertEquals(e, queue.poll());
      assertEquals(g, queue.poll());
      assertEquals(i, queue.poll());
      assertEquals(j, queue.poll());
      assertEquals(h, queue.poll());
      assertEquals(f, queue.poll());
      assertEquals(d, queue.poll());
      assertEquals(b, queue.poll());
      
      assertNull(queue.poll());
      
      queue.offerLast(a, 0);
      queue.offerLast(b, 3);
      queue.offerLast(c, 3);
      queue.offerLast(d, 3);
      queue.offerLast(e, 6);
      queue.offerLast(f, 6);
      queue.offerLast(g, 6);
      queue.offerLast(h, 9);
      queue.offerLast(i, 9);
      queue.offerLast(j, 9);
      
      assertEquals(h, queue.poll());
      assertEquals(i, queue.poll());
      assertEquals(j, queue.poll());
      assertEquals(e, queue.poll());
      assertEquals(f, queue.poll());
      assertEquals(g, queue.poll());
      assertEquals(b, queue.poll());
      assertEquals(c, queue.poll());
      assertEquals(d, queue.poll());
      assertEquals(a, queue.poll());
      
      assertNull(queue.poll());
      
      queue.offerLast(a, 5);
      queue.offerLast(b, 5);
      queue.offerLast(c, 5);
      queue.offerLast(d, 5);
      queue.offerLast(e, 5);
      queue.offerLast(f, 5);
      queue.offerLast(g, 5);
      queue.offerLast(h, 5);
      queue.offerLast(i, 5);
      queue.offerLast(j, 5);
      
      assertEquals(a, queue.poll());
      assertEquals(b, queue.poll());
      assertEquals(c, queue.poll());
      assertEquals(d, queue.poll());
      assertEquals(e, queue.poll());
      assertEquals(f, queue.poll());
      assertEquals(g, queue.poll());
      assertEquals(h, queue.poll());
      assertEquals(i, queue.poll());
      assertEquals(j, queue.poll());
      
      assertNull(queue.poll());
      
      queue.offerLast(j, 5);
      queue.offerLast(i, 5);
      queue.offerLast(h, 5);
      queue.offerLast(g, 5);
      queue.offerLast(f, 5);
      queue.offerLast(e, 5);
      queue.offerLast(d, 5);
      queue.offerLast(c, 5);
      queue.offerLast(b, 5);
      queue.offerLast(a, 5);
      
      assertEquals(j, queue.poll());
      assertEquals(i, queue.poll());
      assertEquals(h, queue.poll());
      assertEquals(g, queue.poll());
      assertEquals(f, queue.poll());
      assertEquals(e, queue.poll());
      assertEquals(d, queue.poll());
      assertEquals(c, queue.poll());
      assertEquals(b, queue.poll());
      assertEquals(a, queue.poll());
      
      assertNull(queue.poll());
      
   }
   
   public void testGetAll() throws Exception
   {
      queue.offerLast(a, 0);
      queue.offerLast(b, 3);
      queue.offerLast(c, 3);
      queue.offerLast(d, 3);
      queue.offerLast(e, 6);
      queue.offerLast(f, 6);
      queue.offerLast(g, 6);
      queue.offerLast(h, 9);
      queue.offerLast(i, 9);
      queue.offerLast(j, 9);
      
      
      Iterator iter = queue.getAll().iterator();
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
      queue.offerLast(a, 9);
      queue.offerLast(b, 9);
      queue.offerLast(c, 8);
      queue.offerLast(d, 8);
      queue.offerLast(e, 7);
      queue.offerLast(f, 7);
      queue.offerLast(g, 7);
      queue.offerLast(h, 6);
      queue.offerLast(i, 6);
      queue.offerLast(j, 6);
      queue.offerLast(k, 5);
      queue.offerLast(l, 5);
      queue.offerLast(m, 4);
      queue.offerLast(n, 4);
      queue.offerLast(o, 4);
      queue.offerLast(p, 3);
      queue.offerLast(q, 3);
      queue.offerLast(r, 3);
      queue.offerLast(s, 2);
      queue.offerLast(t, 2);
      queue.offerLast(u, 2);
      queue.offerLast(v, 1);
      queue.offerLast(w, 1);
      queue.offerLast(x, 1);
      queue.offerLast(y, 0);
      queue.offerLast(z, 0);
      
      Iterator<Wibble> iter = queue.iterator();
      
      int c = 0;
      while (iter.hasNext())
      {
         iter.next();
         c++;
      }      
      assertEquals(c, 26);
      assertEquals(26, queue.size());
      
      iter = queue.iterator();
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
      
      iter = queue.iterator();
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      assertEquals("a", w.s);   
      
      iter.remove();
      
      assertEquals(25, queue.size());
      
      w = (Wibble)iter.next();
      assertEquals("b", w.s);
      w = (Wibble)iter.next();
      assertEquals("c", w.s);
      w = (Wibble)iter.next();
      assertEquals("d", w.s);
      
      iter.remove();
      
      assertEquals(24, queue.size());
      
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
      
      assertEquals(23, queue.size());
      
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
      
      iter = queue.iterator();
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
      queue.offerLast(a, 0);
      queue.offerLast(b, 3);
      queue.offerLast(c, 3);
      queue.offerLast(d, 3);
      queue.offerLast(e, 6);
      queue.offerLast(f, 6);
      queue.offerLast(g, 6);
      queue.offerLast(h, 9);
      queue.offerLast(i, 9);
      queue.offerLast(j, 9);
      
      queue.clear();
      
      assertNull(queue.poll());
      
      assertTrue(queue.getAll().isEmpty());
   }
   
   public void testIsEmpty() throws Exception
   {
      assertTrue(queue.isEmpty());

      queue.offerLast(a, 0);

      assertFalse(queue.isEmpty());

      Wibble w = queue.poll();
      assertEquals(a, w);
      assertTrue(queue.isEmpty());
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

