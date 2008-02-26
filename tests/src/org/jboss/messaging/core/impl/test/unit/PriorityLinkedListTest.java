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
package org.jboss.messaging.core.impl.test.unit;

import java.util.Iterator;
import java.util.ListIterator;

import org.jboss.messaging.core.list.PriorityLinkedList;
import org.jboss.messaging.core.list.impl.PriorityLinkedListImpl;
import org.jboss.messaging.test.unit.UnitTestCase;

/**
 * @author <a href="tim.fox@jboss.com>Tim Fox</a>
 *
 * $Id$
 */
public class PriorityLinkedListTest extends UnitTestCase
{
   protected PriorityLinkedList<Wibble> list;
   
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
      
      list = new PriorityLinkedListImpl<Wibble>(10);
      
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
      list.addFirst(a, 0);
      list.addFirst(b, 0);
      list.addFirst(c, 0);
      list.addFirst(d, 0);
      list.addFirst(e, 0);


      assertEquals(e, list.removeFirst());
      assertEquals(d, list.removeFirst());
      assertEquals(c, list.removeFirst());
      assertEquals(b, list.removeFirst());
      assertEquals(a, list.removeFirst());
      assertNull(list.removeFirst());
   }
   
   public void testAddLast() throws Exception
   {
      list.addLast(a, 0);
      list.addLast(b, 0);
      list.addLast(c, 0);
      list.addLast(d, 0);
      list.addLast(e, 0);
      
      assertEquals(a, list.removeFirst());
      assertEquals(b, list.removeFirst());
      assertEquals(c, list.removeFirst());
      assertEquals(d, list.removeFirst());
      assertEquals(e, list.removeFirst());
      assertNull(list.removeFirst());

   }
   
   
   public void testRemoveFirst() throws Exception
   {
      list.addLast(a, 0);
      list.addLast(b, 1);
      list.addLast(c, 2);
      list.addLast(d, 3);
      list.addLast(e, 4);
      list.addLast(f, 5);
      list.addLast(g, 6);
      list.addLast(h, 7);
      list.addLast(i, 8);
      list.addLast(j, 9);
      
      assertEquals(j, list.removeFirst());
      assertEquals(i, list.removeFirst());
      assertEquals(h, list.removeFirst());
      assertEquals(g, list.removeFirst());
      assertEquals(f, list.removeFirst());
      assertEquals(e, list.removeFirst());
      assertEquals(d, list.removeFirst());
      assertEquals(c, list.removeFirst());
      assertEquals(b, list.removeFirst());
      assertEquals(a, list.removeFirst());
    
      assertNull(list.removeFirst());
      
      list.addLast(a, 9);
      list.addLast(b, 8);
      list.addLast(c, 7);
      list.addLast(d, 6);
      list.addLast(e, 5);
      list.addLast(f, 4);
      list.addLast(g, 3);
      list.addLast(h, 2);
      list.addLast(i, 1);
      list.addLast(j, 0);
      
      assertEquals(a, list.removeFirst());
      assertEquals(b, list.removeFirst());
      assertEquals(c, list.removeFirst());
      assertEquals(d, list.removeFirst());
      assertEquals(e, list.removeFirst());
      assertEquals(f, list.removeFirst());
      assertEquals(g, list.removeFirst());
      assertEquals(h, list.removeFirst());
      assertEquals(i, list.removeFirst());
      assertEquals(j, list.removeFirst());
    
      assertNull(list.removeFirst());
      
      list.addLast(a, 9);
      list.addLast(b, 0);
      list.addLast(c, 8);
      list.addLast(d, 1);
      list.addLast(e, 7);
      list.addLast(f, 2);
      list.addLast(g, 6);
      list.addLast(h, 3);
      list.addLast(i, 5);
      list.addLast(j, 4);
      
      assertEquals(a, list.removeFirst());
      assertEquals(c, list.removeFirst());
      assertEquals(e, list.removeFirst());
      assertEquals(g, list.removeFirst());
      assertEquals(i, list.removeFirst());
      assertEquals(j, list.removeFirst());
      assertEquals(h, list.removeFirst());
      assertEquals(f, list.removeFirst());
      assertEquals(d, list.removeFirst());
      assertEquals(b, list.removeFirst());
      
      assertNull(list.removeFirst());
      
      list.addLast(a, 0);
      list.addLast(b, 3);
      list.addLast(c, 3);
      list.addLast(d, 3);
      list.addLast(e, 6);
      list.addLast(f, 6);
      list.addLast(g, 6);
      list.addLast(h, 9);
      list.addLast(i, 9);
      list.addLast(j, 9);
      
      assertEquals(h, list.removeFirst());
      assertEquals(i, list.removeFirst());
      assertEquals(j, list.removeFirst());
      assertEquals(e, list.removeFirst());
      assertEquals(f, list.removeFirst());
      assertEquals(g, list.removeFirst());
      assertEquals(b, list.removeFirst());
      assertEquals(c, list.removeFirst());
      assertEquals(d, list.removeFirst());
      assertEquals(a, list.removeFirst());
      
      assertNull(list.removeFirst());
      
      list.addLast(a, 5);
      list.addLast(b, 5);
      list.addLast(c, 5);
      list.addLast(d, 5);
      list.addLast(e, 5);
      list.addLast(f, 5);
      list.addLast(g, 5);
      list.addLast(h, 5);
      list.addLast(i, 5);
      list.addLast(j, 5);
      
      assertEquals(a, list.removeFirst());
      assertEquals(b, list.removeFirst());
      assertEquals(c, list.removeFirst());
      assertEquals(d, list.removeFirst());
      assertEquals(e, list.removeFirst());
      assertEquals(f, list.removeFirst());
      assertEquals(g, list.removeFirst());
      assertEquals(h, list.removeFirst());
      assertEquals(i, list.removeFirst());
      assertEquals(j, list.removeFirst());
      
      assertNull(list.removeFirst());
      
      list.addLast(j, 5);
      list.addLast(i, 5);
      list.addLast(h, 5);
      list.addLast(g, 5);
      list.addLast(f, 5);
      list.addLast(e, 5);
      list.addLast(d, 5);
      list.addLast(c, 5);
      list.addLast(b, 5);
      list.addLast(a, 5);
      
      assertEquals(j, list.removeFirst());
      assertEquals(i, list.removeFirst());
      assertEquals(h, list.removeFirst());
      assertEquals(g, list.removeFirst());
      assertEquals(f, list.removeFirst());
      assertEquals(e, list.removeFirst());
      assertEquals(d, list.removeFirst());
      assertEquals(c, list.removeFirst());
      assertEquals(b, list.removeFirst());
      assertEquals(a, list.removeFirst());
      
      assertNull(list.removeFirst());
      
   }
   
   public void testGetAll() throws Exception
   {
      list.addLast(a, 0);
      list.addLast(b, 3);
      list.addLast(c, 3);
      list.addLast(d, 3);
      list.addLast(e, 6);
      list.addLast(f, 6);
      list.addLast(g, 6);
      list.addLast(h, 9);
      list.addLast(i, 9);
      list.addLast(j, 9);
      
      
      Iterator iter = list.getAll().iterator();
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
      list.addLast(a, 9);
      list.addLast(b, 9);
      list.addLast(c, 8);
      list.addLast(d, 8);
      list.addLast(e, 7);
      list.addLast(f, 7);
      list.addLast(g, 7);
      list.addLast(h, 6);
      list.addLast(i, 6);
      list.addLast(j, 6);
      list.addLast(k, 5);
      list.addLast(l, 5);
      list.addLast(m, 4);
      list.addLast(n, 4);
      list.addLast(o, 4);
      list.addLast(p, 3);
      list.addLast(q, 3);
      list.addLast(r, 3);
      list.addLast(s, 2);
      list.addLast(t, 2);
      list.addLast(u, 2);
      list.addLast(v, 1);
      list.addLast(w, 1);
      list.addLast(x, 1);
      list.addLast(y, 0);
      list.addLast(z, 0);
      
      ListIterator iter = list.iterator();
      
      int c = 0;
      while (iter.hasNext())
      {
         iter.next();
         c++;
      }      
      assertEquals(c, 26);
      assertEquals(26, list.size());
      
      iter = list.iterator();
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
      
      iter = list.iterator();
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      assertEquals("a", w.s);   
      
      iter.remove();
      
      assertEquals(25, list.size());
      
      w = (Wibble)iter.next();
      assertEquals("b", w.s);
      w = (Wibble)iter.next();
      assertEquals("c", w.s);
      w = (Wibble)iter.next();
      assertEquals("d", w.s);
      
      iter.remove();
      
      assertEquals(24, list.size());
      
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
      
      assertEquals(23, list.size());
      
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
      
      iter = list.iterator();
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
      list.addLast(a, 0);
      list.addLast(b, 3);
      list.addLast(c, 3);
      list.addLast(d, 3);
      list.addLast(e, 6);
      list.addLast(f, 6);
      list.addLast(g, 6);
      list.addLast(h, 9);
      list.addLast(i, 9);
      list.addLast(j, 9);
      
      list.clear();
      
      assertNull(list.removeFirst());
      
      assertTrue(list.getAll().isEmpty());
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

