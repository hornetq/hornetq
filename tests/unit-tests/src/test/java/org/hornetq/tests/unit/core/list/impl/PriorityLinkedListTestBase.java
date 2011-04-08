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

package org.hornetq.tests.unit.core.list.impl;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.hornetq.utils.LinkedListIterator;
import org.hornetq.utils.PriorityLinkedListImpl;

/**
 * @author <a href="tim.fox@jboss.com>Tim Fox</a>
 *
 * $Id$
 */
public abstract class PriorityLinkedListTestBase extends TestCase
{
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
   
   private PriorityLinkedListImpl<Wibble> list;
   
   protected abstract PriorityLinkedListImpl<Wibble> getList();

   public void setUp() throws Exception
   {
      super.setUp();

      list = getList();

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
      list = null;
      super.tearDown();
   }

   public void testEmpty() throws Exception
   {
      Assert.assertTrue(list.isEmpty());

      list.addHead(a, 0);

      Assert.assertFalse(list.isEmpty());

      Wibble w = list.poll();
      Assert.assertEquals(a, w);
      Assert.assertTrue(list.isEmpty());
      
      assertEquals(0, list.size());
   }

   public void testaddHead() throws Exception
   {
      list.addHead(a, 0);
      list.addHead(b, 0);
      list.addHead(c, 0);
      list.addHead(d, 0);
      list.addHead(e, 0);
      
      assertEquals(5, list.size());

      Assert.assertEquals(e, list.poll());
      Assert.assertEquals(d, list.poll());
      Assert.assertEquals(c, list.poll());
      Assert.assertEquals(b, list.poll());
      Assert.assertEquals(a, list.poll());
      Assert.assertNull(list.poll());
      
      assertEquals(0, list.size());
   }

   public void testaddTail() throws Exception
   {
      list.addTail(a, 0);
      list.addTail(b, 0);
      list.addTail(c, 0);
      list.addTail(d, 0);
      list.addTail(e, 0);
      assertEquals(5, list.size());

      Assert.assertEquals(a, list.poll());
      Assert.assertEquals(b, list.poll());
      Assert.assertEquals(c, list.poll());
      Assert.assertEquals(d, list.poll());
      Assert.assertEquals(e, list.poll());
      Assert.assertNull(list.poll());
      
      assertEquals(0, list.size());

   }
   
   public void testAddLastAndFirst() throws Exception
   {
      list.addTail(a, 0);
      list.addTail(b, 0);
      list.addTail(c, 0);
      list.addTail(d, 0);
      list.addTail(e, 0);
      list.addTail(f, 0);
      list.addTail(g, 0);
      list.addTail(h, 0);
      list.addTail(i, 0);
      list.addTail(j, 0);
      
      list.addHead(k, 0);
      list.addHead(l, 0);
      list.addHead(m, 0);
      list.addHead(n, 0);
      list.addHead(o, 0);
      list.addHead(p, 0);
      list.addHead(q, 0);
      list.addHead(r, 0);
      list.addHead(s, 0);
      list.addHead(t, 0);
      
      assertEquals(t, list.poll());
      assertEquals(s, list.poll());
      assertEquals(r, list.poll());
      assertEquals(q, list.poll());
      assertEquals(p, list.poll());
      assertEquals(o, list.poll());
      assertEquals(n, list.poll());
      assertEquals(m, list.poll());
      assertEquals(l, list.poll());
      assertEquals(k, list.poll());
      
      assertEquals(a, list.poll());
      assertEquals(b, list.poll());
      assertEquals(c, list.poll());
      assertEquals(d, list.poll());
      assertEquals(e, list.poll());
      assertEquals(f, list.poll());
      assertEquals(g, list.poll());
      assertEquals(h, list.poll());
      assertEquals(i, list.poll());
      assertEquals(j, list.poll());
   }
   
   public void testAddLastAndFirstWithIterator() throws Exception
   {
      list.addTail(a, 0);
      list.addTail(b, 0);
      list.addTail(c, 0);
      list.addTail(d, 0);
      list.addTail(e, 0);
      list.addTail(f, 0);
      list.addTail(g, 0);
      list.addTail(h, 0);
      list.addTail(i, 0);
      list.addTail(j, 0);
      
      list.addHead(k, 0);
      list.addHead(l, 0);
      list.addHead(m, 0);
      list.addHead(n, 0);
      list.addHead(o, 0);
      list.addHead(p, 0);
      list.addHead(q, 0);
      list.addHead(r, 0);
      list.addHead(s, 0);
      list.addHead(t, 0);
      
      LinkedListIterator<Wibble> iter = list.iterator();
           
      assertTrue(iter.hasNext());
      assertEquals(t, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(s, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(r, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(q, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(p, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(o, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(n, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(m, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(l, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(k, iter.next());
 
      assertTrue(iter.hasNext());
      assertEquals(a, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(b, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(c, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(d, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(e, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(f, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(g, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(h, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(i, iter.next());
      assertTrue(iter.hasNext());
      assertEquals(j, iter.next());
   }

   public void testPoll() throws Exception
   {
      list.addTail(a, 0);
      list.addTail(b, 1);
      list.addTail(c, 2);
      list.addTail(d, 3);
      list.addTail(e, 4);
      list.addTail(f, 5);
      list.addTail(g, 6);
      list.addTail(h, 7);
      list.addTail(i, 8);
      list.addTail(j, 9);

      Assert.assertEquals(j, list.poll());
      Assert.assertEquals(i, list.poll());
      Assert.assertEquals(h, list.poll());
      Assert.assertEquals(g, list.poll());
      Assert.assertEquals(f, list.poll());
      Assert.assertEquals(e, list.poll());
      Assert.assertEquals(d, list.poll());
      Assert.assertEquals(c, list.poll());
      Assert.assertEquals(b, list.poll());
      Assert.assertEquals(a, list.poll());

      Assert.assertNull(list.poll());

      list.addTail(a, 9);
      list.addTail(b, 8);
      list.addTail(c, 7);
      list.addTail(d, 6);
      list.addTail(e, 5);
      list.addTail(f, 4);
      list.addTail(g, 3);
      list.addTail(h, 2);
      list.addTail(i, 1);
      list.addTail(j, 0);

      Assert.assertEquals(a, list.poll());
      Assert.assertEquals(b, list.poll());
      Assert.assertEquals(c, list.poll());
      Assert.assertEquals(d, list.poll());
      Assert.assertEquals(e, list.poll());
      Assert.assertEquals(f, list.poll());
      Assert.assertEquals(g, list.poll());
      Assert.assertEquals(h, list.poll());
      Assert.assertEquals(i, list.poll());
      Assert.assertEquals(j, list.poll());

      Assert.assertNull(list.poll());

      list.addTail(a, 9);
      list.addTail(b, 0);
      list.addTail(c, 8);
      list.addTail(d, 1);
      list.addTail(e, 7);
      list.addTail(f, 2);
      list.addTail(g, 6);
      list.addTail(h, 3);
      list.addTail(i, 5);
      list.addTail(j, 4);

      Assert.assertEquals(a, list.poll());
      Assert.assertEquals(c, list.poll());
      Assert.assertEquals(e, list.poll());
      Assert.assertEquals(g, list.poll());
      Assert.assertEquals(i, list.poll());
      Assert.assertEquals(j, list.poll());
      Assert.assertEquals(h, list.poll());
      Assert.assertEquals(f, list.poll());
      Assert.assertEquals(d, list.poll());
      Assert.assertEquals(b, list.poll());

      Assert.assertNull(list.poll());

      list.addTail(a, 0);
      list.addTail(b, 3);
      list.addTail(c, 3);
      list.addTail(d, 3);
      list.addTail(e, 6);
      list.addTail(f, 6);
      list.addTail(g, 6);
      list.addTail(h, 9);
      list.addTail(i, 9);
      list.addTail(j, 9);

      Assert.assertEquals(h, list.poll());
      Assert.assertEquals(i, list.poll());
      Assert.assertEquals(j, list.poll());
      Assert.assertEquals(e, list.poll());
      Assert.assertEquals(f, list.poll());
      Assert.assertEquals(g, list.poll());
      Assert.assertEquals(b, list.poll());
      Assert.assertEquals(c, list.poll());
      Assert.assertEquals(d, list.poll());
      Assert.assertEquals(a, list.poll());

      Assert.assertNull(list.poll());

      list.addTail(a, 5);
      list.addTail(b, 5);
      list.addTail(c, 5);
      list.addTail(d, 5);
      list.addTail(e, 5);
      list.addTail(f, 5);
      list.addTail(g, 5);
      list.addTail(h, 5);
      list.addTail(i, 5);
      list.addTail(j, 5);

      Assert.assertEquals(a, list.poll());
      Assert.assertEquals(b, list.poll());
      Assert.assertEquals(c, list.poll());
      Assert.assertEquals(d, list.poll());
      Assert.assertEquals(e, list.poll());
      Assert.assertEquals(f, list.poll());
      Assert.assertEquals(g, list.poll());
      Assert.assertEquals(h, list.poll());
      Assert.assertEquals(i, list.poll());
      Assert.assertEquals(j, list.poll());

      Assert.assertNull(list.poll());

      list.addTail(j, 5);
      list.addTail(i, 5);
      list.addTail(h, 5);
      list.addTail(g, 5);
      list.addTail(f, 5);
      list.addTail(e, 5);
      list.addTail(d, 5);
      list.addTail(c, 5);
      list.addTail(b, 5);
      list.addTail(a, 5);

      Assert.assertEquals(j, list.poll());
      Assert.assertEquals(i, list.poll());
      Assert.assertEquals(h, list.poll());
      Assert.assertEquals(g, list.poll());
      Assert.assertEquals(f, list.poll());
      Assert.assertEquals(e, list.poll());
      Assert.assertEquals(d, list.poll());
      Assert.assertEquals(c, list.poll());
      Assert.assertEquals(b, list.poll());
      Assert.assertEquals(a, list.poll());

      Assert.assertNull(list.poll());
      
      assertEquals(0, list.size());

   }

   public void testIterator()
   {
      list.addTail(a, 9);
      list.addTail(b, 9);
      list.addTail(c, 8);
      list.addTail(d, 8);
      list.addTail(e, 7);
      list.addTail(f, 7);
      list.addTail(g, 7);
      list.addTail(h, 6);
      list.addTail(i, 6);
      list.addTail(j, 6);
      list.addTail(k, 5);
      list.addTail(l, 5);
      list.addTail(m, 4);
      list.addTail(n, 4);
      list.addTail(o, 4);
      list.addTail(p, 3);
      list.addTail(q, 3);
      list.addTail(r, 3);
      list.addTail(s, 2);
      list.addTail(t, 2);
      list.addTail(u, 2);
      list.addTail(v, 1);
      list.addTail(w, 1);
      list.addTail(x, 1);
      list.addTail(y, 0);
      list.addTail(z, 0);

      LinkedListIterator<Wibble> iter = list.iterator();

      int count = 0;
      Wibble w;
      while (iter.hasNext())
      {
         w = iter.next();
         count++;
      }
      Assert.assertEquals(26, count);
      Assert.assertEquals(26, list.size());

      iter = list.iterator();

      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      assertTrue(iter.hasNext());
      Assert.assertEquals("a", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      assertTrue(iter.hasNext());
      Assert.assertEquals("b", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("c", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("d", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("e", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("f", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("g", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("h", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("i", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("j", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("k", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("l", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("m", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("n", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("o", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("p", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("q", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("r", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("s", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("t", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("u", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("v", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("w", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("x", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("y", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("z", w.s);
      assertFalse(iter.hasNext());
      
      
      iter = list.iterator();
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("a", w.s);

      iter.remove();

      Assert.assertEquals(25, list.size());

      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("b", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("c", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("d", w.s);

      iter.remove();

      Assert.assertEquals(24, list.size());

      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("c", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("e", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("f", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("g", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("h", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("i", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("j", w.s);

      iter.remove();

      Assert.assertEquals(23, list.size());
      
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("i", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("k", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("l", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("m", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("n", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("o", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("p", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("q", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("r", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("s", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("t", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("u", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("v", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("w", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("x", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("y", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("z", w.s);
      iter.remove();


      iter = list.iterator();
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("b", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("c", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("e", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("f", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("g", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("h", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("i", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("k", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("l", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("m", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("n", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("o", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("p", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("q", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("r", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("s", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("t", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("u", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("v", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("w", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("x", w.s);
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("y", w.s);
      
      assertFalse(iter.hasNext());
      assertFalse(iter.hasNext());

      //Test the elements added after iter created are seen
      
      list.addTail(a, 4);
      list.addTail(b, 4);
      
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("a", w.s);
      
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("b", w.s);
      
      assertFalse(iter.hasNext());      
      
      list.addTail(c, 4);
      list.addTail(d, 4);
      
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("c", w.s);
      
      assertTrue(iter.hasNext());
      w = (Wibble)iter.next();
      Assert.assertEquals("d", w.s);
      
      assertFalse(iter.hasNext());
      

   }
   
   public void testIteratorPicksUpHigherPriorities()
   {
      list.addTail(a, 4);
      list.addTail(b, 4);
      list.addTail(c, 4);
     
      LinkedListIterator<Wibble> iter = list.iterator();
      
      assertTrue(iter.hasNext());
      assertEquals(a, iter.next());
      
      assertTrue(iter.hasNext());
      assertEquals(b, iter.next());
      
      list.addTail(d, 5);
      list.addTail(e, 5);
      
      assertTrue(iter.hasNext());
      assertEquals(d, iter.next());
      
      assertTrue(iter.hasNext());
      assertEquals(e, iter.next());
      
      assertTrue(iter.hasNext());
      assertEquals(c, iter.next());
      
      list.addTail(f, 1);
      list.addTail(g, 9);
      
      assertTrue(iter.hasNext());
      assertEquals(g, iter.next());
      
      assertTrue(iter.hasNext());
      assertEquals(f, iter.next());
   }


   public void testClear()
   {
      list.addTail(a, 0);
      list.addTail(b, 3);
      list.addTail(c, 3);
      list.addTail(d, 3);
      list.addTail(e, 6);
      list.addTail(f, 6);
      list.addTail(g, 6);
      list.addTail(h, 9);
      list.addTail(i, 9);
      list.addTail(j, 9);

      list.clear();

      Assert.assertNull(list.poll());
   }

   class Wibble
   {
      String s;

      Wibble(final String s)
      {
         this.s = s;
      }

      public String toString()
      {
         return s;
      }
   }

}
