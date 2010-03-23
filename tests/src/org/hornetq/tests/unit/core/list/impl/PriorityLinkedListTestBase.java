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

import org.hornetq.utils.PriorityLinkedListImpl;
import org.hornetq.utils.concurrent.HQIterator;

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

      list.addFirst(a, 0);

      Assert.assertFalse(list.isEmpty());

      Wibble w = list.removeFirst();
      Assert.assertEquals(a, w);
      Assert.assertTrue(list.isEmpty());
   }

   public void testAddFirst() throws Exception
   {
      list.addFirst(a, 0);
      list.addFirst(b, 0);
      list.addFirst(c, 0);
      list.addFirst(d, 0);
      list.addFirst(e, 0);

      Assert.assertEquals(e, list.removeFirst());
      Assert.assertEquals(d, list.removeFirst());
      Assert.assertEquals(c, list.removeFirst());
      Assert.assertEquals(b, list.removeFirst());
      Assert.assertEquals(a, list.removeFirst());
      Assert.assertNull(list.removeFirst());
   }

   public void testAddLast() throws Exception
   {
      list.addLast(a, 0);
      list.addLast(b, 0);
      list.addLast(c, 0);
      list.addLast(d, 0);
      list.addLast(e, 0);

      Assert.assertEquals(a, list.removeFirst());
      Assert.assertEquals(b, list.removeFirst());
      Assert.assertEquals(c, list.removeFirst());
      Assert.assertEquals(d, list.removeFirst());
      Assert.assertEquals(e, list.removeFirst());
      Assert.assertNull(list.removeFirst());

   }
   
   public void testAddLastAndFirst() throws Exception
   {
      list.addLast(a, 0);
      list.addLast(b, 0);
      list.addLast(c, 0);
      list.addLast(d, 0);
      list.addLast(e, 0);
      list.addLast(f, 0);
      list.addLast(g, 0);
      list.addLast(h, 0);
      list.addLast(i, 0);
      list.addLast(j, 0);
      
      list.addFirst(k, 0);
      list.addFirst(l, 0);
      list.addFirst(m, 0);
      list.addFirst(n, 0);
      list.addFirst(o, 0);
      list.addFirst(p, 0);
      list.addFirst(q, 0);
      list.addFirst(r, 0);
      list.addFirst(s, 0);
      list.addFirst(t, 0);
      
      assertEquals(t, list.removeFirst());
      assertEquals(s, list.removeFirst());
      assertEquals(r, list.removeFirst());
      assertEquals(q, list.removeFirst());
      assertEquals(p, list.removeFirst());
      assertEquals(o, list.removeFirst());
      assertEquals(n, list.removeFirst());
      assertEquals(m, list.removeFirst());
      assertEquals(l, list.removeFirst());
      assertEquals(k, list.removeFirst());
      
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
   }
   
   public void testAddLastAndFirstWithIterator() throws Exception
   {
      list.addLast(a, 0);
      list.addLast(b, 0);
      list.addLast(c, 0);
      list.addLast(d, 0);
      list.addLast(e, 0);
      list.addLast(f, 0);
      list.addLast(g, 0);
      list.addLast(h, 0);
      list.addLast(i, 0);
      list.addLast(j, 0);
      
      list.addFirst(k, 0);
      list.addFirst(l, 0);
      list.addFirst(m, 0);
      list.addFirst(n, 0);
      list.addFirst(o, 0);
      list.addFirst(p, 0);
      list.addFirst(q, 0);
      list.addFirst(r, 0);
      list.addFirst(s, 0);
      list.addFirst(t, 0);
      
      HQIterator<Wibble> iter = list.iterator();
           
      assertEquals(t, iter.next());
      assertEquals(s, iter.next());
      assertEquals(r, iter.next());
      assertEquals(q, iter.next());
      assertEquals(p, iter.next());
      assertEquals(o, iter.next());
      assertEquals(n, iter.next());
      assertEquals(m, iter.next());
      assertEquals(l, iter.next());
      assertEquals(k, iter.next());
 
      
      assertEquals(a, iter.next());
      assertEquals(b, iter.next());
      assertEquals(c, iter.next());
      assertEquals(d, iter.next());
      assertEquals(e, iter.next());
      assertEquals(f, iter.next());
      assertEquals(g, iter.next());
      assertEquals(h, iter.next());
      assertEquals(i, iter.next());
      assertEquals(j, iter.next());
   }

   public void testPeekFirst()
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

      Assert.assertEquals(j, list.peekFirst());
      Assert.assertEquals(j, list.peekFirst());

      list.removeFirst();

      Assert.assertEquals(i, list.peekFirst());
      Assert.assertEquals(i, list.peekFirst());

      list.clear();
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

      Assert.assertEquals(j, list.removeFirst());
      Assert.assertEquals(i, list.removeFirst());
      Assert.assertEquals(h, list.removeFirst());
      Assert.assertEquals(g, list.removeFirst());
      Assert.assertEquals(f, list.removeFirst());
      Assert.assertEquals(e, list.removeFirst());
      Assert.assertEquals(d, list.removeFirst());
      Assert.assertEquals(c, list.removeFirst());
      Assert.assertEquals(b, list.removeFirst());
      Assert.assertEquals(a, list.removeFirst());

      Assert.assertNull(list.removeFirst());

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

      Assert.assertEquals(a, list.removeFirst());
      Assert.assertEquals(b, list.removeFirst());
      Assert.assertEquals(c, list.removeFirst());
      Assert.assertEquals(d, list.removeFirst());
      Assert.assertEquals(e, list.removeFirst());
      Assert.assertEquals(f, list.removeFirst());
      Assert.assertEquals(g, list.removeFirst());
      Assert.assertEquals(h, list.removeFirst());
      Assert.assertEquals(i, list.removeFirst());
      Assert.assertEquals(j, list.removeFirst());

      Assert.assertNull(list.removeFirst());

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

      Assert.assertEquals(a, list.removeFirst());
      Assert.assertEquals(c, list.removeFirst());
      Assert.assertEquals(e, list.removeFirst());
      Assert.assertEquals(g, list.removeFirst());
      Assert.assertEquals(i, list.removeFirst());
      Assert.assertEquals(j, list.removeFirst());
      Assert.assertEquals(h, list.removeFirst());
      Assert.assertEquals(f, list.removeFirst());
      Assert.assertEquals(d, list.removeFirst());
      Assert.assertEquals(b, list.removeFirst());

      Assert.assertNull(list.removeFirst());

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

      Assert.assertEquals(h, list.removeFirst());
      Assert.assertEquals(i, list.removeFirst());
      Assert.assertEquals(j, list.removeFirst());
      Assert.assertEquals(e, list.removeFirst());
      Assert.assertEquals(f, list.removeFirst());
      Assert.assertEquals(g, list.removeFirst());
      Assert.assertEquals(b, list.removeFirst());
      Assert.assertEquals(c, list.removeFirst());
      Assert.assertEquals(d, list.removeFirst());
      Assert.assertEquals(a, list.removeFirst());

      Assert.assertNull(list.removeFirst());

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

      Assert.assertEquals(a, list.removeFirst());
      Assert.assertEquals(b, list.removeFirst());
      Assert.assertEquals(c, list.removeFirst());
      Assert.assertEquals(d, list.removeFirst());
      Assert.assertEquals(e, list.removeFirst());
      Assert.assertEquals(f, list.removeFirst());
      Assert.assertEquals(g, list.removeFirst());
      Assert.assertEquals(h, list.removeFirst());
      Assert.assertEquals(i, list.removeFirst());
      Assert.assertEquals(j, list.removeFirst());

      Assert.assertNull(list.removeFirst());

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

      Assert.assertEquals(j, list.removeFirst());
      Assert.assertEquals(i, list.removeFirst());
      Assert.assertEquals(h, list.removeFirst());
      Assert.assertEquals(g, list.removeFirst());
      Assert.assertEquals(f, list.removeFirst());
      Assert.assertEquals(e, list.removeFirst());
      Assert.assertEquals(d, list.removeFirst());
      Assert.assertEquals(c, list.removeFirst());
      Assert.assertEquals(b, list.removeFirst());
      Assert.assertEquals(a, list.removeFirst());

      Assert.assertNull(list.removeFirst());

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

      HQIterator<Wibble> iter = list.iterator();

      int c = 0;
      Wibble w;
      while ((w = iter.next()) != null)
      {
         c++;
      }
      Assert.assertEquals(c, 26);
      Assert.assertEquals(26, list.size());

      iter = list.iterator();

      w = (Wibble)iter.next();
      Assert.assertEquals("a", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("b", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("c", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("d", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("e", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("f", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("g", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("h", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("i", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("j", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("k", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("l", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("m", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("n", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("o", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("p", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("q", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("r", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("s", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("t", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("u", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("v", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("w", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("x", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("y", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("z", w.s);
      Assert.assertNull(iter.next());

      iter = list.iterator();
      w = (Wibble)iter.next();
      Assert.assertEquals("a", w.s);

      iter.remove();

      Assert.assertEquals(25, list.size());

      w = (Wibble)iter.next();
      Assert.assertEquals("b", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("c", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("d", w.s);

      iter.remove();

      Assert.assertEquals(24, list.size());

      w = (Wibble)iter.next();
      Assert.assertEquals("e", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("f", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("g", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("h", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("i", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("j", w.s);

      iter.remove();

      Assert.assertEquals(23, list.size());

      w = (Wibble)iter.next();
      Assert.assertEquals("k", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("l", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("m", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("n", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("o", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("p", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("q", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("r", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("s", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("t", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("u", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("v", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("w", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("x", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("y", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("z", w.s);
      iter.remove();
      Assert.assertNull(iter.next());

      iter = list.iterator();
      w = (Wibble)iter.next();
      Assert.assertEquals("b", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("c", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("e", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("f", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("g", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("h", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("i", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("k", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("l", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("m", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("n", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("o", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("p", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("q", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("r", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("s", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("t", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("u", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("v", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("w", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("x", w.s);
      w = (Wibble)iter.next();
      Assert.assertEquals("y", w.s);
      Assert.assertNull(iter.next());
      
      //We test again - should still be null (there was a bug here)
      Assert.assertNull(iter.next());

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

      Assert.assertNull(list.removeFirst());
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
