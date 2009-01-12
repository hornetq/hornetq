/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.tests.unit.core.postoffice.impl;

import org.jboss.messaging.core.postoffice.Address;
import org.jboss.messaging.core.postoffice.impl.AddressImpl;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class AddressImplTest  extends UnitTestCase
{
   public void testNoDots()
   {
      SimpleString s1 = new SimpleString("abcde");
      SimpleString s2 = new SimpleString("abcde");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      assertTrue(a1.matches(a2));
   }

   public void testDotsSameLength2()
   {
      SimpleString s1 = new SimpleString("a.b");
      SimpleString s2 = new SimpleString("a.b");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      assertTrue(a1.matches(a2));
   }

   public void testA()
   {
      SimpleString s1 = new SimpleString("a.b.c");
      SimpleString s2 = new SimpleString("a.b.c.d.e.f.g.h.i.j.k.l.m.n.*");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      assertFalse(a1.matches(a2));
   }

   public void testB()
   {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s2 = new SimpleString("a.b.x.e");
      SimpleString s3 = new SimpleString("a.b.c.*");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));
   }

   public void testC()
   {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s2 = new SimpleString("a.b.c.x");
      SimpleString s3 = new SimpleString("a.b.*.d");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));
   }

   public void testD()
   {
      SimpleString s1 = new SimpleString("a.b.c.d.e");
      SimpleString s2 = new SimpleString("a.b.c.x.e");
      SimpleString s3 = new SimpleString("a.b.*.d.*");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));
   }

   public void testE()
   {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("a.b.*.d.*.f");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));
   }

   public void testF()
   {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));
   }

   public void testG()
   {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("a.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));
   }

   public void testH()
   {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("#.b.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));
   }

   public void testI()
   {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("a.#.b.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));
   }

   public void testJ()
   {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("a.#.c.d.e.f");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));
   }

   public void testK()
   {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.d.e.x");
      SimpleString s3 = new SimpleString("a.#.c.d.e.*");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));
   }

   public void testL()
   {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.d.e.x");
      SimpleString s3 = new SimpleString("a.#.c.d.*.f");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));
   }

   public void testM()
   {
      SimpleString s1 = new SimpleString("a.b.c");
      SimpleString s2 = new SimpleString("a.b.x.e");
      SimpleString s3 = new SimpleString("a.b.c.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));
   }

   public void testN()
   {
      SimpleString s1 = new SimpleString("usd.stock");
      SimpleString s2 = new SimpleString("a.b.x.e");
      SimpleString s3 = new SimpleString("*.stock.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));
   }

   public void testO()
   {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s2 = new SimpleString("a.b.x.e");
      SimpleString s3 = new SimpleString("a.b.c.*");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));
   }

   public void testP()
   {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("a.b.c#");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));
   }

   public void testQ()
   {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("#a.b.c");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));
   }

    public void testR()
   {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("#*a.b.c");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));
   }

   public void testS()
   {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("a.b.c*");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));
   }

   public void testT()
   {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("*a.b.c");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));
   }

   public void testU()
   {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("*a.b.c");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));
   }

}
