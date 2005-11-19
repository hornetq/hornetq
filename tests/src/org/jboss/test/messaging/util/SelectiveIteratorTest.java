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
package org.jboss.test.messaging.util;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.util.SelectiveIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class SelectiveIteratorTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public SelectiveIteratorTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testHasNext1()
   {
      List l = new ArrayList();
      l.add("a");
      l.add(new Integer(1));
      l.add("b");

      SelectiveIterator si = new SelectiveIterator(l.iterator(), Integer.class);

      assertTrue(si.hasNext());
      assertEquals("a", si.next());
      assertTrue(si.hasNext());
      assertEquals("b", si.next());
      assertFalse(si.hasNext());
   }

   public void testHasNext2()
   {
      List l = new ArrayList();
      l.add("a");
      l.add("b");
      l.add("c");

      SelectiveIterator si = new SelectiveIterator(l.iterator(), String.class);

      assertFalse(si.hasNext());
   }

   public void testRemove()
   {
      SelectiveIterator si =
         new SelectiveIterator(Collections.EMPTY_LIST.iterator(), String.class);

      try
      {
         si.remove();
         fail("If you implement remove(), provide tests");
      }
      catch(UnsupportedOperationException e)
      {

      }
   }

}
