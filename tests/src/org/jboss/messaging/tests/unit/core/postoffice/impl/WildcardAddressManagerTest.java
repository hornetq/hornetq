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

import org.jboss.messaging.core.postoffice.impl.WildcardAddressManager;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class WildcardAddressManagerTest extends SimpleAddressManagerTest
{
   protected void setUp() throws Exception
   {
      sam = new WildcardAddressManager();
   }

   
   
   public void testAddDestinations()
   {
      /*SimpleString address = new SimpleString("test.add1");
      SimpleString address2 = new SimpleString("test.add2");
      SimpleString address3 = new SimpleString("test.add3");
      SimpleString address4 = new SimpleString("test.add4");
      SimpleString address5 = new SimpleString("test.add5");
      sam.addDestination(address);
      sam.addDestination(address2);
      sam.addDestination(address3);
      sam.addDestination(address4);
      sam.addDestination(address5);
      assertTrue(sam.containsDestination(address));
      assertTrue(sam.containsDestination(address2));
      assertTrue(sam.containsDestination(address3));
      assertTrue(sam.containsDestination(address4));
      assertTrue(sam.containsDestination(address5));
      assertTrue(sam.containsDestination(new SimpleString("test.*")));
      assertTrue(sam.containsDestination(new SimpleString("*.*")));
      assertTrue(sam.containsDestination(new SimpleString("*.add1")));
      assertTrue(sam.containsDestination(new SimpleString("*.add2")));
      assertTrue(sam.containsDestination(new SimpleString("*.add3")));
      assertTrue(sam.containsDestination(new SimpleString("*.add4")));
      assertTrue(sam.containsDestination(new SimpleString("*.add5")));
      assertTrue(sam.containsDestination(new SimpleString("test.#")));
      assertTrue(sam.containsDestination(new SimpleString("#.add1")));
      assertTrue(sam.containsDestination(new SimpleString("#.add2")));
      assertTrue(sam.containsDestination(new SimpleString("#.add3")));
      assertTrue(sam.containsDestination(new SimpleString("#.add4")));
      assertTrue(sam.containsDestination(new SimpleString("#.add5")));
      assertTrue(sam.containsDestination(new SimpleString("#")));
      assertEquals(sam.getDestinations().size(), 5);*/
   }

 /*  public void testRemoveDestinations()
   {
      SimpleString address = new SimpleString("test.add1");
      SimpleString address2 = new SimpleString("test2.add2");
      SimpleString address3 = new SimpleString("test.add3");
      SimpleString address4 = new SimpleString("test2.add4");
      SimpleString address5 = new SimpleString("test.add5");
      sam.addDestination(address);
      sam.addDestination(address2);
      sam.addDestination(address3);
      sam.addDestination(address4);
      sam.addDestination(address5);
      assertTrue(sam.containsDestination(address));
      assertTrue(sam.containsDestination(address2));
      assertTrue(sam.containsDestination(address3));
      assertTrue(sam.containsDestination(address4));
      assertTrue(sam.containsDestination(address5));
      assertTrue(sam.containsDestination(new SimpleString("test.*")));
      assertTrue(sam.containsDestination(new SimpleString("test2.*")));
      assertTrue(sam.containsDestination(new SimpleString("*.*")));
      assertTrue(sam.containsDestination(new SimpleString("*.add1")));
      assertTrue(sam.containsDestination(new SimpleString("*.add2")));
      assertTrue(sam.containsDestination(new SimpleString("*.add3")));
      assertTrue(sam.containsDestination(new SimpleString("*.add4")));
      assertTrue(sam.containsDestination(new SimpleString("*.add5")));
      assertTrue(sam.containsDestination(new SimpleString("*.add5")));
      assertTrue(sam.containsDestination(new SimpleString("test.#")));
      assertTrue(sam.containsDestination(new SimpleString("#.add1")));
      assertTrue(sam.containsDestination(new SimpleString("#.add2")));
      assertTrue(sam.containsDestination(new SimpleString("#.add3")));
      assertTrue(sam.containsDestination(new SimpleString("#.add4")));
      assertTrue(sam.containsDestination(new SimpleString("#.add5")));
      assertTrue(sam.containsDestination(new SimpleString("#")));
      assertEquals(sam.getDestinations().size(), 5);

      sam.removeDestination(address2);
      sam.removeDestination(address4);

      assertTrue(sam.containsDestination(address));
      assertFalse(sam.containsDestination(address2));
      assertTrue(sam.containsDestination(address3));
      assertFalse(sam.containsDestination(address4));
      assertTrue(sam.containsDestination(address5));
      assertTrue(sam.containsDestination(new SimpleString("test.*")));
      assertFalse(sam.containsDestination(new SimpleString("test2.*")));
      assertTrue(sam.containsDestination(new SimpleString("*.*")));
      assertTrue(sam.containsDestination(new SimpleString("*.add1")));
      assertFalse(sam.containsDestination(new SimpleString("*.add2")));
      assertTrue(sam.containsDestination(new SimpleString("*.add3")));
      assertFalse(sam.containsDestination(new SimpleString("*.add4")));
      assertTrue(sam.containsDestination(new SimpleString("*.add5")));
      assertTrue(sam.containsDestination(new SimpleString("*.add5")));
      assertTrue(sam.containsDestination(new SimpleString("test.#")));
      assertTrue(sam.containsDestination(new SimpleString("#.add1")));
      assertFalse(sam.containsDestination(new SimpleString("#.add2")));
      assertTrue(sam.containsDestination(new SimpleString("#.add3")));
      assertFalse(sam.containsDestination(new SimpleString("#.add4")));
      assertTrue(sam.containsDestination(new SimpleString("#.add5")));
      assertTrue(sam.containsDestination(new SimpleString("#")));
      assertEquals(sam.getDestinations().size(), 3);
   }*/
}
