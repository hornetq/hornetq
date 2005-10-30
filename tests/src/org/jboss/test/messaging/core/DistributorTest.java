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
package org.jboss.test.messaging.core;

import org.jboss.test.messaging.MessagingTestCase;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class DistributorTest extends MessagingTestCase
{
//   // Attributes ----------------------------------------------------
//
//   protected Distributor distributor;

   // Constructors --------------------------------------------------

   public DistributorTest(String name)
   {
      super(name);
   }

//   //
//   // Distributor tests
//   //
//
//   public void testAddOneReceiver()
//   {
//      if (distributor == null) { return; }
//
//      Receiver r = new ReceiverImpl("ReceiverID1", ReceiverImpl.HANDLING);
//      assertTrue(distributor.add(r));
//      assertFalse(distributor.add(r));
//      assertTrue(distributor.contains("ReceiverID1"));
//      assertTrue(r == distributor.get("ReceiverID1"));
//      Iterator i = distributor.iterator();
//      assertEquals("ReceiverID1", i.next());
//      assertFalse(i.hasNext());
//      distributor.clear();
//      i = distributor.iterator();
//      assertFalse(i.hasNext());
//
//   }
//
//   public void testAddMultipleReceivers()
//   {
//      if (distributor == null) { return; }
//
//      Receiver r1 = new ReceiverImpl("ReceiverID1", ReceiverImpl.HANDLING);
//      Receiver r2 = new ReceiverImpl("ReceiverID2", ReceiverImpl.HANDLING);
//      assertTrue(distributor.add(r1));
//      assertTrue(distributor.add(r2));
//      assertTrue(distributor.contains("ReceiverID1"));
//      assertTrue(distributor.contains("ReceiverID2"));
//      assertTrue(r1 == distributor.get("ReceiverID1"));
//      assertTrue(r2 == distributor.get("ReceiverID2"));
//      distributor.clear();
//      Iterator i = distributor.iterator();
//      assertFalse(i.hasNext());
//   }
//
//   public void testRemoveInexistentRouter()
//   {
//      if (distributor == null) { return; }
//
//      assertNull(distributor.remove("NoSuchId"));
//
//   }
//
//   public void testVariousAdds()
//   {
//      if (distributor == null) { return; }
//
//      assertFalse(distributor.iterator().hasNext());
//
//      ReceiverImpl r = new ReceiverImpl();
//      assertTrue(distributor.add(r));
//
//      if (distributor instanceof Receiver)
//      {
//         boolean result = ((Receiver)distributor).handle(new MessageSupport(""));
//         assertTrue(result);
//      }
//      else if (distributor instanceof Router)
//      {
//         Set acks = ((Router)distributor).handle(new MessageSupport(""));
//         assertEquals(1, acks.size());
//         assertTrue(((Acknowledgment)acks.iterator().next()).isPositive());
//      }
//
//      assertFalse(distributor.add(r));
//   }
//
//
//   /**
//    * This method assumes the router has no associates receivers.
//    */
//   public void testVariousRemoves()
//   {
//      if (distributor == null) { return; }
//
//      assertFalse(distributor.iterator().hasNext());
//
//      ReceiverImpl r = new ReceiverImpl();
//      assertTrue(distributor.add(r));
//      Iterator i = distributor.iterator();
//      assertEquals(r.getReceiverID(), i.next());
//      assertFalse(i.hasNext());
//
//      assertTrue(r == distributor.remove(r.getReceiverID()));
//
//      // make sure that removing an inexistent Receiver returns false
//      assertNull(distributor.remove(r.getReceiverID()));
//
//      i = distributor.iterator();
//      assertFalse(i.hasNext());
//   }

   public void testNoop()
   {
   }
}


