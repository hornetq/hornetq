/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Distributor;
import org.jboss.messaging.core.message.MessageSupport;
import org.jboss.messaging.core.Distributor;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.message.MessageSupport;

import java.util.Iterator;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class DistributorTest extends MessagingTestCase
{
   // Attributes ----------------------------------------------------

   protected Distributor distributor;

   // Constructors --------------------------------------------------

   public DistributorTest(String name)
   {
      super(name);
   }

   //
   // Distributor tests
   //

   public void testAddOneReceiver()
   {
      if (distributor == null) { return; }

      Receiver r = new ReceiverImpl("ReceiverID1", ReceiverImpl.HANDLING);
      assertTrue(distributor.add(r));
      assertFalse(distributor.add(r));
      assertTrue(distributor.contains("ReceiverID1"));
      assertTrue(r == distributor.get("ReceiverID1"));
      Iterator i = distributor.iterator();
      assertEquals("ReceiverID1", i.next());
      assertFalse(i.hasNext());
      distributor.clear();
      i = distributor.iterator();
      assertFalse(i.hasNext());

   }

   public void testAddMultipleReceivers()
   {
      if (distributor == null) { return; }

      Receiver r1 = new ReceiverImpl("ReceiverID1", ReceiverImpl.HANDLING);
      Receiver r2 = new ReceiverImpl("ReceiverID2", ReceiverImpl.HANDLING);
      assertTrue(distributor.add(r1));
      assertTrue(distributor.add(r2));
      assertTrue(distributor.contains("ReceiverID1"));
      assertTrue(distributor.contains("ReceiverID2"));
      assertTrue(r1 == distributor.get("ReceiverID1"));
      assertTrue(r2 == distributor.get("ReceiverID2"));
      distributor.clear();
      Iterator i = distributor.iterator();
      assertFalse(i.hasNext());
   }

   public void testRemoveInexistentRouter()
   {
      if (distributor == null) { return; }

      assertNull(distributor.remove("NoSuchId"));

   }

   public void testVariousAdds()
   {
      if (distributor == null) { return; }

      assertFalse(distributor.iterator().hasNext());

      ReceiverImpl r = new ReceiverImpl();
      assertTrue(distributor.add(r));
      assertFalse(distributor.acknowledged(r.getReceiverID()));

      assertTrue(((Receiver)distributor).handle(new MessageSupport("")));
      assertTrue(distributor.acknowledged(r.getReceiverID()));

      // make sure that a spurious add attempt does not change the status of the last handle() call
      assertFalse(distributor.add(r));
      assertTrue(distributor.acknowledged(r.getReceiverID()));
   }


   /**
    * This method assumes the router has no associates receivers.
    */
   public void testVariousRemoves()
   {
      if (distributor == null) { return; }
      
      assertFalse(distributor.iterator().hasNext());

      ReceiverImpl r = new ReceiverImpl();
      assertTrue(distributor.add(r));
      Iterator i = distributor.iterator();
      assertEquals(r.getReceiverID(), i.next());
      assertFalse(i.hasNext());

      assertTrue(r == distributor.remove(r.getReceiverID()));

      // make sure that removing an inexistent Receiver returns false
      assertNull(distributor.remove(r.getReceiverID()));

      i = distributor.iterator();
      assertFalse(i.hasNext());
   }
}


