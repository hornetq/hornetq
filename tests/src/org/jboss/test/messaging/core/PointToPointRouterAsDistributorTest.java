/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Acknowledgment;
import org.jboss.messaging.core.local.PointToPointRouter;
import org.jboss.messaging.core.message.MessageSupport;

import java.util.Iterator;
import java.util.Set;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class PointToPointRouterAsDistributorTest extends DistributorTest
{
   // Constructors --------------------------------------------------

   public PointToPointRouterAsDistributorTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      super.setUp();

      // Create a PointToMultipointRouter to be tested by the superclass tests
      distributor = new PointToPointRouter("P2PRouterID");
   }

   public void tearDown()throws Exception
   {
      distributor.clear();
      distributor = null;
      super.tearDown();
   }

   public void testPointToPointRouter() throws Exception
   {
      PointToPointRouter router = new PointToPointRouter("");

      // send without a receiver

      Routable r = new MessageSupport(new Integer(0));
      Set result = router.handle(r);
      assertEquals(0, result.size());

      // send with one receiver

      ReceiverImpl rOne = new ReceiverImpl("ONE", ReceiverImpl.HANDLING);
      assertTrue(router.add(rOne));

      r = new MessageSupport(new Integer(1));
      result = router.handle(r);
      assertEquals(1, result.size());
      Acknowledgment a = (Acknowledgment)result.iterator().next();
      assertTrue(a.isPositive());
      assertEquals("ONE", a.getReceiverID());

      Iterator i = rOne.iterator();
      r = (Routable)i.next();
      assertFalse(i.hasNext());
      assertEquals(new Integer(1), r.getMessageID());

      rOne.clear();

      // send with two receivers

      ReceiverImpl rTwo = new ReceiverImpl("TWO", ReceiverImpl.HANDLING);
      assertTrue(router.add(rTwo));

      r = new MessageSupport(new Integer(2));
      result = router.handle(r);
      assertEquals(1, result.size());
      a = (Acknowledgment)result.iterator().next();
      assertTrue(a.isPositive());

      Iterator iOne = rOne.iterator(), iTwo = rTwo.iterator();
      if (iOne.hasNext())
      {
         // then rOne got the message
         r = (Routable)iOne.next();
         assertFalse(iOne.hasNext());
         assertEquals(new Integer(2), r.getMessageID());
         assertFalse(iOne.hasNext());
      }
      else
      {
         // otherwise rTwo got the message
         r = (Routable)iTwo.next();
         assertFalse(iTwo.hasNext());
         assertEquals(new Integer(2), r.getMessageID());
         assertFalse(iOne.hasNext());
      }
   }


   public void testNackingReceiver() throws Exception
   {
      PointToPointRouter router = new PointToPointRouter("");

      ReceiverImpl denying = new ReceiverImpl("NackingID", ReceiverImpl.NACKING);
      assertTrue(router.add(denying));

      Routable r = new MessageSupport("");
      Set result = router.handle(r);
      assertEquals(1, result.size());
      Acknowledgment a = (Acknowledgment)result.iterator().next();
      assertTrue(a.isNegative());
      assertEquals("NackingID", a.getReceiverID());

      Iterator i = denying.iterator();
      assertFalse(i.hasNext());

      ReceiverImpl handling = new ReceiverImpl("HandlingID", ReceiverImpl.HANDLING);
      assertTrue(router.add(handling));
      result = router.handle(r);

      assertEquals(1, result.size());
      a = (Acknowledgment)result.iterator().next();
      assertTrue(a.isNegative());
      assertEquals("NackingID", a.getReceiverID());

      i = denying.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      assertFalse(i.hasNext());
   }

   public void testBrokenReceiver() throws Exception
   {
      PointToPointRouter router = new PointToPointRouter("");

      // one broken receiver

      ReceiverImpl broken1 = new ReceiverImpl("BrokenID1", ReceiverImpl.BROKEN);
      assertTrue(router.add(broken1));

      Routable r = new MessageSupport("");
      Set acks = router.handle(r);
      assertEquals(0, acks.size());

      assertFalse(broken1.iterator().hasNext());

      // two broken receivers

      ReceiverImpl broken2 = new ReceiverImpl("BrokenID2", ReceiverImpl.BROKEN);
      assertTrue(router.add(broken2));

      acks = router.handle(r);
      assertEquals(0, acks.size());

      assertFalse(broken1.iterator().hasNext());
      assertFalse(broken2.iterator().hasNext());

      // two broken receivers and a nacking receiver

      ReceiverImpl nacking = new ReceiverImpl("NackingID", ReceiverImpl.NACKING);
      assertTrue(router.add(nacking));

      Set result = router.handle(r);
      assertEquals(1, result.size());
      Acknowledgment a = (Acknowledgment)result.iterator().next();
      assertTrue(a.isNegative());
      assertEquals("NackingID", a.getReceiverID());

      assertFalse(broken1.iterator().hasNext());
      assertFalse(broken2.iterator().hasNext());
      assertFalse(nacking.iterator().hasNext());

      // two broken receivers and a handling receiver
      assertEquals(nacking, router.remove("NackingID"));

      ReceiverImpl handling = new ReceiverImpl("HandlingID", ReceiverImpl.HANDLING);
      assertTrue(router.add(handling));

      result = router.handle(r);
      assertEquals(1, result.size());
      a = (Acknowledgment)result.iterator().next();
      assertTrue(a.isPositive());
      assertEquals("HandlingID", a.getReceiverID());

      assertFalse(broken1.iterator().hasNext());
      assertFalse(broken2.iterator().hasNext());

      Iterator i = handling.iterator();
      r = (Routable)i.next();
      assertFalse(i.hasNext());
      assertEquals("", r.getMessageID());
   }
}
