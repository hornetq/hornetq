/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.interfaces.Routable;
import org.jboss.messaging.core.PointToPointRouter;
import org.jboss.messaging.core.MessageSupport;

import java.util.Iterator;

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
      assertFalse(router.handle(r));

      // send with one receiver

      ReceiverImpl rOne = new ReceiverImpl("ONE", ReceiverImpl.HANDLING);
      assertTrue(router.add(rOne));

      r = new MessageSupport(new Integer(1));
      assertTrue(router.handle(r));

      Iterator i = rOne.iterator();
      r = (Routable)i.next();
      assertFalse(i.hasNext());
      assertEquals(new Integer(1), r.getMessageID());

      rOne.clear();

      // send with two receivers

      ReceiverImpl rTwo = new ReceiverImpl("TWO", ReceiverImpl.HANDLING);
      assertTrue(router.add(rTwo));

      r = new MessageSupport(new Integer(2));
      assertTrue(router.handle(r));

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


   public void testDenyingReceiver() throws Exception
   {
      PointToPointRouter router = new PointToPointRouter("");

      ReceiverImpl denying = new ReceiverImpl("DenyingID", ReceiverImpl.DENYING);
      assertTrue(router.add(denying));

      Routable r = new MessageSupport("");
      assertFalse(router.handle(r));

      Iterator i = denying.iterator();
      assertFalse(i.hasNext());

      ReceiverImpl handling = new ReceiverImpl("HandlingID", ReceiverImpl.HANDLING);
      assertTrue(router.add(handling));
      assertTrue(router.handle(r));

      i = denying.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      r = (Routable)i.next();
      assertFalse(i.hasNext());
      assertEquals("", r.getMessageID());
   }

   public void testBrokenReceiver() throws Exception
   {
      PointToPointRouter router = new PointToPointRouter("");

      ReceiverImpl broken = new ReceiverImpl("BrokenID", ReceiverImpl.BROKEN);
      assertTrue(router.add(broken));

      Routable r = new MessageSupport("");
      assertFalse(router.handle(r));

      Iterator i = broken.iterator();
      assertFalse(i.hasNext());

      ReceiverImpl handling = new ReceiverImpl("HandlingID", ReceiverImpl.HANDLING);
      assertTrue(router.add(handling));
      assertTrue(router.handle(r));

      i = broken.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      r = (Routable)i.next();
      assertFalse(i.hasNext());
      assertEquals("", r.getMessageID());
   }
}
