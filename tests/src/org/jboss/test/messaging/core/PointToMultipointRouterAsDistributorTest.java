/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.interfaces.Routable;
import org.jboss.messaging.core.PointToMultipointRouter;
import org.jboss.messaging.core.MessageSupport;

import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class PointToMultipointRouterAsDistributorTest extends DistributorTest
{
   // Constructors --------------------------------------------------

   public PointToMultipointRouterAsDistributorTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      super.setUp();

      // Create a PointToMultipointRouter to be tested by the superclass tests
      distributor = new PointToMultipointRouter("P2MPRouterID");
   }

   public void tearDown()throws Exception
   {
      distributor.clear();
      distributor = null;
      super.tearDown();
   }

   //
   // This test also runs all DistributorTest's tests
   //

   public void testPointToMultipointRouter() throws Exception
   {
      PointToMultipointRouter router = new PointToMultipointRouter("");

      // by default, the router sends the message by reference
      assertTrue(router.isPassByReference());

      // send without a receiver

      Routable m = new MessageSupport(new Integer(0));
      assertFalse(router.handle(m));

      // send with one receiver

      ReceiverImpl rOne = new ReceiverImpl("ONE", ReceiverImpl.HANDLING);
      assertTrue(router.add(rOne));

      m = new MessageSupport(new Integer(1));
      assertTrue(router.handle(m));

      Iterator i = rOne.iterator();
      Routable n = (Routable)i.next();
      assertFalse(i.hasNext());
      assertTrue(m == n);

      rOne.clear();

      // send with two receivers

      ReceiverImpl rTwo = new ReceiverImpl("TWO", ReceiverImpl.HANDLING);
      assertTrue(router.add(rTwo));

      m = new MessageSupport(new Integer(2));
      assertTrue(router.handle(m));

      Iterator iOne = rOne.iterator();
      n = (Routable)iOne.next();
      assertFalse(iOne.hasNext());
      assertTrue(m == n);

      Iterator iTwo = rTwo.iterator();
      n = (Routable)iTwo.next();
      assertFalse(iTwo.hasNext());
      assertTrue(m == n);
   }


   public void testDenyingReceiver() throws Exception
   {
      PointToMultipointRouter router = new PointToMultipointRouter("");

      ReceiverImpl denying = new ReceiverImpl("ReceiverONE", ReceiverImpl.DENYING);
      assertTrue(router.add(denying));

      Routable m = new MessageSupport("");
      assertFalse(router.handle(m));

      Iterator i = denying.iterator();
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(router.acknowledged(denying.getReceiverID()));

      ReceiverImpl handling = new ReceiverImpl("ReceiverTWO", ReceiverImpl.HANDLING);
      assertTrue(router.add(handling));
      assertFalse(router.handle(m));

      i = denying.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(router.acknowledged(denying.getReceiverID()));
      assertTrue(router.acknowledged(handling.getReceiverID()));
   }

   public void testBrokenReceiver() throws Exception
   {
      PointToMultipointRouter router = new PointToMultipointRouter("");

      ReceiverImpl broken = new ReceiverImpl("ReceiverONE", ReceiverImpl.BROKEN);
      assertTrue(router.add(broken));

      Routable m = new MessageSupport("");
      assertFalse(router.handle(m));

      Iterator i = broken.iterator();
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(router.acknowledged(broken.getReceiverID()));

      ReceiverImpl handling = new ReceiverImpl("ReceiverTWO", ReceiverImpl.HANDLING);
      assertTrue(router.add(handling));
      assertFalse(router.handle(m));

      i = broken.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(router.acknowledged(broken.getReceiverID()));
      assertTrue(router.acknowledged(handling.getReceiverID()));
   }
}
