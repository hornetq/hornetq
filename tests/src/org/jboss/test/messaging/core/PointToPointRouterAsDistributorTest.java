/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.interfaces.Routable;
import org.jboss.messaging.interfaces.Message;
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

      Routable m = new MessageSupport(new Integer(0));
      assertFalse(router.handle(m));

      // send with one receiver

      ReceiverImpl rOne = new ReceiverImpl("ONE", ReceiverImpl.HANDLING);
      assertTrue(router.add(rOne));

      m = new MessageSupport(new Integer(1));
      assertTrue(router.handle(m));

      Iterator i = rOne.iterator();
      m = (Routable)i.next();
      assertFalse(i.hasNext());
      // TODO ((Message)m).getMessageID() is a hack! Added to pass the tests. Change it!
      assertEquals(new Integer(1), ((Message)m).getMessageID());

      rOne.clear();

      // send with two receivers

      ReceiverImpl rTwo = new ReceiverImpl("TWO", ReceiverImpl.HANDLING);
      assertTrue(router.add(rTwo));

      m = new MessageSupport(new Integer(2));
      assertTrue(router.handle(m));

      Iterator iOne = rOne.iterator(), iTwo = rTwo.iterator();
      if (iOne.hasNext())
      {
         // then rOne got the message
         m = (Routable)iOne.next();
         assertFalse(iOne.hasNext());
         // TODO ((Message)m).getMessageID() is a hack! Added to pass the tests. Change it!
         assertEquals(new Integer(2), ((Message)m).getMessageID());
         assertFalse(iOne.hasNext());
      }
      else
      {
         // otherwise rTwo got the message
         m = (Routable)iTwo.next();
         assertFalse(iTwo.hasNext());
         // TODO ((Message)m).getMessageID() is a hack! Added to pass the tests. Change it!
         assertEquals(new Integer(2), ((Message)m).getMessageID());
         assertFalse(iOne.hasNext());
      }
   }


   public void testDenyingReceiver() throws Exception
   {
      PointToPointRouter router = new PointToPointRouter("");

      ReceiverImpl denying = new ReceiverImpl("DenyingID", ReceiverImpl.DENYING);
      assertTrue(router.add(denying));

      Routable m = new MessageSupport("");
      assertFalse(router.handle(m));

      Iterator i = denying.iterator();
      assertFalse(i.hasNext());

      ReceiverImpl handling = new ReceiverImpl("HandlingID", ReceiverImpl.HANDLING);
      assertTrue(router.add(handling));
      assertTrue(router.handle(m));

      i = denying.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      m = (Routable)i.next();
      assertFalse(i.hasNext());
      // TODO ((Message)m).getMessageID() is a hack! Added to pass the tests. Change it!
      assertEquals("", ((Message)m).getMessageID());
   }

   public void testBrokenReceiver() throws Exception
   {
      PointToPointRouter router = new PointToPointRouter("");

      ReceiverImpl broken = new ReceiverImpl("BrokenID", ReceiverImpl.BROKEN);
      assertTrue(router.add(broken));

      Routable m = new MessageSupport("");
      assertFalse(router.handle(m));

      Iterator i = broken.iterator();
      assertFalse(i.hasNext());

      ReceiverImpl handling = new ReceiverImpl("HandlingID", ReceiverImpl.HANDLING);
      assertTrue(router.add(handling));
      assertTrue(router.handle(m));

      i = broken.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      m = (Routable)i.next();
      assertFalse(i.hasNext());
      // TODO ((Message)m).getMessageID() is a hack! Added to pass the tests. Change it!
      assertEquals("", ((Message)m).getMessageID());
   }
}
