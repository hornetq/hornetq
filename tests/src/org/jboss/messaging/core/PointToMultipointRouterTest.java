/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.MessagingTestCase;
import org.jboss.messaging.interfaces.Message;


import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class PointToMultipointRouterTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public PointToMultipointRouterTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testPointToMultipointRouter() throws Exception
   {
      PointToMultipointRouter router = new PointToMultipointRouter();

      // by default, the router sends the message by reference
      assertTrue(router.isPassByReference());

      // send without a receiver

      Message m = new CoreMessage(new Integer(0));
      assertFalse(router.handle(m));

      // send with one receiver

      ReceiverImpl rOne = new ReceiverImpl();
      assertTrue(router.add(rOne));

      m = new CoreMessage(new Integer(1));
      assertTrue(router.handle(m));

      Iterator i = rOne.iterator();
      Message n = (Message)i.next();
      assertFalse(i.hasNext());
      assertTrue(m == n);

      rOne.clear();

      // send with two receivers

      ReceiverImpl rTwo = new ReceiverImpl();
      assertTrue(router.add(rTwo));

      m = new CoreMessage(new Integer(2));
      assertTrue(router.handle(m));

      Iterator iOne = rOne.iterator();
      n = (Message)iOne.next();
      assertFalse(iOne.hasNext());
      assertTrue(m == n);

      Iterator iTwo = rTwo.iterator();
      n = (Message)iTwo.next();
      assertFalse(iTwo.hasNext());
      assertTrue(m == n);
   }


   public void testDenyingReceiver() throws Exception
   {
      PointToMultipointRouter router = new PointToMultipointRouter();

      ReceiverImpl denying = new ReceiverImpl(ReceiverImpl.DENYING);
      assertTrue(router.add(denying));

      Message m = new CoreMessage("");
      assertFalse(router.handle(m));

      Iterator i = denying.iterator();
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(router.acknowledged(denying));

      ReceiverImpl handling = new ReceiverImpl(ReceiverImpl.HANDLING);
      assertTrue(router.add(handling));
      assertFalse(router.handle(m));

      i = denying.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(router.acknowledged(denying));
      assertTrue(router.acknowledged(handling));
   }

   public void testBrokenReceiver() throws Exception
   {
      PointToMultipointRouter router = new PointToMultipointRouter();

      ReceiverImpl broken = new ReceiverImpl(ReceiverImpl.BROKEN);
      assertTrue(router.add(broken));

      Message m = new CoreMessage("");
      assertFalse(router.handle(m));

      Iterator i = broken.iterator();
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(router.acknowledged(broken));

      ReceiverImpl handling = new ReceiverImpl(ReceiverImpl.HANDLING);
      assertTrue(router.add(handling));
      assertFalse(router.handle(m));

      i = broken.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      // test the acknowledgement
      assertFalse(router.acknowledged(broken));
      assertTrue(router.acknowledged(handling));
   }
}
