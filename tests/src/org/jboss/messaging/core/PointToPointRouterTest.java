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
public class PointToPointRouterTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public PointToPointRouterTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testPointToPointRouter() throws Exception
   {
      PointToPointRouter router = new PointToPointRouter();

      // send without a receiver

      Message m = new CoreMessage(new Integer(0));
      assertFalse(router.handle(m));

      // send with one receiver

      ReceiverImpl rOne = new ReceiverImpl();
      assertTrue(router.add(rOne));

      m = new CoreMessage(new Integer(1));
      assertTrue(router.handle(m));

      Iterator i = rOne.iterator();
      m = (Message)i.next();
      assertFalse(i.hasNext());
      assertEquals(new Integer(1), m.getMessageID());

      rOne.clear();

      // send with two receivers

      ReceiverImpl rTwo = new ReceiverImpl();
      assertTrue(router.add(rTwo));

      m = new CoreMessage(new Integer(2));
      assertTrue(router.handle(m));

      Iterator iOne = rOne.iterator(), iTwo = rTwo.iterator();
      if (iOne.hasNext())
      {
         // then rOne got the message
         m = (Message)iTwo.next();
         assertFalse(iTwo.hasNext());
         assertEquals(new Integer(2), m.getMessageID());
         assertFalse(iTwo.hasNext());
      }
      else
      {
         // otherwise rTwo got the message
         m = (Message)iTwo.next();
         assertFalse(iTwo.hasNext());
         assertEquals(new Integer(2), m.getMessageID());
         assertFalse(iOne.hasNext());
      }
   }


   public void testDenyingReceiver() throws Exception
   {
      PointToPointRouter router = new PointToPointRouter();

      ReceiverImpl denying = new ReceiverImpl(ReceiverImpl.DENYING);
      assertTrue(router.add(denying));

      Message m = new CoreMessage("");
      assertFalse(router.handle(m));

      Iterator i = denying.iterator();
      assertFalse(i.hasNext());

      ReceiverImpl handling = new ReceiverImpl(ReceiverImpl.HANDLING);
      assertTrue(router.add(handling));
      assertTrue(router.handle(m));

      i = denying.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      m = (Message)i.next();
      assertFalse(i.hasNext());
      assertEquals("", m.getMessageID());
   }

   public void testBrokenReceiver() throws Exception
   {
      PointToPointRouter router = new PointToPointRouter();

      ReceiverImpl broken = new ReceiverImpl(ReceiverImpl.BROKEN);
      assertTrue(router.add(broken));

      Message m = new CoreMessage("");
      assertFalse(router.handle(m));

      Iterator i = broken.iterator();
      assertFalse(i.hasNext());

      ReceiverImpl handling = new ReceiverImpl(ReceiverImpl.HANDLING);
      assertTrue(router.add(handling));
      assertTrue(router.handle(m));

      i = broken.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      m = (Message)i.next();
      assertFalse(i.hasNext());
      assertEquals("", m.getMessageID());
   }
}
