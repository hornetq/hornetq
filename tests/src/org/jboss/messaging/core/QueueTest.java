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
public class QueueTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public QueueTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testQueue() throws Exception
   {
      Queue queue = new Queue();

      // send without a receiver

      Message m = new CoreMessage("");
      assertTrue(queue.handle(m));

      // attach a receiver

      ReceiverImpl rOne = new ReceiverImpl();
      assertTrue(queue.add(rOne));

      // verify if the receiver got the message; by default the queue is configured to pass by
      // reference

      Iterator i = rOne.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      rOne.clear();

      // send with one receiver

      m = new CoreMessage("");
      assertTrue(queue.handle(m));

      i = rOne.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      rOne.clear();

      // send with two receivers

      ReceiverImpl rTwo = new ReceiverImpl();
      assertTrue(queue.add(rTwo));

      m = new CoreMessage("");
      assertTrue(queue.handle(m));

      Iterator iOne = rOne.iterator(), iTwo = rTwo.iterator();
      if (iOne.hasNext())
      {
         // then rOne got the message
         assertTrue(m == iOne.next());
         assertFalse(iOne.hasNext());
         assertFalse(iTwo.hasNext());
      }
      else
      {
         // otherwise rTwo got the message
         assertTrue(m == iTwo.next());
         assertFalse(iTwo.hasNext());
         assertFalse(iOne.hasNext());
      }
   }


   /**
    * Tests the behaviour of a "denying" receiver (whose handle() returns false). However, the
    * test don't deal with the case when the receiver "heals" (starts accepting messages). See
    * testDenyingReceiverThatStartsAccepting() for that.
    */
   public void testDenyingReceiver() throws Exception
   {
      Queue queue = new Queue();

      ReceiverImpl denying = new ReceiverImpl(ReceiverImpl.DENYING);
      assertTrue(queue.add(denying));

      Message m = new CoreMessage("");
      assertTrue(queue.handle(m));

      Iterator i = denying.iterator();
      assertFalse(i.hasNext());

      ReceiverImpl handling = new ReceiverImpl(ReceiverImpl.HANDLING);
      assertTrue(queue.add(handling));

      // the delivery should have taken place already

      i = denying.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());
   }

   public void testDenyingReceiverThatStartsAccepting() throws Exception
   {
      Queue queue = new Queue();

      ReceiverImpl denying = new ReceiverImpl(ReceiverImpl.DENYING);
      assertTrue(queue.add(denying));

      Message m = new CoreMessage("");
      assertTrue(queue.handle(m));

      Iterator i = denying.iterator();
      assertFalse(i.hasNext());
      assertTrue(queue.hasMessages());

      // "heal" the receiver and attempt re-delivery
      denying.setState(ReceiverImpl.HANDLING);
      assertTrue(queue.deliver());
      assertFalse(queue.hasMessages());
      i = denying.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());
   }


   /**
    * Tests the behaviour of a "broken" receiver (whose handle() throws unchecked exceptions).
    * However, the test don't deal with the case when the receiver "heals" (starts accepting
    * messages). See testBrokenReceiverThatHeals()
    */
   public void testBrokenReceiver() throws Exception
   {
      Queue queue = new Queue();

      ReceiverImpl broken = new ReceiverImpl(ReceiverImpl.BROKEN);
      assertTrue(queue.add(broken));

      Message m = new CoreMessage("");
      assertTrue(queue.handle(m));

      Iterator i = broken.iterator();
      assertFalse(i.hasNext());

      ReceiverImpl handling = new ReceiverImpl(ReceiverImpl.HANDLING);
      assertTrue(queue.add(handling));

      // the delivery should have taken place already

      i = broken.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());
   }

   public void testBrokenReceiverThatHeals() throws Exception
   {
      Queue queue = new Queue();

      ReceiverImpl broken = new ReceiverImpl(ReceiverImpl.BROKEN);
      assertTrue(queue.add(broken));

      Message m = new CoreMessage("");
      assertTrue(queue.handle(m));

      Iterator i = broken.iterator();
      assertFalse(i.hasNext());
      assertTrue(queue.hasMessages());

      // "heal" the receiver and attempt re-delivery
      broken.setState(ReceiverImpl.HANDLING);
      assertTrue(queue.deliver());
      assertFalse(queue.hasMessages());
      i = broken.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());
   }
}
