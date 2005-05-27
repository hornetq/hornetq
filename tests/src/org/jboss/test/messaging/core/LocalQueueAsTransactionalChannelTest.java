/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.local.LocalQueue;
import org.jboss.messaging.core.message.MessageSupport;


import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class LocalQueueAsTransactionalChannelTest extends TransactionalChannelSupportTest
{
   private boolean runLocalTests = true;

   // Constructors --------------------------------------------------

   public LocalQueueAsTransactionalChannelTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      // Create a LocalQueue to be tested by the superclass tests
      channel = new LocalQueue("LocalQueueID");
      receiverOne = new ReceiverImpl("ReceiverOne", ReceiverImpl.HANDLING);
      ((LocalQueue)channel).add(receiverOne);

      runChannelSupportTests = true;
      runTransactionalChannelSupportTests = true;
      runLocalTests = true;

      super.setUp();
   }

   public void tearDown()throws Exception
   {
      ((LocalQueue)channel).clear();
      channel = null;
      receiverOne = null;

      super.tearDown();
   }

   //
   // This test also runs all ChannelSupportTest's tests
   //

   public void testDefaultAsynchronous()
   {
      if (skip()) { return; }

      assertFalse(channel.isSynchronous());
   }

   public void testQueue() throws Exception
   {
      if (skip()) { return; }

      LocalQueue queue = new LocalQueue("");

      // send without a receiver

      Routable m = new MessageSupport("");
      assertTrue(queue.handle(m));

      // attach a receiver

      ReceiverImpl rOne = new ReceiverImpl("ReceiverONE", ReceiverImpl.HANDLING);
      assertTrue(queue.add(rOne));

      // verify if the receiver got the message; by default the queue is configured to pass by
      // reference

      Iterator i = rOne.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      rOne.clear();

      // send with one receiver

      m = new MessageSupport("");
      assertTrue(queue.handle(m));

      i = rOne.iterator();
      assertTrue(m == i.next());
      assertFalse(i.hasNext());

      rOne.clear();

      // send with two receivers

      ReceiverImpl rTwo = new ReceiverImpl("ReceiverTWO", ReceiverImpl.HANDLING);
      assertTrue(queue.add(rTwo));

      m = new MessageSupport("");
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
    * Tests the behaviour of a "nacking" receiver (whose handle() returns false). However, the
    * test doesn't deal with the case when the receiver "heals" (starts accepting messages). See
    * testNackingReceiverThatStartsAccepting() for that.
    */
   public void testNackingReceiver() throws Exception
   {
      if (skip()) { return; }

      LocalQueue queue = new LocalQueue("");

      ReceiverImpl nacking = new ReceiverImpl("NackingReceiverID", ReceiverImpl.NACKING);
      assertTrue(queue.add(nacking));

      Routable m = new MessageSupport("");
      assertTrue(queue.handle(m));

      Iterator i = nacking.iterator();
      assertFalse(i.hasNext());

      // the queue holds a NACK for the nacking receiver, so it cannot deliver the message
      // until either the nacking receiver ACKs or the message expires, otherwise the queue
      // will deliver the same message to two receivers.

      ReceiverImpl handling = new ReceiverImpl("HandlingReceiverID", ReceiverImpl.HANDLING);
      assertTrue(queue.add(handling));

      // there should be an unsuccessful delivery attempt

      assertTrue(queue.hasMessages());

      // an explicit delivery attempt should faile
      assertFalse(queue.deliver());

      i = nacking.iterator();
      assertFalse(i.hasNext());

      i = handling.iterator();
      assertFalse(i.hasNext());
   }

   public void testNackingReceiverThatStartsAccepting() throws Exception
   {
      if (skip()) { return; }

      LocalQueue queue = new LocalQueue("");

      ReceiverImpl nacking = new ReceiverImpl(ReceiverImpl.NACKING);
      assertTrue(queue.add(nacking));

      Routable m = new MessageSupport("");
      assertTrue(queue.handle(m));

      Iterator i = nacking.iterator();
      assertFalse(i.hasNext());

      assertTrue(queue.hasMessages());

      // "heal" the receiver and attempt re-delivery
      nacking.setState(ReceiverImpl.HANDLING);

      assertTrue(queue.deliver());
      assertFalse(queue.hasMessages());

      i = nacking.iterator();
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
      if (skip()) { return; }

      LocalQueue queue = new LocalQueue("");

      ReceiverImpl broken = new ReceiverImpl("BrokenReceiverID", ReceiverImpl.BROKEN);
      assertTrue(queue.add(broken));

      Routable m = new MessageSupport("");
      assertTrue(queue.handle(m));

      Iterator i = broken.iterator();
      assertFalse(i.hasNext());

      ReceiverImpl handling = new ReceiverImpl("HandlingReceiverID", ReceiverImpl.HANDLING);
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
      if (skip()) { return; }

      LocalQueue queue = new LocalQueue("");

      ReceiverImpl broken = new ReceiverImpl(ReceiverImpl.BROKEN);
      assertTrue(queue.add(broken));

      Routable m = new MessageSupport("");
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


   private boolean skip()
   {
      return !runLocalTests;
   }
}
