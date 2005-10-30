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



/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class LocalQueueAsChannelTest extends TransactionalChannelSupportTest
{
   // Constructors --------------------------------------------------

   public LocalQueueAsChannelTest(String name)
   {
      super(name);
   }

//   public void setUp() throws Exception
//   {
//      // Create a LocalQueue to be tested by the superclass tests
//      channel = new LocalQueue("LocalQueueID");
//      receiverOne = new ReceiverImpl("ReceiverOne", ReceiverImpl.HANDLING);
//      ((LocalQueue)channel).add(receiverOne);
//
//      super.setUp();
//   }
//
//   public void tearDown()throws Exception
//   {
//      ((LocalQueue)channel).clear();
//      channel = null;
//      receiverOne = null;
//
//      super.tearDown();
//   }
//
//   //
//   // This test also runs all ChannelSupportTest's tests
//   //
//
//   public void testDefaultAsynchronous()
//   {
//      assertFalse(channel.isSynchronous());
//   }
//
//   public void testQueue() throws Exception
//   {
//      LocalQueue queue = new LocalQueue("");
//
//      // send without a receiver
//
//      Routable m = new MessageSupport("");
//      assertTrue(queue.handle(m));
//
//      // attach a receiver
//
//      ReceiverImpl rOne = new ReceiverImpl("ReceiverONE", ReceiverImpl.HANDLING);
//      assertTrue(queue.add(rOne));
//
//      // verify if the receiver got the message; by default the queue is configured to pass by
//      // reference
//
//      Iterator i = rOne.iterator();
//      assertTrue(m == i.next());
//      assertFalse(i.hasNext());
//
//      rOne.clear();
//
//      // send with one receiver
//
//      m = new MessageSupport("");
//      assertTrue(queue.handle(m));
//
//      i = rOne.iterator();
//      assertTrue(m == i.next());
//      assertFalse(i.hasNext());
//
//      rOne.clear();
//
//      // send with two receivers
//
//      ReceiverImpl rTwo = new ReceiverImpl("ReceiverTWO", ReceiverImpl.HANDLING);
//      assertTrue(queue.add(rTwo));
//
//      m = new MessageSupport("");
//      assertTrue(queue.handle(m));
//
//      Iterator iOne = rOne.iterator(), iTwo = rTwo.iterator();
//      if (iOne.hasNext())
//      {
//         // then rOne got the message
//         assertTrue(m == iOne.next());
//         assertFalse(iOne.hasNext());
//         assertFalse(iTwo.hasNext());
//      }
//      else
//      {
//         // otherwise rTwo got the message
//         assertTrue(m == iTwo.next());
//         assertFalse(iTwo.hasNext());
//         assertFalse(iOne.hasNext());
//      }
//   }
//
//
//   /**
//    * Tests the behaviour of a "nacking" receiver (whose handle() returns false). However, the
//    * test doesn't deal with the case when the receiver "heals" (starts accepting messages). See
//    * testNackingReceiverThatStartsAccepting() for that.
//    */
//   public void testNackingReceiver() throws Exception
//   {
//      LocalQueue queue = new LocalQueue("");
//
//      ReceiverImpl nacking = new ReceiverImpl("NackingReceiverID", ReceiverImpl.NACKING);
//      assertTrue(queue.add(nacking));
//
//      Routable m = new MessageSupport("");
//      assertTrue(queue.handle(m));
//
//      Iterator i = nacking.iterator();
//      assertFalse(i.hasNext());
//
//      // the queue holds a NACK for the nacking receiver, so it cannot deliver the message
//      // until either the nacking receiver ACKs or the message expires, otherwise the queue
//      // will deliver the same message to two receivers.
//
//      ReceiverImpl handling = new ReceiverImpl("HandlingReceiverID", ReceiverImpl.HANDLING);
//      assertTrue(queue.add(handling));
//
//      // there should be an unsuccessful delivery attempt
//
//      assertTrue(queue.hasMessages());
//
//      // an explicit delivery attempt should faile
//      assertFalse(queue.deliver());
//
//      i = nacking.iterator();
//      assertFalse(i.hasNext());
//
//      i = handling.iterator();
//      assertFalse(i.hasNext());
//   }
//
//   public void testNackingReceiverThatStartsAccepting() throws Exception
//   {
//      LocalQueue queue = new LocalQueue("");
//
//      ReceiverImpl nacking = new ReceiverImpl(ReceiverImpl.NACKING);
//      assertTrue(queue.add(nacking));
//
//      Routable m = new MessageSupport("");
//      assertTrue(queue.handle(m));
//
//      Iterator i = nacking.iterator();
//      assertFalse(i.hasNext());
//
//      assertTrue(queue.hasMessages());
//
//      // "heal" the receiver and attempt re-delivery
//      nacking.setState(ReceiverImpl.HANDLING);
//
//      assertTrue(queue.deliver());
//      assertFalse(queue.hasMessages());
//
//      i = nacking.iterator();
//      assertTrue(m == i.next());
//      assertFalse(i.hasNext());
//   }
//
//
//   /**
//    * Tests the behaviour of a "broken" receiver (whose handle() throws unchecked exceptions).
//    * However, the test don't deal with the case when the receiver "heals" (starts accepting
//    * messages). See testBrokenReceiverThatHeals()
//    */
//   public void testBrokenReceiver() throws Exception
//   {
//      LocalQueue queue = new LocalQueue("");
//
//      ReceiverImpl broken = new ReceiverImpl("BrokenReceiverID", ReceiverImpl.BROKEN);
//      assertTrue(queue.add(broken));
//
//      Routable m = new MessageSupport("");
//      assertTrue(queue.handle(m));
//
//      Iterator i = broken.iterator();
//      assertFalse(i.hasNext());
//
//      ReceiverImpl handling = new ReceiverImpl("HandlingReceiverID", ReceiverImpl.HANDLING);
//      assertTrue(queue.add(handling));
//
//      // the delivery should have taken place already
//
//      i = broken.iterator();
//      assertFalse(i.hasNext());
//
//      i = handling.iterator();
//      assertTrue(m == i.next());
//      assertFalse(i.hasNext());
//   }
//
//   public void testBrokenReceiverThatHeals() throws Exception
//   {
//      LocalQueue queue = new LocalQueue("");
//
//      ReceiverImpl broken = new ReceiverImpl(ReceiverImpl.BROKEN);
//      assertTrue(queue.add(broken));
//
//      Routable m = new MessageSupport("");
//      assertTrue(queue.handle(m));
//
//      Iterator i = broken.iterator();
//      assertFalse(i.hasNext());
//      assertTrue(queue.hasMessages());
//
//      // "heal" the receiver and attempt re-delivery
//      broken.setState(ReceiverImpl.HANDLING);
//      assertTrue(queue.deliver());
//      assertFalse(queue.hasMessages());
//      i = broken.iterator();
//      assertTrue(m == i.next());
//      assertFalse(i.hasNext());
//   }
}
