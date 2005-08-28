/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.test.messaging.MessagingTestCase;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ChannelSupportTest extends MessagingTestCase
{
//   // Attributes ----------------------------------------------------
//
//   protected Channel channel;
//   protected ReceiverImpl receiverOne;
//
   // Constructors --------------------------------------------------

   public ChannelSupportTest(String name)
   {
      super(name);
   }

//   //
//   // Test synchronous - asynchronous switch
//   //
//
//   public void testSwitchFromAsynchToSynchWhenChannelNotEmpty()
//   {
//      if(skip()) { return; }
//
//      receiverOne.setState(ReceiverImpl.NACKING);
//      assertTrue(channel.setSynchronous(false));
//      assertTrue(channel.handle(new RoutableSupport("routableID")));
//      assertTrue(channel.hasMessages());
//      assertFalse(channel.setSynchronous(true));
//      assertFalse(channel.isSynchronous());
//   }
//
//   //
//   // Test handle() on synchronous channel
//   //
//
//   public void testSuccessfulSynchronousDeliverOnSynchronousChannel()
//   {
//      if(skip()) { return; }
//
//      receiverOne.setState(ReceiverImpl.HANDLING);
//      assertTrue(channel.setSynchronous(true));
//      assertTrue(channel.handle(new RoutableSupport("routableID")));
//      assertFalse(channel.hasMessages());
//      List l = receiverOne.getMessages();
//      assertEquals(1, l.size());
//      assertEquals("routableID", ((RoutableSupport)receiverOne.iterator().next()).getMessageID());
//   }
//
//   public void testDenyingReceiverOnSynchronousChannel()
//   {
//      if(skip()) { return; }
//
//      receiverOne.setState(ReceiverImpl.NACKING);
//      assertTrue(channel.setSynchronous(true));
//      assertFalse(channel.handle(new RoutableSupport("routableID")));
//      assertFalse(channel.hasMessages());
//      List l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//   }
//
//   public void testBrokenReceiverOnSynchronousChannel()
//   {
//      if(skip()) { return; }
//
//      receiverOne.setState(ReceiverImpl.BROKEN);
//      assertTrue(channel.setSynchronous(true));
//      assertFalse(channel.handle(new RoutableSupport("routableID")));
//      assertFalse(channel.hasMessages());
//      List l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//   }
//
//
//
//   //
//   // Test handle() on asynchronous channel - unreliable routable
//   //
//
//   public void testSuccessfulSynchronousDeliverOnAsynchronousChannel_UnreliableRoutable()
//         throws Exception
//   {
//      if(skip()) { return; }
//
//      receiverOne.setState(ReceiverImpl.HANDLING);
//      assertTrue(channel.setSynchronous(false));
//
//      assertTrue(channel.handle(new RoutableSupport("routableID", false)));
//
//      // wait for the acknowledgment to arrive and store to clear - if it doesn't, the test will timeout
//      while(channel.hasMessages())
//      {
//         Thread.sleep(500);
//      }
//
//      List l = receiverOne.getMessages();
//      assertEquals(1, l.size());
//      assertEquals("routableID", ((RoutableSupport)receiverOne.iterator().next()).getMessageID());
//   }
//
//   public void testNackingReceiverOnAsynchronousChannel_UnreliableRoutable()
//   {
//      if(skip()) { return; }
//
//      receiverOne.setState(ReceiverImpl.NACKING);
//      assertTrue(channel.setSynchronous(false));
//
//      assertTrue(channel.handle(new RoutableSupport("routableID", false)));
//      assertTrue(channel.hasMessages());
//      List l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//   }
//
//   public void testBrokenReceiverOnAsynchronousChannel_UnreliableRoutable()
//   {
//      if(skip()) { return; }
//
//      receiverOne.setState(ReceiverImpl.BROKEN);
//      assertTrue(channel.setSynchronous(false));
//
//      assertTrue(channel.handle(new RoutableSupport("routableID", false)));
//      if (channel.isStoringUndeliverableMessages())
//      {
//         assertTrue(channel.hasMessages());
//      }
//      else
//      {
//         assertFalse(channel.hasMessages());
//      }
//      List l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//   }
//
//   //
//   // Test handle() on asynchronous channel - reliable routable
//   //
//
//   public void testSuccessfulSynchronousDeliverOnAsynchronousChannel_ReliableRoutable()
//         throws Exception
//   {
//      if(skip()) { return; }
//
//      receiverOne.setState(ReceiverImpl.HANDLING);
//      channel.setAcknowledgmentStore(new TestAcknowledgmentStore("ACKStoreID"));
//      channel.setMessageStore(new MessageStoreImpl("MessageStoreID"));
//      assertTrue(channel.setSynchronous(false));
//
//      assertTrue(channel.handle(new RoutableSupport("routableID", true)));
//
//      // wait for the acknowledgment to arrive and store to clear - if it doesn't, the test will timeout
//      while(channel.hasMessages())
//      {
//         Thread.sleep(500);
//      }
//
//      assertFalse(channel.hasMessages());
//      List l = receiverOne.getMessages();
//      assertEquals(1, l.size());
//      assertEquals("routableID", ((RoutableSupport)receiverOne.iterator().next()).getMessageID());
//   }
//
//
//   public void testDenyingReceiverOnAsynchronousChannel()
//   {
//      if(skip()) { return; }
//
//      internalTestReceiverOnAsynchronousChannel_ReliableRoutable(ReceiverImpl.NACKING);
//   }
//
//   public void testBrokenReceiverOnAsynchronousChannel()
//   {
//      if(skip()) { return; }
//
//      internalTestReceiverOnAsynchronousChannel_ReliableRoutable(ReceiverImpl.BROKEN);
//   }
//
//   // Private --------------------------------------------------------
//
//   private void internalTestReceiverOnAsynchronousChannel_ReliableRoutable(String receiverState)
//   {
//      receiverOne.setState(receiverState);
//      assertTrue(channel.setSynchronous(false));
//
//
//
//      // no message store, no acknowledgment store
//      channel.setMessageStore(null);
//      channel.setAcknowledgmentStore(null);
//
//      // send a reliable message
//
//      boolean ack = channel.handle(new MessageSupport("messageID", true));
//
//      if (receiverState == ReceiverImpl.BROKEN && !channel.isStoringUndeliverableMessages())
//      {
//         assertTrue(ack);
//      }
//      else
//      {
//         assertFalse(ack);
//      }
//      assertFalse(channel.hasMessages());
//      List l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//
//      // send a reliable reference
//
//      ack = channel.handle(new MessageReferenceSupport("messageID", true, 0, "someStoreID"));
//      if (receiverState == ReceiverImpl.BROKEN && !channel.isStoringUndeliverableMessages())
//      {
//          assertTrue(ack);
//      }
//      else
//      {
//          assertFalse(ack);
//      }
//
//      assertFalse(channel.hasMessages());
//      l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//
//
//
//
//      // install a broken acknowledgment store, but not a message store yet
//      channel.setAcknowledgmentStore(new TestAcknowledgmentStore("ACKStoreID",
//                                                                 TestAcknowledgmentStore.BROKEN));
//
//      // send a reliable message
//
//      ack = channel.handle(new MessageSupport("messageID", true));
//      if (receiverState == ReceiverImpl.BROKEN && !channel.isStoringUndeliverableMessages())
//      {
//          assertTrue(ack);
//      }
//      else
//      {
//          assertFalse(ack);
//      }
//      assertFalse(channel.hasMessages());
//      l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//
//      // send a reliable reference
//
//      ack = channel.handle(new MessageReferenceSupport("messageID", true, 0, "someStoreID"));
//      if (receiverState == ReceiverImpl.BROKEN && !channel.isStoringUndeliverableMessages())
//      {
//          assertTrue(ack);
//      }
//      else
//      {
//          assertFalse(ack);
//      }
//      assertFalse(channel.hasMessages());
//      l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//
//
//
//
//      // install a valid acknowledgment store, but not a message store yet
//      channel.setAcknowledgmentStore(new TestAcknowledgmentStore("ACKStoreID"));
//
//      // send a reliable message
//
//      ack = channel.handle(new MessageSupport("messageID", true));
//      if (receiverState == ReceiverImpl.BROKEN && !channel.isStoringUndeliverableMessages())
//      {
//          assertTrue(ack);
//      }
//      else
//      {
//          assertFalse(ack);
//      }
//      assertFalse(channel.hasMessages());
//      l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//
//      // send a reliable reference
//
//      assertTrue(channel.handle(new MessageReferenceSupport("messageID", true, Long.MAX_VALUE, "someStoreID")));
//
//      if (receiverState == ReceiverImpl.NACKING ||
//          receiverState == ReceiverImpl.BROKEN && channel.isStoringUndeliverableMessages())
//      {
//         assertTrue(channel.hasMessages());
//      }
//      else
//      {
//         assertFalse(channel.hasMessages());
//      }
//
//      l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//
//      // "fix" the receiver store and get rid of the nacked message
//      String s = receiverOne.getState();
//      receiverOne.setState(ReceiverImpl.HANDLING);
//      assertTrue(channel.deliver());
//      assertFalse(channel.hasMessages());
//      // "break" the receiver again
//      receiverOne.setState(s);
//      receiverOne.clear();
//
//
//
//      // install a broken message store
//      channel.setMessageStore(new MessageStoreImpl("MessageStoreID",
//                                                   MessageStoreImpl.BROKEN));
//
//      // send a reliable message
//
//      ack = channel.handle(new MessageSupport("messageID", true));
//      if (receiverState == ReceiverImpl.BROKEN && !channel.isStoringUndeliverableMessages())
//      {
//          assertTrue(ack);
//      }
//      else
//      {
//          assertFalse(ack);
//      }
//      assertFalse(channel.hasMessages());
//      l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//
//      // send a reliable reference
//
//      assertTrue(channel.handle(new MessageReferenceSupport("messageID", true, Long.MAX_VALUE, "someStoreID")));
//
//      if (receiverState == ReceiverImpl.NACKING ||
//          receiverState == ReceiverImpl.BROKEN && channel.isStoringUndeliverableMessages())
//      {
//         assertTrue(channel.hasMessages());
//      }
//      else
//      {
//         assertFalse(channel.hasMessages());
//      }
//
//      l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//      // "fix" the receiver store and get rid of the nacked message
//      s = receiverOne.getState();
//      receiverOne.setState(ReceiverImpl.HANDLING);
//      assertTrue(channel.deliver());
//      assertFalse(channel.hasMessages());
//      // "break" the receiver again
//      receiverOne.setState(s);
//      receiverOne.clear();
//
//
//
//
//      // finally install a valid message store - all should work now
//      channel.setMessageStore(new MessageStoreImpl("MessageStoreID"));
//
//      // send a reliable message
//
//      assertTrue(channel.handle(new MessageSupport("messageID", true)));
//      if (receiverState == ReceiverImpl.NACKING ||
//          receiverState == ReceiverImpl.BROKEN && channel.isStoringUndeliverableMessages())
//      {
//         assertTrue(channel.hasMessages());
//      }
//      else
//      {
//         assertFalse(channel.hasMessages());
//      }
//      l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//
//      // "fix" the receiver store and get rid of the nacked message
//      s = receiverOne.getState();
//      receiverOne.setState(ReceiverImpl.HANDLING);
//      assertTrue(channel.deliver());
//      assertFalse(channel.hasMessages());
//      // "break" the receiver again
//      receiverOne.setState(s);
//      receiverOne.clear();
//
//      // send a reliable reference
//
//      assertTrue(channel.handle(new MessageReferenceSupport("messageID", true, Long.MAX_VALUE, "someStoreID")));
//      if (receiverState == ReceiverImpl.NACKING ||
//          receiverState == ReceiverImpl.BROKEN && channel.isStoringUndeliverableMessages())
//      {
//         assertTrue(channel.hasMessages());
//      }
//      else
//      {
//         assertFalse(channel.hasMessages());
//      }
//      l = receiverOne.getMessages();
//      assertEquals(0, l.size());
//
//      // "fix" the receiver store and get rid of the nacked message
//      s = receiverOne.getState();
//      receiverOne.setState(ReceiverImpl.HANDLING);
//      assertTrue(channel.deliver());
//      assertFalse(channel.hasMessages());
//      // "break" the receiver again
//      receiverOne.setState(s);
//      receiverOne.clear();
//   }
//
//
//   //
//   // Test deliver()
//   //
//
//   public void testDeliver()
//   {
//      if(skip()) { return; }
//
//      // deliver none
//
//      receiverOne.setState(ReceiverImpl.NACKING);
//      assertTrue(channel.setSynchronous(false));
//      assertTrue(channel.handle(new RoutableSupport("routableID1")));
//      assertTrue(channel.hasMessages());
//
//      receiverOne.setState(ReceiverImpl.HANDLING);
//      assertTrue(channel.deliver());
//
//      assertEquals(1, receiverOne.getMessages().size());
//      assertTrue(receiverOne.contains("routableID1"));
//   }
//
//
//   public void testDeliverMultiple()
//   {
//      if(skip()) { return; }
//
//      // deliver none
//
//      receiverOne.setState(ReceiverImpl.NACKING);
//      assertTrue(channel.setSynchronous(false));
//      assertTrue(channel.handle(new RoutableSupport("routableID1")));
//      assertTrue(channel.handle(new RoutableSupport("routableID2")));
//      assertTrue(channel.handle(new RoutableSupport("routableID3")));
//      assertTrue(channel.hasMessages());
//
//      assertFalse(channel.deliver());
//
//      receiverOne.setState(ReceiverImpl.BROKEN);
//      assertFalse(channel.deliver());
//
//      // deliver all
//
//      receiverOne.setState(ReceiverImpl.HANDLING);
//      assertTrue(channel.deliver());
//
//      assertEquals(3, receiverOne.getMessages().size());
//      assertTrue(receiverOne.contains("routableID1"));
//      assertTrue(receiverOne.contains("routableID2"));
//      assertTrue(receiverOne.contains("routableID3"));
//
//   }
//
//   public void testDeliverPartiallyandSwitchToDenying()
//   {
//      if(skip()) { return; }
//
//      receiverOne.setState(ReceiverImpl.NACKING);
//      assertTrue(channel.setSynchronous(false));
//
//      assertTrue(channel.handle(new RoutableSupport("routableID1", false)));
//      assertTrue(channel.handle(new RoutableSupport("routableID2", false)));
//      assertTrue(channel.handle(new RoutableSupport("routableID3", false)));
//      assertTrue(channel.hasMessages());
//
//      receiverOne.setState(ReceiverImpl.HANDLING);
//      receiverOne.setState(ReceiverImpl.NACKING, 2);
//
//      assertFalse(channel.deliver());
//
//      Set unacked = channel.getUndelivered();
//      assertEquals(1, unacked.size());
//
//      receiverOne.setState(ReceiverImpl.HANDLING);
//
//      assertTrue(channel.deliver());
//
//      assertEquals(3, receiverOne.getMessages().size());
//      assertTrue(receiverOne.contains("routableID1"));
//      assertTrue(receiverOne.contains("routableID2"));
//      assertTrue(receiverOne.contains("routableID3"));
//   }
//
//   public void testDeliverPartiallyandSwitchToBroken()
//   {
//      if(skip()) { return; }
//
//      receiverOne.setState(ReceiverImpl.NACKING);
//      assertTrue(channel.setSynchronous(false));
//
//      assertTrue(channel.handle(new RoutableSupport("routableID1", false)));
//      assertTrue(channel.handle(new RoutableSupport("routableID2", false)));
//      assertTrue(channel.handle(new RoutableSupport("routableID3")));
//      assertTrue(channel.hasMessages());
//
//      receiverOne.setState(ReceiverImpl.HANDLING);
//      receiverOne.setState(ReceiverImpl.BROKEN, 2);
//
//      assertFalse(channel.deliver());
//
//      Set unacked = channel.getUndelivered();
//      assertEquals(1, unacked.size());
//
//      receiverOne.setState(ReceiverImpl.HANDLING);
//
//      assertTrue(channel.deliver());
//
//      assertEquals(3, receiverOne.getMessages().size());
//      assertTrue(receiverOne.contains("routableID1"));
//      assertTrue(receiverOne.contains("routableID2"));
//      assertTrue(receiverOne.contains("routableID3"));
//   }
//
//   public void testMessageExpiration() throws Exception
//   {
//      if(skip()) { return; }
//
//      channel.setSynchronous(false);
//      receiverOne.setState(ReceiverImpl.NACKING);
//
//      assertTrue(channel.handle(new RoutableSupport("routableID1", false, 1000))); // this should expire
//      assertTrue(channel.handle(new RoutableSupport("routableID2", false, 100000)));
//
//      assertEquals(2, channel.getUndelivered().size());
//
//      Thread.sleep(2000);
//
//      receiverOne.setState(ReceiverImpl.HANDLING);
//
//      assertTrue(channel.deliver());
//
//      assertEquals(1, receiverOne.getMessages().size());
//      assertTrue(receiverOne.contains("routableID2"));
//   }
//
//
//   //
//   // Asynchronous ACK tests
//   //
//   public void testInvalidAsynchronousACK() throws Exception
//   {
//      if(skip()) { return; }
//
//      receiverOne.setState(ReceiverImpl.NACKING);
//      assertTrue(channel.setSynchronous(false));
//
//      assertTrue(channel.handle(new RoutableSupport("routableID1", false)));
//      assertTrue(channel.handle(new RoutableSupport("routableID2", false)));
//
//      channel.acknowledge("inexistentMessageID", receiverOne.getReceiverID());
//
//      assertTrue(channel.hasMessages());
//      Set stillNaked = channel.getUndelivered();
//      assertEquals(2, stillNaked.size());
//      assertTrue(stillNaked.contains("routableID1"));
//      assertTrue(stillNaked.contains("routableID2"));
//
//      channel.acknowledge("routableID1", "inexistentReceiver");
//
//      assertTrue(channel.hasMessages());
//      stillNaked = channel.getUndelivered();
//      assertEquals(2, stillNaked.size());
//      assertTrue(stillNaked.contains("routableID1"));
//      assertTrue(stillNaked.contains("routableID2"));
//   }
//
//   public void testAsynchronousACK() throws Exception
//   {
//      if(skip()) { return; }
//
//      receiverOne.setState(ReceiverImpl.NACKING);
//      assertTrue(channel.setSynchronous(false));
//
//      assertTrue(channel.handle(new RoutableSupport("routableID1", false)));
//      assertTrue(channel.handle(new RoutableSupport("routableID2", false)));
//
//      channel.acknowledge("routableID1", receiverOne.getReceiverID());
//
//      assertTrue(channel.hasMessages());
//      Set stillNaked = channel.getUndelivered();
//      assertEquals(1, stillNaked.size());
//      assertTrue(stillNaked.contains("routableID2"));
//
//      channel.acknowledge("routableID2", receiverOne.getReceiverID());
//
//      assertFalse(channel.hasMessages());
//   }
//
//
//   private boolean skip()
//   {
//      return channel == null;
//   }

   public void testNoop()
   {

   }
}