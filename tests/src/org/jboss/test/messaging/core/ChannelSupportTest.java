/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.util.MessageStoreImpl;
import org.jboss.messaging.core.util.AcknowledgmentStoreImpl;
import org.jboss.messaging.core.message.MessageReferenceSupport;
import org.jboss.messaging.core.message.RoutableSupport;
import org.jboss.messaging.core.message.MessageSupport;

import java.util.List;
import java.util.Set;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ChannelSupportTest extends MessagingTestCase
{
   // Attributes ----------------------------------------------------

   protected Channel channel;
   protected ReceiverImpl receiverOne;

   // Constructors --------------------------------------------------

   public ChannelSupportTest(String name)
   {
      super(name);
   }

   //
   // Test synchronous - asynchronous switch
   //

   public void testSwitchFromAsynchToSynchWhenChannelNotEmpty()
   {
      if (channel == null) { return; }

      receiverOne.setState(ReceiverImpl.DENYING);
      assertTrue(channel.setSynchronous(false));
      assertTrue(channel.handle(new RoutableSupport("routableID")));
      assertTrue(channel.hasMessages());
      assertFalse(channel.setSynchronous(true));
      assertFalse(channel.isSynchronous());
   }

   //
   // Test handle() on synchronous channel
   //

   public void testSuccessfulSynchronousDeliverOnSynchronousChannel()
   {
      if (channel == null) { return; }

      receiverOne.setState(ReceiverImpl.HANDLING);
      assertTrue(channel.setSynchronous(true));
      assertTrue(channel.handle(new RoutableSupport("routableID")));
      assertFalse(channel.hasMessages());
      List l = receiverOne.getMessages();
      assertEquals(1, l.size());
      assertEquals("routableID", ((RoutableSupport)receiverOne.iterator().next()).getMessageID());
   }

   public void testDenyingReceiverOnSynchronousChannel()
   {
      if (channel == null) { return; }

      receiverOne.setState(ReceiverImpl.DENYING);
      assertTrue(channel.setSynchronous(true));
      assertFalse(channel.handle(new RoutableSupport("routableID")));
      assertFalse(channel.hasMessages());
      List l = receiverOne.getMessages();
      assertEquals(0, l.size());
   }

   public void testBrokenReceiverOnSynchronousChannel()
   {
      if (channel == null) { return; }

      receiverOne.setState(ReceiverImpl.BROKEN);
      assertTrue(channel.setSynchronous(true));
      assertFalse(channel.handle(new RoutableSupport("routableID")));
      assertFalse(channel.hasMessages());
      List l = receiverOne.getMessages();
      assertEquals(0, l.size());
   }



   //
   // Test handle() on asynchronous channel - unreliable routable
   //

   public void testSuccessfulSynchronousDeliverOnAsynchronousChannel_UnreliableRoutable()
         throws Exception
   {
      if (channel == null) { return; }

      receiverOne.setState(ReceiverImpl.HANDLING);
      assertTrue(channel.setSynchronous(false));

      assertTrue(channel.handle(new RoutableSupport("routableID", false)));

      // wait for the acknowledgment to arrive and store to clear - if it doesn't, the test will timeout
      while(channel.hasMessages())
      {
         Thread.sleep(500);
      }

      List l = receiverOne.getMessages();
      assertEquals(1, l.size());
      assertEquals("routableID", ((RoutableSupport)receiverOne.iterator().next()).getMessageID());
   }

   public void testDenyingReceiverOnAsynchronousChannel_UnreliableRoutable()
   {
      if (channel == null) { return; }

      receiverOne.setState(ReceiverImpl.DENYING);
      assertTrue(channel.setSynchronous(false));

      assertTrue(channel.handle(new RoutableSupport("routableID", false)));
      assertTrue(channel.hasMessages());
      List l = receiverOne.getMessages();
      assertEquals(0, l.size());
   }

   public void testBrokenReceiverOnAsynchronousChannel_UnreliableRoutable()
   {
      if (channel == null) { return; }

      receiverOne.setState(ReceiverImpl.BROKEN);
      assertTrue(channel.setSynchronous(false));

      assertTrue(channel.handle(new RoutableSupport("routableID", false)));
      assertTrue(channel.hasMessages());
      List l = receiverOne.getMessages();
      assertEquals(0, l.size());
   }

   //
   // Test handle() on asynchronous channel - reliable routable
   //

   public void testSuccessfulSynchronousDeliverOnAsynchronousChannel_ReliableRoutable()
         throws Exception
   {
      if (channel == null) { return; }

      receiverOne.setState(ReceiverImpl.HANDLING);
      channel.setAcknowledgmentStore(new AcknowledgmentStoreImpl("ACKStoreID"));
      channel.setMessageStore(new MessageStoreImpl("MessageStoreID"));
      assertTrue(channel.setSynchronous(false));

      assertTrue(channel.handle(new RoutableSupport("routableID", true)));

      // wait for the acknowledgment to arrive and store to clear - if it doesn't, the test will timeout
      while(channel.hasMessages())
      {
         Thread.sleep(500);
      }

      assertFalse(channel.hasMessages());
      List l = receiverOne.getMessages();
      assertEquals(1, l.size());
      assertEquals("routableID", ((RoutableSupport)receiverOne.iterator().next()).getMessageID());
   }


   public void testDenyingReceiverOnAsynchronousChannel()
   {
      if (channel == null) { return; }

      internalTestReceiverOnAsynchronousChannel_ReliableRoutable(ReceiverImpl.DENYING);
   }

   public void testBrokenReceiverOnAsynchronousChannel()
   {
      if (channel == null) { return; }

      internalTestReceiverOnAsynchronousChannel_ReliableRoutable(ReceiverImpl.BROKEN);
   }

   // Private --------------------------------------------------------

   private void internalTestReceiverOnAsynchronousChannel_ReliableRoutable(String receiverState)
   {
      receiverOne.setState(receiverState);
      assertTrue(channel.setSynchronous(false));



      // no message store, no acknowledgment store
      channel.setMessageStore(null);
      channel.setAcknowledgmentStore(null);

      // send a reliable message

      assertFalse(channel.handle(new MessageSupport("messageID", true)));
      assertFalse(channel.hasMessages());
      List l = receiverOne.getMessages();
      assertEquals(0, l.size());

      // send a reliable reference

      assertFalse(channel.handle(new MessageReferenceSupport("messageID", true, 0, "someStoreID")));
      assertFalse(channel.hasMessages());
      l = receiverOne.getMessages();
      assertEquals(0, l.size());




      // install a broken acknowledgment store, but not a message store yet
      channel.setAcknowledgmentStore(new AcknowledgmentStoreImpl("ACKStoreID",
                                                                 AcknowledgmentStoreImpl.BROKEN));

      // send a reliable message

      assertFalse(channel.handle(new MessageSupport("messageID", true)));
      assertFalse(channel.hasMessages());
      l = receiverOne.getMessages();
      assertEquals(0, l.size());

      // send a reliable reference

      assertFalse(channel.handle(new MessageReferenceSupport("messageID", true, 0, "someStoreID")));
      assertFalse(channel.hasMessages());
      l = receiverOne.getMessages();
      assertEquals(0, l.size());




      // install a valid acknowledgment store, but not a message store yet
      channel.setAcknowledgmentStore(new AcknowledgmentStoreImpl("ACKStoreID"));

      // send a reliable message

      assertFalse(channel.handle(new MessageSupport("messageID", true)));
      assertFalse(channel.hasMessages());
      l = receiverOne.getMessages();
      assertEquals(0, l.size());

      // send a reliable reference

      assertTrue(channel.handle(new MessageReferenceSupport("messageID", true, 0, "someStoreID")));
      assertTrue(channel.hasMessages());
      l = receiverOne.getMessages();
      assertEquals(0, l.size());

      // "fix" the receiver store and get rid of the nacked message
      String s = receiverOne.getState();
      receiverOne.setState(ReceiverImpl.HANDLING);
      assertTrue(channel.deliver());
      assertFalse(channel.hasMessages());
      // "break" the receiver again
      receiverOne.setState(s);
      receiverOne.clear();



      // install a broken message store
      channel.setMessageStore(new MessageStoreImpl("MessageStoreID",
                                                   MessageStoreImpl.BROKEN));

      // send a reliable message

      assertFalse(channel.handle(new MessageSupport("messageID", true)));
      assertFalse(channel.hasMessages());
      l = receiverOne.getMessages();
      assertEquals(0, l.size());

      // send a reliable reference

      assertTrue(channel.handle(new MessageReferenceSupport("messageID", true, 0, "someStoreID")));
      assertTrue(channel.hasMessages());
      l = receiverOne.getMessages();
      assertEquals(0, l.size());

      // "fix" the receiver store and get rid of the nacked message
      s = receiverOne.getState();
      receiverOne.setState(ReceiverImpl.HANDLING);
      assertTrue(channel.deliver());
      assertFalse(channel.hasMessages());
      // "break" the receiver again
      receiverOne.setState(s);
      receiverOne.clear();




      // finally install a valid message store - all should work now
      channel.setMessageStore(new MessageStoreImpl("MessageStoreID"));

      // send a reliable message

      assertTrue(channel.handle(new MessageSupport("messageID", true)));
      assertTrue(channel.hasMessages());
      l = receiverOne.getMessages();
      assertEquals(0, l.size());

      // "fix" the receiver store and get rid of the nacked message
      s = receiverOne.getState();
      receiverOne.setState(ReceiverImpl.HANDLING);
      assertTrue(channel.deliver());
      assertFalse(channel.hasMessages());
      // "break" the receiver again
      receiverOne.setState(s);
      receiverOne.clear();

      // send a reliable reference

      assertTrue(channel.handle(new MessageReferenceSupport("messageID", true, 0, "someStoreID")));
      assertTrue(channel.hasMessages());
      l = receiverOne.getMessages();
      assertEquals(0, l.size());

      // "fix" the receiver store and get rid of the nacked message
      s = receiverOne.getState();
      receiverOne.setState(ReceiverImpl.HANDLING);
      assertTrue(channel.deliver());
      assertFalse(channel.hasMessages());
      // "break" the receiver again
      receiverOne.setState(s);
      receiverOne.clear();
   }


   //
   // Test deliver()
   //

   public void testDeliver()
   {
      if (channel == null) { return; }

      // deliver none

      receiverOne.setState(ReceiverImpl.DENYING);
      assertTrue(channel.setSynchronous(false));
      assertTrue(channel.handle(new RoutableSupport("routableID1")));
      assertTrue(channel.hasMessages());

      receiverOne.setState(ReceiverImpl.HANDLING);
      assertTrue(channel.deliver());

      assertEquals(1, receiverOne.getMessages().size());
      assertTrue(receiverOne.contains("routableID1"));
   }


   public void testDeliverMultiple()
   {
      if (channel == null) { return; }

      // deliver none

      receiverOne.setState(ReceiverImpl.DENYING);
      assertTrue(channel.setSynchronous(false));
      assertTrue(channel.handle(new RoutableSupport("routableID1")));
      assertTrue(channel.handle(new RoutableSupport("routableID2")));
      assertTrue(channel.handle(new RoutableSupport("routableID3")));
      assertTrue(channel.hasMessages());

      assertFalse(channel.deliver());

      receiverOne.setState(ReceiverImpl.BROKEN);
      assertFalse(channel.deliver());

      // deliver all

      receiverOne.setState(ReceiverImpl.HANDLING);
      assertTrue(channel.deliver());

      assertEquals(3, receiverOne.getMessages().size());
      assertTrue(receiverOne.contains("routableID1"));
      assertTrue(receiverOne.contains("routableID2"));
      assertTrue(receiverOne.contains("routableID3"));

   }

   public void testDeliverPartiallyandSwitchToDenying()
   {
      if (channel == null) { return; }

      receiverOne.setState(ReceiverImpl.DENYING);
      assertTrue(channel.setSynchronous(false));

      assertTrue(channel.handle(new RoutableSupport("routableID1", false)));
      assertTrue(channel.handle(new RoutableSupport("routableID2", false)));
      assertTrue(channel.handle(new RoutableSupport("routableID3", false)));
      assertTrue(channel.hasMessages());

      receiverOne.setState(ReceiverImpl.HANDLING);
      receiverOne.setState(ReceiverImpl.DENYING, 2);

      assertFalse(channel.deliver());

      Set unacked = channel.getUnacknowledged();
      assertEquals(1, unacked.size());

      receiverOne.setState(ReceiverImpl.HANDLING);

      assertTrue(channel.deliver());

      assertEquals(3, receiverOne.getMessages().size());
      assertTrue(receiverOne.contains("routableID1"));
      assertTrue(receiverOne.contains("routableID2"));
      assertTrue(receiverOne.contains("routableID3"));
   }

   public void testDeliverPartiallyandSwitchToBroken()
   {
      if (channel == null) { return; }

      receiverOne.setState(ReceiverImpl.DENYING);
      assertTrue(channel.setSynchronous(false));

      assertTrue(channel.handle(new RoutableSupport("routableID1", false)));
      assertTrue(channel.handle(new RoutableSupport("routableID2", false)));
      assertTrue(channel.handle(new RoutableSupport("routableID3")));
      assertTrue(channel.hasMessages());

      receiverOne.setState(ReceiverImpl.HANDLING);
      receiverOne.setState(ReceiverImpl.BROKEN, 2);

      assertFalse(channel.deliver());

      Set unacked = channel.getUnacknowledged();
      assertEquals(1, unacked.size());

      receiverOne.setState(ReceiverImpl.HANDLING);

      assertTrue(channel.deliver());

      assertEquals(3, receiverOne.getMessages().size());
      assertTrue(receiverOne.contains("routableID1"));
      assertTrue(receiverOne.contains("routableID2"));
      assertTrue(receiverOne.contains("routableID3"));
   }

   public void testMessageExpiration() throws Exception
   {
      if (channel == null) { return; }

      channel.setSynchronous(false);
      receiverOne.setState(ReceiverImpl.DENYING);

      assertTrue(channel.handle(new RoutableSupport("routableID1", false, 1000))); // this should expire
      assertTrue(channel.handle(new RoutableSupport("routableID2", false, 100000)));

      assertEquals(2, channel.getUnacknowledged().size());

      Thread.sleep(2000);

      receiverOne.setState(ReceiverImpl.HANDLING);

      assertTrue(channel.deliver());

      assertEquals(1, receiverOne.getMessages().size());
      assertTrue(receiverOne.contains("routableID2"));
   }


}