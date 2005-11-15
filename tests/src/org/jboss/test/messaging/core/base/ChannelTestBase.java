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
package org.jboss.test.messaging.core.base;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.message.Factory;
import org.jboss.messaging.core.message.MemoryMessageStore;
import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.test.messaging.core.SimpleReceiver;

import javax.naming.InitialContext;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * The Channel test strategy is to try as many combination as it makes sense of the following
 * variables:
 *
 * 1. The channel can be non-recoverable (does not have access to a PersistenceManager) or
 *    recoverable. A non-recoverable channel can accept reliable messages or not.
 * 2. The channel may have zero or one receivers (the behavior for more than one receiver depends
 *    on the particular router implementation).
 * 3. The receiver may be ACKING or NACKING (the case when it throws unchecked exceptions is handled
 *    at the Router level).
 * 4. The sender can send message(s) non-transactionally or transactionally (and then can commit
 *    or rollback the transaction).
 * 5. The NACKING receiver can send acknowledgment(s) non-transactionally or transactionally (and
 *    then can commit or rollback the transaction).
 * 6. The message can be non-reliable or reliable.
 * 7. The sender can send one or multiple messages.
 * 8. A recoverable channel may be crashed and tested if it successfully recovers.
 *
 * This test base also tests the Distributor interface.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class ChannelTestBase extends NoTestsChannelTestBase
{
   // Constants -----------------------------------------------------

   public static final int NUMBER_OF_MESSAGES = 10;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected TransactionRepository tr;
   protected MessageStore ms;

   // Constructors --------------------------------------------------

   public ChannelTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      InitialContext ic = new InitialContext();
      tr = new TransactionRepository(null);
      ic.close();

      ms = new MemoryMessageStore("message-store");
   }

   public void tearDown() throws Exception
   {
      ms = null;
      tr = null;
      super.tearDown();
   }

   public static void assertEqualSets(Routable[] a, List routables)
   {
      assertEquals(a.length, routables.size());
      List l = new ArrayList(routables);

      for(int i = 0; i < a.length; i++)
      {
         for(Iterator j = l.iterator(); j.hasNext(); )
         {
            Object o = j.next();
            Message m = null;
            if (o instanceof MessageReference)
            {
               MessageReference ref = (MessageReference)o;
               m = ref.getMessage();
            }
            else
            {
               m = (Message)o;
            }

            if (a[i].getMessageID().equals(m.getMessageID()) &&
                (a[i] instanceof Message ? ((Message)a[i]).getPayload().equals(m.getPayload()) : true))
            {
               j.remove();
               break;
            }
         }
      }

      if (!l.isEmpty())
      {
         fail("Messages " + l + " do not match!");
      }
   }

   public static void assertEqualSets(Delivery[] a, List deliveries)
   {
      assertEquals(a.length, deliveries.size());
      List l = new ArrayList(deliveries);

      for(int i = 0; i < a.length; i++)
      {
         for(Iterator j = l.iterator(); j.hasNext(); )
         {
            Delivery d = (Delivery)j.next();
            MessageReference ref = d.getReference();

            if (a[i].getReference().getMessageID().equals(ref.getMessageID()))
            {
               j.remove();
               break;
            }
         }
      }

      if (!l.isEmpty())
      {
         fail("Deliveries " + l + " do not match!");
      }
   }


   // Channel tests -------------------------------------------------

   public void testClosedChannel() throws Exception
   {
      channel.close();
      try
      {
         channel.handle(null, Factory.createMessage("message0"), null);
         fail("should throw exception");
      }
      catch(IllegalStateException e)
      {
         //OK
      }
   }

   public void testHandleNullRoutable() throws Exception
   {
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      assertNull(channel.handle(observer, null, null));
   }

   //////////////////////////////////
   ////////////////////////////////// Test matrix
   //////////////////////////////////

   //
   // Non-recoverable channel
   //

   ////
   //// Zero receivers
   ////

   //////
   ////// Non-transacted send
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableChannel_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      List stored = channel.browse();
      assertEqualSets(messages, stored);
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_3_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      // the channel must not accept the message
      assertNull(delivery);

      assertTrue(channel.browse().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_3_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_4_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         // the channel must not accept the message
         assertNull(delivery);
      }

      assertTrue(channel.browse().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_4_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
   }



   //////
   ////// Transacted send and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableChannel_5() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.commit();

      List stored = channel.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }



   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_6() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.commit();

      List stored = channel.browse();
      assertEqualSets(messages, stored);
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_7_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      try
      {
         tx.commit();
         fail("this should throw exception");
      }
      catch(Exception e)
      {
         // OK
      }

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_7_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.commit();

      List stored = channel.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_8_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      try
      {
         tx.commit();
         fail("this should throw exception");
      }
      catch(Exception e)
      {
         // OK
      }

      // still no messages in the channel
      assertEquals(0, channel.browse().size());

   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_8_1_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      try
      {
         tx.commit();
         fail("this should throw exception");
      }
      catch(Exception e)
      {
         // OK
      }

      // still no messages in the channel
      assertEquals(0, channel.browse().size());

   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_8_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.commit();

      assertEqualSets(messages, channel.browse());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_8_2_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.commit();

      assertEqualSets(messages, channel.browse());
   }


   //////
   ////// Transacted send and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableChannel_9() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_10() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableChannel_11() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_12() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_12_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
   }

   ////
   //// One receiver
   ////

   //////
   ////// ACKING receiver
   //////

   //////
   ////// Non-transacted send
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableChannel_13() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());
      assertTrue(channel.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_14() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertTrue(channel.browse().isEmpty());

      List received = r.getMessages();
      assertEqualSets(messages, received);
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_15_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertNull(delivery);

      assertTrue(channel.browse().isEmpty());
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_15_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      assertTrue(channel.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_16_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertNull(delivery);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_16_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertTrue(channel.browse().isEmpty());
      assertEqualSets(messages, r.getMessages());
   }

   //////
   ////// Transacted send and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableChannel_17() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(channel.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_18() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertEqualSets(messages, r.getMessages());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_19_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      try
      {
         tx.commit();
         fail("this should throw exception");
      }
      catch(Exception e)
      {
         // OK
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_19_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_20_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      try
      {
         tx.commit();
         fail("this should throw exception");
      }
      catch(Exception e)
      {
         // OK
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_20_1_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      try
      {
         tx.commit();
         fail("this should throw exception");
      }
      catch(Exception e)
      {
         // OK
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_20_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      assertEqualSets(messages, r.getMessages());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_20_2_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      assertEqualSets(messages, r.getMessages());
   }

   //////
   ////// Transacted send and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableChannel_21() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_22() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableChannel_23() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_24() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_24_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////
   ////// NACKING receiver
   //////

   //////
   ////// Non-transacted send
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testNonRecoverableChannel_25() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
   }

   /**
    * The same test as before, but with a Receiver configured to acknowledge immediately
    * on the Delivery. Simulates a race condition in which the acknoledgment arrives before
    * the Delivery is returned to channel.
    *
    * @throws Throwable
    */
   public void testNonRecoverableChannel_25_race() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      r.setImmediateAsynchronousAcknowledgment(true);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      // the receiver should have returned a "done" delivery
      assertTrue(channel.browse().isEmpty());

      List messages = r.getMessages();
      assertEquals(1, messages.size());
      Message ackm = (Message)messages.get(0);
      assertEquals("message0", ackm.getMessageID());

      // an extra acknowledgment should be discarded
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testNonRecoverableChannel_25_1() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testNonRecoverableChannel_25_2() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      tx.rollback();

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      // acknowledge non-transactionally
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
   }


   //////////
   ////////// Multiple message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testNonRecoverableChannel_26() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());

   }

   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testNonRecoverableChannel_26_1() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testNonRecoverableChannel_26_2() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());

      tx.rollback();

      assertEqualSets(messages, channel.browse());

      // acknowledge non-transactionally
      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_27_1_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      // not accepted by the channel
      assertNull(delivery);
      assertTrue(channel.browse().isEmpty());
      assertTrue(r.getMessages().isEmpty());

   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_27_1_2() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message rm = (Message)received.iterator().next();
      assertTrue(rm.isReliable());
      assertEquals("message0", rm.getMessageID());

      r.acknowledge(rm, null);

      assertTrue(channel.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment
   ////////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////


   // Doesn't make sense, the message won't be accepted anyway.

   ///////////
   /////////// Channel accepts reliable messages
   ///////////


   public void testNonRecoverableChannel_27_2_2() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message rm = (Message)received.iterator().next();
      assertTrue(rm.isReliable());
      assertEquals("message0", rm.getMessageID());

      Transaction tx = tr.createTransaction();

      r.acknowledge(rm, tx);

      stored = channel.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_28_1_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         // not accepted by the channel
         assertNull(delivery);
      }

      assertTrue(channel.browse().isEmpty());
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_28_1_2() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment
   ////////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   // Doesn't make sense, the message won't be accepted anyway.

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_28_2_2() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
   }

   //////
   ////// Transacted send and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableChannel_29() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_30() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_31_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      try
      {
         tx.commit();
         fail("this should throw exception");
      }
      catch(Exception e)
      {
         // OK
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel does accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_31_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      List stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message rm = (Message)received.iterator().next();
      assertTrue(rm.isReliable());
      assertEquals("message0", rm.getMessageID());
   }


   //////////
   ////////// Multiple message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_32_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      try
      {
         tx.commit();
         fail("this should throw exception");
      }
      catch(Exception e)
      {
         // OK
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_32_1_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (channel.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      try
      {
         tx.commit();
         fail("this should throw exception");
      }
      catch(Exception e)
      {
         // OK
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_32_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_32_2_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!channel.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());
   }



   //////
   ////// Transacted send and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableChannel_33() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_34() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableChannel_35() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_36() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_36_mixed() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }




   //
   // Recoverable channel
   //

   public void testRecoverableChannel_0() throws Exception
   {
      if (channel.isRecoverable())
      {
         assertTrue(channel.acceptReliableMessages());
      }
   }

   ////
   //// Zero receivers
   ////

   //////
   ////// Non-transacted send
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_1() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_2() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_3() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_4() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());

   }

   //////
   ////// Transacted send and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_5() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.commit();

      List stored = channel.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_6() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.commit();

      assertEqualSets(messages, channel.browse());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_7() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.commit();

      List stored = channel.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_8() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.commit();

      assertEqualSets(messages, channel.browse());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableChannel_8_mixed() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.commit();

      assertEqualSets(messages, channel.browse());
   }


   //////
   ////// Transacted send and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_9() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_10() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_11() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_12() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableChannel_12_mixed() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, channel.browse().size());
   }

   ////
   //// One receiver
   ////

   //////
   ////// ACKING receiver
   //////

   //////
   ////// Non-transacted send
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_13() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());
      assertTrue(channel.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_14() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertTrue(channel.browse().isEmpty());

      List received = r.getMessages();
      assertEqualSets(messages, received);
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_15() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      assertTrue(channel.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_16() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertTrue(channel.browse().isEmpty());
      assertEqualSets(messages, r.getMessages());
   }

   //////
   ////// Transacted send and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_17() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(channel.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_18() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
      assertEqualSets(messages, r.getMessages());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_19() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_20() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      assertEqualSets(messages, r.getMessages());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableChannel_20_mixed() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      assertEqualSets(messages, r.getMessages());
   }


   //////
   ////// Transacted send and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_21() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_22() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_23() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_24() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableChannel_24_mixed() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////
   ////// NACKING receiver
   //////

   //////
   ////// Non-transacted send
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testRecoverableChannel_25() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
   }

   /**
    * The same test as before, but with a Receiver configured to acknowledge immediately
    * on the Delivery. Simulates a race condition in which the acknoledgment arrives before
    * the Delivery is returned to channel.
    *
    * @throws Throwable
    */
   public void testRecoverableChannel_25_race() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      r.setImmediateAsynchronousAcknowledgment(true);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      // the receiver should have returned a "done" delivery
      assertTrue(channel.browse().isEmpty());

      List messages = r.getMessages();
      assertEquals(1, messages.size());
      Message ackm = (Message)messages.get(0);
      assertEquals("message0", ackm.getMessageID());

      // an extra acknowledgment should be discarded
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testRecoverableChannel_25_1() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testRecoverableChannel_25_2() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      tx.rollback();

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      // acknowledge non-transactionally
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
   }


   //////////
   ////////// Multiple message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testRecoverableChannel_26() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());

   }

   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testRecoverableChannel_26_1() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testRecoverableChannel_26_2() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());

      tx.rollback();

      assertEqualSets(messages, channel.browse());

      // acknowledge non-transactionally
      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testRecoverableChannel_27() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
   }

   /**
    * The same test as before, but with a Receiver configured to acknowledge immediately
    * on the Delivery. Simulates a race condition in which the acknoledgment arrives before
    * the Delivery is returned to channel.
    *
    * @throws Throwable
    */
   public void testRecoverableChannel_27_race() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      r.setImmediateAsynchronousAcknowledgment(true);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      // the receiver should have returned a "done" delivery
      assertTrue(channel.browse().isEmpty());

      List messages = r.getMessages();
      assertEquals(1, messages.size());
      Message ackm = (Message)messages.get(0);
      assertEquals("message0", ackm.getMessageID());

      // an extra acknowledgment should be discarded
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testRecoverableChannel_27_1() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testRecoverableChannel_27_2() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = channel.handle(observer, m, null);

      assertTrue(delivery.isDone());

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      tx.rollback();

      delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      // acknowledge non-transactionally
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
   }


   //////////
   ////////// Multiple message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testRecoverableChannel_28() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testRecoverableChannel_28_1() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());

      tx.commit();

      assertTrue(channel.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testRecoverableChannel_28_2() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = channel.handle(observer, messages[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(messages, channel.browse());

      tx.rollback();

      assertEqualSets(messages, channel.browse());

      // acknowledge non-transactionally
      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
   }


   //////
   ////// Transacted send and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_29() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_30() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_31() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      List delivering = channel.browse();
      assertEquals(1, delivering.size());
      assertEquals("message0", ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals("message0", ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(channel.browse().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_32() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableChannel_32_mixed() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(messages, channel.browse());
      assertEqualSets(messages, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(channel.browse().isEmpty());
   }


   //////
   ////// Transacted send and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_33() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_34() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_35() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      channel.handle(observer, m, tx);

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_36() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         messages[i] = Factory.createMessage("message" + i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableChannel_36_mixed() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.NACKING);
      assertTrue(channel.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      Message[] messages = new Message[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         messages[i] = Factory.createMessage("message" + i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         channel.handle(observer, messages[i], tx);
      }

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, channel.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////////////////////////
   /////////////////////////////// Add receiver tests
   ///////////////////////////////
   ///////////////////////////////
   ///////////////////////////////

   //
   // Non-recoverable channel
   //

   ////
   //// Non-reliable message
   ////

   //////
   ////// Broken receiver
   //////

   public void testAddReceiver_1() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = channel.handle(observer, m, null);
      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());


      SimpleReceiver receiver = new SimpleReceiver("BrokenReceiver", SimpleReceiver.BROKEN);
      assertTrue(channel.add(receiver));

      stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      assertTrue(receiver.getMessages().isEmpty());
   }

   //////
   ////// ACKING receiver
   //////

   public void testAddReceiver_2() throws Exception
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = channel.handle(observer, m, null);
      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("ACKINGReceiver", SimpleReceiver.ACKING, channel);
      assertTrue(channel.add(receiver));

      assertEquals(1, channel.browse().size());

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertTrue(channel.browse().isEmpty());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////
   ////// NACKING receiver
   //////

   public void testAddReceiver_3() throws Throwable
   {
      if (channel.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = channel.handle(observer, m, null);
      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("NACKINGReceiver", SimpleReceiver.NACKING, channel);
      assertTrue(channel.add(receiver));

      assertEquals(1, channel.browse().size());

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertEquals(1, channel.browse().size());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      receiver.acknowledge(sm, null);

      assertTrue(channel.browse().isEmpty());

      messages = receiver.getMessages();
      assertEquals(1, messages.size());
      sm = (Message)messages.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //
   // Recoverable channel
   //

   ////
   //// Reliable message
   ////

   public void testAddReceiver_4() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = channel.handle(observer, m, null);
      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());


      SimpleReceiver receiver = new SimpleReceiver("BrokenReceiver", SimpleReceiver.BROKEN);
      assertTrue(channel.add(receiver));

      stored = channel.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      assertTrue(receiver.getMessages().isEmpty());
   }

   //////
   ////// ACKING receiver
   //////

   public void testAddReceiver_5() throws Exception
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = channel.handle(observer, m, null);
      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("ACKINGReceiver", SimpleReceiver.ACKING, channel);
      assertTrue(channel.add(receiver));

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertTrue(channel.browse().isEmpty());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }

   //////
   ////// NACKING receiver
   //////

   public void testAddReceiver_6() throws Throwable
   {
      if (!channel.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(channel.iterator().hasNext());

      Message m = Factory.createMessage("message0", true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = channel.handle(observer, m, null);
      assertTrue(delivery.isDone());

      List stored = channel.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("NACKINGReceiver", SimpleReceiver.NACKING, channel);
      assertTrue(channel.add(receiver));

      assertEquals(1, channel.browse().size());

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertEquals(1, channel.browse().size());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());

      receiver.acknowledge(sm, null);

      assertTrue(channel.browse().isEmpty());

      messages = receiver.getMessages();
      assertEquals(1, messages.size());
      sm = (Message)messages.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals("message0", sm.getMessageID());
   }




   ///////////////////////////////
   /////////////////////////////// Channel crash tests
   ///////////////////////////////

//   public void testReliableChannelFailure() throws Throwable
//   {
//      if (!channel.isRecoverable())
//      {
//         return;
//      }
//
//      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
//      SimpleReceiver r = new SimpleReceiver("ONE", SimpleReceiver.NACKING);
//      channel.add(r);
//
//      Routable m = Factory.createMessage("m0", true, "payload");
//      Delivery d = channel.handle(observer, m, null);
//      assertTrue(d.isDone());
//
//      List l = r.getMessages();
//      assertEquals(1, l.size());
//      Message rm  = (Message)l.get(0);
//      assertEquals(rm.getMessageID(), m.getMessageID());
//
//      crashChannel();
//
//      recoverChannel();
//
//      // make sure the recovered channel still holds the message
//
//      l = channel.browse();
//      assertEquals(1, l.size());
//      MessageReference ref = (MessageReference)l.get(0);
//      rm  = ref.getMessage();
//      assertEquals(rm.getMessageID(), m.getMessageID());
//
//
//      // TODO review this
//      try
//      {
//         r.acknowledge(m, null);
//         fail("should throw exception");
//      }
//      catch(IllegalStateException e)
//      {
//         // OK
//      }
//   }


   // Distributor tests ---------------------------------------------

   public void testAddOneReceiver()
   {
      Receiver r = new SimpleReceiver("ONE");

      assertTrue(channel.add(r));
      assertFalse(channel.add(r));

      assertTrue(channel.contains(r));

      Iterator i = channel.iterator();
      assertEquals(r, i.next());
      assertFalse(i.hasNext());

      channel.clear();
      assertFalse(channel.iterator().hasNext());
   }

   public void testRemoveInexistentReceiver()
   {
      assertFalse(channel.remove(new SimpleReceiver("INEXISTENT")));
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected abstract void crashChannel() throws Exception;

   protected abstract void recoverChannel() throws Exception;

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
