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
package org.jboss.test.messaging.core.local;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.message.CoreMessage;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.BrokenReceiver;
import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.test.messaging.core.SimpleFilter;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.util.CoreMessageFactory;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * The QueueTest test strategy is to try as many combination as it makes sense of the following
 * variables:
 *
 * 1. The Queue can be non-recoverable  or
 *    recoverable. A non-recoverable channel can accept reliable messages or not.
 * 2. The Queue may have zero or one receivers (the behavior for more than one receiver depends
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
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1019 $</tt>
 *
 * $Id: ChannelTestBase.java 1019 2006-07-17 17:15:04Z timfox $
 */
public abstract class PagingFilteredQueueTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   public static final int NUMBER_OF_MESSAGES = 10;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected PersistenceManager pm;
   
   protected TransactionRepository tr;
   
   protected MessageStore ms;
   
   protected ServiceContainer sc;

   protected PagingFilteredQueue queue;
   
   // Constructors --------------------------------------------------

   public PagingFilteredQueueTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      
      sc = new ServiceContainer("all,-remoting,-security");
      sc.start();

      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(), null,
                                    true, true, true, 100);      
      pm.start();
      
      tr = new TransactionRepository(pm, new IdManager("TRANSACTION_ID", 10, pm));
      tr.start();
      
      ms = new SimpleMessageStore();
      ms.start();
   }

   public void tearDown() throws Exception
   {
      sc.stop();
      sc = null;
      
      ms = null;
      tr = null;
      super.tearDown();
   }

   public static void assertEqualSets(MessageReference[] a, List msgs)
   {
      assertEquals(a.length, msgs.size());
      List l = new ArrayList(msgs);

      for(int i = 0; i < a.length; i++)
      {
         for(Iterator j = l.iterator(); j.hasNext(); )
         {
            Object o = j.next();
            Message m = (Message)o;
            

            if (a[i].getMessageID() == m.getMessageID() &&
                m.getPayload().equals(a[i].getMessage().getPayload()))
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

            if (a[i].getReference().getMessageID() == ref.getMessageID())
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
   

   public void testWithFilter()
   {
      Filter f = new SimpleFilter(3);
            
      PagingFilteredQueue queue = new PagingFilteredQueue("queue1", 1, ms, pm, true, false, new QueuedExecutor(), f);
      
      Message m1 = new CoreMessage(1, false, 0, 0, (byte)0, null, null);
      Message m2 = new CoreMessage(2, false, 0, 0, (byte)0, null, null);
      Message m3 = new CoreMessage(3, false, 0, 0, (byte)0, null, null);
      Message m4 = new CoreMessage(4, false, 0, 0, (byte)0, null, null);
      Message m5 = new CoreMessage(5, false, 0, 0, (byte)0, null, null);
      
      MessageReference ref1 = ms.reference(m1);
      MessageReference ref2 = ms.reference(m2);
      MessageReference ref3 = ms.reference(m3);
      MessageReference ref4 = ms.reference(m4);
      MessageReference ref5 = ms.reference(m5);
      
      Delivery del = queue.handle(null, ref1, null);
      assertFalse(del.isSelectorAccepted());
      
      del = queue.handle(null, ref2, null);
      assertFalse(del.isSelectorAccepted());
      
      del = queue.handle(null, ref3, null);
      assertTrue(del.isSelectorAccepted());
      
      del = queue.handle(null, ref4, null);
      assertFalse(del.isSelectorAccepted());
      
      del = queue.handle(null, ref5, null);
      assertFalse(del.isSelectorAccepted());
   }
   
   
   public void testUnreliableSynchronousDeliveryTwoReceivers() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r1 = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      SimpleReceiver r2 = new SimpleReceiver("TWO", SimpleReceiver.ACKING);
      
      queue.add(r1);
      queue.add(r2);
      
      Delivery d = queue.handle(observer, createReference(0, false, "payload"), null);
      
      assertTrue(d.isDone());
      List l1 = r1.getMessages();
      List l2 = r2.getMessages();
      if (l2.isEmpty())
      {
         assertEquals(1, l1.size());
         Message m = (Message)l1.get(0);
         assertEquals("payload", m.getPayload());
      }
      else
      {
         assertTrue(l1.isEmpty());
         assertEquals(1, l2.size());
         Message m = (Message)l2.get(0);
         assertEquals("payload", m.getPayload());
      }
   }


   public void testReliableSynchronousDeliveryTwoReceivers() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      SimpleReceiver r1 = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
      SimpleReceiver r2 = new SimpleReceiver("TWO", SimpleReceiver.ACKING);
      assertTrue(queue.add(r1));
      assertTrue(queue.add(r2));

      Delivery d = queue.handle(observer, createReference(0, true, "payload"), null);

      assertTrue(d.isDone());
      List l1 = r1.getMessages();
      List l2 = r2.getMessages();
      if (l2.isEmpty())
      {
         assertEquals(1, l1.size());
         Message m = (Message)l1.get(0);
         assertEquals("payload", m.getPayload());
      }
      else
      {
         assertTrue(l1.isEmpty());
         assertEquals(1, l2.size());
         Message m = (Message)l2.get(0);
         assertEquals("payload", m.getPayload());
      }
   }
   
   /*
    * If a channel has a set a receiver and remove is called with a different receiver
    * need to ensure the receiver is not removed (since it doesn't match)
    */
   public void testRemoveDifferentReceiver() throws Exception
   {
      Receiver receiver1 = new SimpleReceiver();
      
      Receiver receiver2 = new SimpleReceiver();
      
      assertFalse(queue.iterator().hasNext());
      
      queue.add(receiver1);
      
      assertTrue(queue.contains(receiver1));
      
      queue.remove(receiver1);
      
      assertFalse(queue.iterator().hasNext());
      
      assertFalse(queue.contains(receiver1));
      
      queue.add(receiver1);
      
      assertTrue(queue.contains(receiver1));
      
      queue.remove(receiver2);
      
      assertTrue(queue.contains(receiver1));
                 
   }

   public void testClosedChannel() throws Exception
   {
      queue.close();
      try
      {
         queue.handle(null, createReference(0), null);
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
      assertNull(queue.handle(observer, null, null));
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List stored = queue.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      SimpleReceiver receiver = new SimpleReceiver("ACKING", SimpleReceiver.ACKING);
      queue.add(receiver);
      queue.deliver(true);
      assertEquals(1, receiver.getMessages().size());
      assertEquals(0, ((Message)receiver.getMessages().get(0)).getMessageID());

      queue.deliver(true);
      assertEquals(1, receiver.getMessages().size());


   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      List stored = queue.browse();
      assertEqualSets(refs, stored);

      SimpleReceiver receiver = new SimpleReceiver("ACKING", SimpleReceiver.ACKING);
      queue.add(receiver);
      queue.deliver(true);
      assertEquals(10, receiver.getMessages().size());
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         assertEquals(i, ((Message)receiver.getMessages().get(i)).getMessageID());         
      }
      receiver.clear();

      queue.deliver(true);
      assertEquals(0, receiver.getMessages().size());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      // the channel must not accept the message
      assertNull(delivery);

      assertTrue(queue.browse().isEmpty());

      queue.deliver(true);
      
      Receiver r = new SimpleReceiver("ACKING", SimpleReceiver.ACKING);
      
      queue.add(r);
      
      queue.deliver(true);
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_3_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List stored = queue.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      queue.deliver(true);

      SimpleReceiver receiver = new SimpleReceiver("ACKING", SimpleReceiver.ACKING);
      queue.add(receiver);
      queue.deliver(true);
      assertEquals(1, receiver.getMessages().size());
      assertEquals(0, ((Message)receiver.getMessages().get(0)).getMessageID());

      queue.deliver(true);
      assertEquals(1, receiver.getMessages().size());
   }

   //////////
   ////////// Multiple message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_4_1() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         // the channel must not accept the message
         assertNull(delivery);

         queue.deliver(true);
         
         Receiver r = new SimpleReceiver("ACKING", SimpleReceiver.ACKING);
         
         queue.add(r);
         
         queue.deliver(true);
      }

      assertTrue(queue.browse().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_4_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());

      SimpleReceiver receiver = new SimpleReceiver("ACKING", SimpleReceiver.ACKING);
      queue.add(receiver);
      queue.deliver(true);
      assertEquals(10, receiver.getMessages().size());
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         assertEquals(i, ((Message)receiver.getMessages().get(i)).getMessageID());         
      }
      receiver.clear();

      queue.deliver(true);
      assertEquals(0, receiver.getMessages().size());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.commit();

      List stored = queue.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }



   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_6() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.commit();

      List stored = queue.browse();
      assertEqualSets(refs, stored);
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

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
      assertEquals(0, queue.browse().size());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_7_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.commit();

      List stored = queue.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_8_1() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

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
      assertEquals(0, queue.browse().size());

   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_8_1_mixed() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

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
      assertEquals(0, queue.browse().size());

   }
   
   public void testNonRecoverableChannel_8_1_mixed_1() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES * 2];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i + NUMBER_OF_MESSAGES] = createReference(i + NUMBER_OF_MESSAGES , true, "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i + NUMBER_OF_MESSAGES], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

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
      assertEquals(0, queue.browse().size());

   }
   

   public void testNonRecoverableChannel_8_1_mixed_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES * 2];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i + NUMBER_OF_MESSAGES] = createReference(i + NUMBER_OF_MESSAGES , true, "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i + NUMBER_OF_MESSAGES], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

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
      assertEquals(0, queue.browse().size());

   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_8_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.commit();

      assertEqualSets(refs, queue.browse());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_8_2_mixed() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.commit();

      assertEqualSets(refs, queue.browse());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse().size());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_10() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse().size());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableChannel_11() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse().size());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_12() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse().size());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_12_mixed() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse().size());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());
      assertTrue(queue.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_14() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertTrue(queue.browse().isEmpty());

      List received = r.getMessages();
      assertEqualSets(refs, received);
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertNull(delivery);

      assertTrue(queue.browse().isEmpty());
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_15_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      assertTrue(queue.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_16_1() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertNull(delivery);
      }

      assertTrue(queue.browse().isEmpty());
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_16_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertTrue(queue.browse().isEmpty());
      assertEqualSets(refs, r.getMessages());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(queue.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_18() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(queue.browse().isEmpty());
      assertEqualSets(refs, r.getMessages());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

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
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_19_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_20_1() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

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
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_20_1_mixed() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

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
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_20_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      assertEqualSets(refs, r.getMessages());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_20_2_mixed() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      assertEqualSets(refs, r.getMessages());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_22() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_24() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_24_mixed() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(queue.browse().isEmpty());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      r.setImmediateAsynchronousAcknowledgment(true);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      // the receiver should have returned a "done" delivery
      assertTrue(queue.browse().isEmpty());

      List messages = r.getMessages();
      assertEquals(1, messages.size());
      Message ackm = (Message)messages.get(0);
      assertEquals(0, ackm.getMessageID());

      // an extra acknowledgment should be discarded
      r.acknowledge(ackm, null);

      assertTrue(queue.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testNonRecoverableChannel_25_1() throws Throwable
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      tx.commit();

      assertTrue(queue.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testNonRecoverableChannel_25_2() throws Throwable
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      tx.rollback();

      delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      // acknowledge non-transactionally
      r.acknowledge(ackm, null);

      assertTrue(queue.browse().isEmpty());
   }


   //////////
   ////////// Multiple message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testNonRecoverableChannel_26() throws Throwable
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse().isEmpty());

   }

   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testNonRecoverableChannel_26_1() throws Throwable
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(refs, queue.browse());

      tx.commit();

      assertTrue(queue.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testNonRecoverableChannel_26_2() throws Throwable
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(refs, queue.browse());

      tx.rollback();

      assertEqualSets(refs, queue.browse());

      // acknowledge non-transactionally
      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse().isEmpty());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      // not accepted by the channel
      assertNull(delivery);
      assertTrue(queue.browse().isEmpty());
      assertTrue(r.getMessages().isEmpty());

   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_27_1_2() throws Throwable
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List stored = queue.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message rm = (Message)received.iterator().next();
      assertTrue(rm.isReliable());
      assertEquals(0, rm.getMessageID());

      r.acknowledge(rm, null);

      assertTrue(queue.browse().isEmpty());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List stored = queue.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message rm = (Message)received.iterator().next();
      assertTrue(rm.isReliable());
      assertEquals(0, rm.getMessageID());

      Transaction tx = tr.createTransaction();

      r.acknowledge(rm, tx);

      stored = queue.browse();
      assertEquals(1, stored.size());
      sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      tx.commit();

      assertTrue(queue.browse().isEmpty());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         // not accepted by the channel
         assertNull(delivery);
      }

      assertTrue(queue.browse().isEmpty());
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_28_1_2() throws Throwable
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse().isEmpty());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(refs, queue.browse());

      tx.commit();

      assertTrue(queue.browse().isEmpty());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      List delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(queue.browse().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_30() throws Throwable
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse().isEmpty());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

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
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel does accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_31_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      List stored = queue.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message rm = (Message)received.iterator().next();
      assertTrue(rm.isReliable());
      assertEquals(0, rm.getMessageID());
   }


   //////////
   ////////// Multiple message
   //////////

   ///////////
   /////////// Channel does NOT accept reliable messages
   ///////////

   public void testNonRecoverableChannel_32_1() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

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
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_32_1_mixed() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (queue.acceptReliableMessages())
      {
         // we test channels that don't accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

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
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   ///////////
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_32_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_32_2_mixed() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      if (!queue.acceptReliableMessages())
      {
         // we test channels that accept reliable messages
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());
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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_34() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testNonRecoverableChannel_36() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testNonRecoverableChannel_36_mixed() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }




   //
   // Recoverable channel
   //

   public void testRecoverableChannel_0() throws Exception
   {
      if (queue.isRecoverable())
      {
         assertTrue(queue.acceptReliableMessages());
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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List stored = queue.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_2() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_3() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List stored = queue.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_4() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());

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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.commit();

      List stored = queue.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_6() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.commit();

      assertEqualSets(refs, queue.browse());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_7() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.commit();

      List stored = queue.browse();
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_8() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.commit();

      assertEqualSets(refs, queue.browse());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableChannel_8_mixed() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.commit();

      assertEqualSets(refs, queue.browse());
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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse().size());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_10() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse().size());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_11() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse().size());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_12() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse().size());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableChannel_12_mixed() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse().size());
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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());
      assertTrue(queue.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_14() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertTrue(queue.browse().isEmpty());

      List received = r.getMessages();
      assertEqualSets(refs, received);
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_15() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      assertTrue(queue.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_16() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertTrue(queue.browse().isEmpty());
      assertEqualSets(refs, r.getMessages());
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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(queue.browse().isEmpty());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }


   public void testRecoverableChannel_17_1() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      BrokenReceiver brokenReceiver = new BrokenReceiver(2);
      assertTrue(queue.add(brokenReceiver));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();


      log.debug("sending message 1");

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      ref = createReference(1, false, "payload");

      log.debug("sending message 2");
      queue.handle(observer, ref, tx);

      ref = createReference(2, false, "payload");

      log.debug("sending message 3");
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      tx.commit();

      assertEquals(2, queue.browse().size());
      assertEquals(1, brokenReceiver.getMessages().size());
   }


   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_18() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(queue.browse().isEmpty());
      assertEqualSets(refs, r.getMessages());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_19() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_20() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      assertEqualSets(refs, r.getMessages());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableChannel_20_mixed() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      assertEqualSets(refs, r.getMessages());
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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_22() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_24() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableChannel_24_mixed() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(queue.browse().isEmpty());
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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      r.setImmediateAsynchronousAcknowledgment(true);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      // the receiver should have returned a "done" delivery
      assertTrue(queue.browse().isEmpty());

      List messages = r.getMessages();
      assertEquals(1, messages.size());
      Message ackm = (Message)messages.get(0);
      assertEquals(0, ackm.getMessageID());

      // an extra acknowledgment should be discarded
      r.acknowledge(ackm, null);

      assertTrue(queue.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testRecoverableChannel_25_1() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      tx.commit();

      assertTrue(queue.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testRecoverableChannel_25_2() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      tx.rollback();

      delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      // acknowledge non-transactionally
      r.acknowledge(ackm, null);

      assertTrue(queue.browse().isEmpty());
   }


   //////////
   ////////// Multiple message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testRecoverableChannel_26() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse().isEmpty());

   }

   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testRecoverableChannel_26_1() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(refs, queue.browse());

      tx.commit();

      assertTrue(queue.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testRecoverableChannel_26_2() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(refs, queue.browse());

      tx.rollback();

      assertEqualSets(refs, queue.browse());

      // acknowledge non-transactionally
      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse().isEmpty());
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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(queue.browse().isEmpty());
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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      r.setImmediateAsynchronousAcknowledgment(true);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      // the receiver should have returned a "done" delivery
      assertTrue(queue.browse().isEmpty());

      List messages = r.getMessages();
      assertEquals(1, messages.size());
      Message ackm = (Message)messages.get(0);
      assertEquals(0, ackm.getMessageID());

      // an extra acknowledgment should be discarded
      
      //TODO - why should it be discarded?
      //If you acknowledge twice surely this is a usage error?
      r.acknowledge(ackm, null);

      assertTrue(queue.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testRecoverableChannel_27_1() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      tx.commit();

      assertTrue(queue.browse().isEmpty());
   }


   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testRecoverableChannel_27_2() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(delivery.isDone());

      List delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      tx.rollback();

      delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      // acknowledge non-transactionally
      r.acknowledge(ackm, null);

      assertTrue(queue.browse().isEmpty());
   }


   //////////
   ////////// Multiple message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////

   public void testRecoverableChannel_28() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and commit
   ////////////

   public void testRecoverableChannel_28_1() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(refs, queue.browse());

      tx.commit();

      assertTrue(queue.browse().isEmpty());
   }

   ////////////
   //////////// Transacted acknowledgment and rollback
   ////////////

   public void testRecoverableChannel_28_2() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

         assertTrue(delivery.isDone());
      }

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEqualSets(refs, queue.browse());

      tx.rollback();

      assertEqualSets(refs, queue.browse());

      // acknowledge non-transactionally
      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse().isEmpty());
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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      List delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(queue.browse().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_30() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse().isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableChannel_31() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      List delivering = queue.browse();
      assertEquals(1, delivering.size());
      assertEquals(0, ((Message)delivering.get(0)).getMessageID());

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(queue.browse().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_32() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel yet
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableChannel_32_mixed() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }


      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(refs, queue.browse());
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse().isEmpty());
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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_34() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // transacted send, non-reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

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
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   public void testRecoverableChannel_36() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // transacted send, reliable message, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
   }

   /**
    * This is a variation where I send a mixture of reliable and non-reliable messages,
    */
   public void testRecoverableChannel_36_mixed() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.add(r));


      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // send a mixture of reliable and non-reliable messages
         refs[i] = createReference(i, (i % 2 == 1), "payload" + i);

         // transacted send, reliable/non-reliable messages, multiple messages
         // for a transactional send, handle() return value is unspecified
         queue.handle(observer, refs[i], tx);
      }

      // no messages in the channel
      assertEquals(0, queue.browse().size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse().size());

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
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = queue.handle(observer, ref, null);
      assertTrue(delivery.isDone());

      List stored = queue.browse();
      assertEquals(1, stored.size());


      SimpleReceiver receiver = new SimpleReceiver("BrokenReceiver", SimpleReceiver.BROKEN);
      assertTrue(queue.add(receiver));

      stored = queue.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      assertTrue(receiver.getMessages().isEmpty());
   }

   //////
   ////// ACKING receiver
   //////

   public void testAddReceiver_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = queue.handle(observer, ref, null);
      assertTrue(delivery.isDone());

      List stored = queue.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("ACKINGReceiver", SimpleReceiver.ACKING, queue);
      assertTrue(queue.add(receiver));

      assertEquals(1, queue.browse().size());

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertTrue(queue.browse().isEmpty());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////
   ////// NACKING receiver
   //////

   public void testAddReceiver_3() throws Throwable
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = queue.handle(observer, ref, null);
      assertTrue(delivery.isDone());

      List stored = queue.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("NACKINGReceiver", SimpleReceiver.ACCEPTING, queue);
      assertTrue(queue.add(receiver));

      assertEquals(1, queue.browse().size());

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertEquals(1, queue.browse().size());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      receiver.acknowledge(sm, null);

      assertTrue(queue.browse().isEmpty());

      messages = receiver.getMessages();
      assertEquals(1, messages.size());
      sm = (Message)messages.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //
   // Recoverable channel
   //

   ////
   //// Reliable message
   ////

   public void testAddReceiver_4() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = queue.handle(observer, ref, null);
      assertTrue(delivery.isDone());

      List stored = queue.browse();
      assertEquals(1, stored.size());


      SimpleReceiver receiver = new SimpleReceiver("BrokenReceiver", SimpleReceiver.BROKEN);
      assertTrue(queue.add(receiver));

      stored = queue.browse();
      assertEquals(1, stored.size());
      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      assertTrue(receiver.getMessages().isEmpty());
   }

   //////
   ////// ACKING receiver
   //////

   public void testAddReceiver_5() throws Exception
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = queue.handle(observer, ref, null);
      assertTrue(delivery.isDone());

      List stored = queue.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("ACKINGReceiver", SimpleReceiver.ACKING, queue);
      assertTrue(queue.add(receiver));

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertTrue(queue.browse().isEmpty());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////
   ////// NACKING receiver
   //////

   public void testAddReceiver_6() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = queue.handle(observer, ref, null);
      assertTrue(delivery.isDone());

      List stored = queue.browse();
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("NACKINGReceiver", SimpleReceiver.ACCEPTING, queue);
      assertTrue(queue.add(receiver));

      assertEquals(1, queue.browse().size());

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertEquals(1, queue.browse().size());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      receiver.acknowledge(sm, null);

      assertTrue(queue.browse().isEmpty());

      messages = receiver.getMessages();
      assertEquals(1, messages.size());
      sm = (Message)messages.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());
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
//      Delivery d = channel.handle(observer, ref, null);
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

      assertTrue(queue.add(r));
      assertFalse(queue.add(r));

      assertTrue(queue.contains(r));

      Iterator i = queue.iterator();
      assertEquals(r, i.next());
      assertFalse(i.hasNext());

      queue.clear();
      assertFalse(queue.iterator().hasNext());
   }

   public void testRemoveInexistentReceiver()
   {
      assertFalse(queue.remove(new SimpleReceiver("INEXISTENT")));
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected abstract void crashChannel() throws Exception;

   protected abstract void recoverChannel() throws Exception;

   // Private -------------------------------------------------------
   
   private MessageReference createReference(long id, boolean reliable, Serializable payload)
   {
      return ms.reference(CoreMessageFactory.createCoreMessage(id, reliable, payload));
   }
   
   private MessageReference createReference(long id)
   {
      return ms.reference(CoreMessageFactory.createCoreMessage(id));
   }
   
   // Inner classes -------------------------------------------------

}
