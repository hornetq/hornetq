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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.contract.Receiver;
import org.jboss.messaging.core.impl.IDManager;
import org.jboss.messaging.core.impl.JDBCPersistenceManager;
import org.jboss.messaging.core.impl.MessagingQueue;
import org.jboss.messaging.core.impl.message.SimpleMessageStore;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.messaging.core.impl.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.container.ServiceContainer;
import org.jboss.test.messaging.util.CoreMessageFactory;

/**
 * The QueueTest test strategy is to try as many combination as it makes sense of the following
 * variables:
 *
 * 1. The Queue can be non-recoverable  or
 *    recoverable.
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
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1019 $</tt>
 *
 * $Id: ChannelTestBase.java 1019 2006-07-17 17:15:04Z timfox $
 */
public abstract class MessagingQueueTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   public static final int NUMBER_OF_MESSAGES = 10;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected PersistenceManager pm;
   
   protected TransactionRepository tr;
   
   protected MessageStore ms;
   
   protected ServiceContainer sc;

   protected MessagingQueue queue;
   
   protected IDManager idm;
   
   // Constructors --------------------------------------------------

   public MessagingQueueTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      
      sc = new ServiceContainer("all,-remoting,-security");
      sc.start();

      pm = new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(),
                                      sc.getPersistenceManagerSQLProperties(),
                                      true, true, true, false, 100, 5000, 0, true);
      ((JDBCPersistenceManager)pm).injectNodeID(1);
      pm.start();
      
      idm = new IDManager("TRANSACTION_ID", 10, pm);
      idm.start();
      
      ms = new SimpleMessageStore();
      ms.start();
      
      tr = new TransactionRepository(pm, ms, idm);
      tr.start();
      
   }

   public void tearDown() throws Exception
   {
      sc.stop();
      
      pm.stop();
      idm.stop();
      tr.stop();
      ms.stop();
      
      sc = null;   
      pm = null;
      idm = null;
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
            
            if (a[i].getMessage().getMessageID() == m.getMessageID() &&
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

            if (a[i].getReference().getMessage().getMessageID() == ref.getMessage().getMessageID())
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
      
      queue.getLocalDistributor().add(r1);
      queue.getLocalDistributor().add(r2);
      
      Delivery d = queue.handle(observer, createReference(0, false, "payload"), null);
      
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
      assertTrue(queue.getLocalDistributor().add(r1));
      assertTrue(queue.getLocalDistributor().add(r2));

      Delivery d = queue.handle(observer, createReference(0, true, "payload"), null);

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
      
      assertFalse(queue.getLocalDistributor().iterator().hasNext());
      
      queue.getLocalDistributor().add(receiver1);
      
      assertTrue(queue.getLocalDistributor().contains(receiver1));
      
      queue.getLocalDistributor().remove(receiver1);
      
      assertFalse(queue.getLocalDistributor().iterator().hasNext());
      
      assertFalse(queue.getLocalDistributor().contains(receiver1));
      
      queue.getLocalDistributor().add(receiver1);
      
      assertTrue(queue.getLocalDistributor().contains(receiver1));
      
      queue.getLocalDistributor().remove(receiver2);
      
      assertTrue(queue.getLocalDistributor().contains(receiver1));
                 
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      List stored = queue.browse(null);
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      SimpleReceiver receiver = new SimpleReceiver("ACKING", SimpleReceiver.ACKING);
      queue.getLocalDistributor().add(receiver);
      queue.deliver();
      assertEquals(1, receiver.getMessages().size());
      assertEquals(0, ((Message)receiver.getMessages().get(0)).getMessageID());

      queue.deliver();
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      List stored = queue.browse(null);
      assertEqualSets(refs, stored);

      SimpleReceiver receiver = new SimpleReceiver("ACKING", SimpleReceiver.ACKING);
      queue.getLocalDistributor().add(receiver);
      queue.deliver();
      assertEquals(10, receiver.getMessages().size());
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         assertEquals(i, ((Message)receiver.getMessages().get(i)).getMessageID());         
      }
      receiver.clear();

      queue.deliver();
      assertEquals(0, receiver.getMessages().size());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////


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

      // the channel has no receivers
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      
      log.trace("ref is reliable:" + ref.getMessage().isReliable());

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      List stored = queue.browse(null);
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      queue.deliver();

      SimpleReceiver receiver = new SimpleReceiver("ACKING", SimpleReceiver.ACKING);
      queue.getLocalDistributor().add(receiver);
      queue.deliver();
      assertEquals(1, receiver.getMessages().size());
      assertEquals(0, ((Message)receiver.getMessages().get(0)).getMessageID());

      queue.deliver();
      assertEquals(1, receiver.getMessages().size());
   }

   //////////
   ////////// Multiple message
   //////////

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

      // the channel has no receivers
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEqualSets(refs, queue.browse(null));

      SimpleReceiver receiver = new SimpleReceiver("ACKING", SimpleReceiver.ACKING);
      queue.getLocalDistributor().add(receiver);
      queue.deliver();
      assertEquals(10, receiver.getMessages().size());
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         assertEquals(i, ((Message)receiver.getMessages().get(i)).getMessageID());         
      }
      receiver.clear();

      queue.deliver();
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse(null).size());

      tx.commit();

      List stored = queue.browse(null);
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.commit();

      List stored = queue.browse(null);
      assertEqualSets(refs, stored);
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////


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

      // the channel has no receivers
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse(null).size());

      tx.commit();

      List stored = queue.browse(null);
      assertEquals(1, stored.size());

      Message sm = (Message)stored.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   
   public void testNonRecoverableChannel_8_1_mixed_1() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // the channel has no receivers
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.commit();
        
      assertEqualSets(refs, queue.browse(null));
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

      // the channel has no receivers
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.commit();

      assertEqualSets(refs, queue.browse(null));
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

      // the channel has no receivers
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.commit();

      assertEqualSets(refs, queue.browse(null));
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse(null).size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse(null).size());
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse(null).size());
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse(null).size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse(null).size());
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse(null).size());
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse(null).size());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      queue.handle(observer, ref, null);

      assertTrue(queue.browse(null).isEmpty());

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
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertTrue(queue.browse(null).isEmpty());

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
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_15_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(queue.browse(null).isEmpty());

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
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_16_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(queue.browse(null).isEmpty());

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
      assertTrue(queue.getLocalDistributor().add(r));

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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(queue.browse(null).isEmpty());
      assertEqualSets(refs, r.getMessages());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

 

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

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.getLocalDistributor().add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message sm = (Message)received.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   //////////
   ////////// Multiple message
   //////////

   

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

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // messages at the receiver
      assertEqualSets(refs, r.getMessages());
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

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.getLocalDistributor().add(r));

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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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

      // add an ACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("AckingReceiver", SimpleReceiver.ACKING);
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));

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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      // the receiver should have returned a "done" delivery
      assertTrue(queue.browse(null).isEmpty());

      List messages = r.getMessages();
      assertEquals(1, messages.size());
      Message ackm = (Message)messages.get(0);
      assertEquals(0, ackm.getMessageID());

      // an extra acknowledgment should be discarded
      r.acknowledge(ackm, null);

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      tx.commit();

      assertTrue(queue.browse(null).isEmpty());
      
      deliveringCount = queue.getDeliveringCount();
      assertEquals(0, deliveringCount);
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      tx.rollback();

      deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      // acknowledge non-transactionally
      r.acknowledge(ackm, null);

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse(null).isEmpty());

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
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      tx.commit();

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      tx.rollback();

      // acknowledge non-transactionally
      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse(null).isEmpty());
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
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_27_1_2() throws Throwable
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message rm = (Message)received.iterator().next();
      assertTrue(rm.isReliable());
      assertEquals(0, rm.getMessageID());

      r.acknowledge(rm, null);

      assertTrue(queue.browse(null).isEmpty());
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

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List received = r.getMessages();
      assertEquals(1, received.size());
      Message rm = (Message)received.iterator().next();
      assertTrue(rm.isReliable());
      assertEquals(0, rm.getMessageID());

      Transaction tx = tr.createTransaction();

      r.acknowledge(rm, tx);

      deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      tx.commit();

      assertTrue(queue.browse(null).isEmpty());
   }

   //////////
   ////////// Multiple message
   //////////

   ////////////
   //////////// Non-transacted acknowledgment
   ////////////


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

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse(null).isEmpty());
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

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         r.acknowledge(ackm, tx);
      }

      tx.commit();

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse(null).isEmpty());
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

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

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.getLocalDistributor().add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);      

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
   /////////// Channel accepts reliable messages
   ///////////

   public void testNonRecoverableChannel_32_2() throws Exception
   {
      if (queue.isRecoverable())
      {
         // we test only non-recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.getLocalDistributor().add(r));

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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(NUMBER_OF_MESSAGES, deliveringCount);
      
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

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(NUMBER_OF_MESSAGES, deliveringCount);
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
      assertTrue(queue.getLocalDistributor().add(r));


      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));

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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      List stored = queue.browse(null);
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);

      }

      assertEqualSets(refs, queue.browse(null));
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      List stored = queue.browse(null);
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEqualSets(refs, queue.browse(null));

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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse(null).size());

      tx.commit();

      List stored = queue.browse(null);
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.commit();

      assertEqualSets(refs, queue.browse(null));
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse(null).size());

      tx.commit();

      List stored = queue.browse(null);
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.commit();

      assertEqualSets(refs, queue.browse(null));
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.commit();

      assertEqualSets(refs, queue.browse(null));
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse(null).size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse(null).size());
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse(null).size());
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse(null).size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse(null).size());
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse(null).size());
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

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
      assertEquals(0, queue.browse(null).size());

      tx.rollback();

      // still no messages in the channel
      assertEquals(0, queue.browse(null).size());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(queue.browse(null).isEmpty());

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
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertTrue(queue.browse(null).isEmpty());

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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      assertTrue(queue.browse(null).isEmpty());

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
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(queue.browse(null).isEmpty());

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
      assertTrue(queue.getLocalDistributor().add(brokenReceiver));

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
      assertEquals(0, queue.browse(null).size());

      tx.commit();

      assertEquals(2, queue.browse(null).size());
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
      assertTrue(queue.getLocalDistributor().add(r));

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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));

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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));

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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);
      
      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      // the receiver should have returned a "done" delivery
      assertTrue(queue.browse(null).isEmpty());

      List messages = r.getMessages();
      assertEquals(1, messages.size());
      Message ackm = (Message)messages.get(0);
      assertEquals(0, ackm.getMessageID());

      // an extra acknowledgment should be discarded
      r.acknowledge(ackm, null);

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      tx.commit();

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, non-reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      tx.rollback();

      deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      // acknowledge non-transactionally
      r.acknowledge(ackm, null);

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse(null).isEmpty());

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
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());
      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());

      tx.commit();

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, false, "payload" + i);

         // non-transacted send, non-reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());
      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());

      tx.rollback();

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());

      // acknowledge non-transactionally
      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(queue.browse(null).isEmpty());
   }

   /**
    * Test duplicate acknowledgment.
    */
   public void testRecoverableChannel_27_Duplicate_ACK() throws Throwable
   {
      if (!queue.isRecoverable())
      {
         // we test only recoverable channels now
         return;
      }

      // add an NACKING receiver to the channel
      SimpleReceiver r = new SimpleReceiver("NackingReceiver", SimpleReceiver.ACCEPTING);
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      queue.handle(observer, ref, null);

      Message ackm = (Message)r.getMessages().get(0);

      // acknowledge once
      r.acknowledge(ackm, null);

      assertTrue(queue.browse(null).isEmpty());

      // acknowledge twice
      try
      {
         r.acknowledge(ackm, null);
      }
      catch(IllegalStateException e)
      {
         // OK
      }

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      // the receiver should have returned a "done" delivery
      assertTrue(queue.browse(null).isEmpty());

      List messages = r.getMessages();
      assertEquals(1, messages.size());
      Message ackm = (Message)messages.get(0);
      assertEquals(0, ackm.getMessageID());

      // Acknowledgment handling implemenation is NOT idempotent, the channel DOES NOT allow
      // extraneous duplicate acknowlegments, so we test for that.

      try
      {
         r.acknowledge(ackm, null);
      }
      catch(IllegalStateException e)
      {
         // OK
         log.trace(e);

      }

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      tx.commit();

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-transacted send, reliable message, one message
      Delivery delivery = queue.handle(observer, ref, null);

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      Transaction tx = tr.createTransaction();

      // transacted acknowledgment
      r.acknowledge(ackm, tx);

      deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      tx.rollback();

      deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      // acknowledge non-transactionally
      r.acknowledge(ackm, null);

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());
      
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());
      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());

      tx.commit();

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = createReference(i, true, "payload" + i);

         // non-transacted send, reliable message, multiple messages
         Delivery delivery = queue.handle(observer, refs[i], null);
      }

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());
      assertEqualSets(refs, r.getMessages());

      Transaction tx = tr.createTransaction();

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // transacted acknowledgment
         r.acknowledge(ackm, tx);
      }

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());

      tx.rollback();

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());

      // acknowledge non-transactionally
      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel yet
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List acknowledging = r.getMessages();
      assertEquals(1, acknowledging.size());
      Message ackm = (Message)acknowledging.get(0);
      assertEquals(0, ackm.getMessageID());

      // non-transacted acknowledgment
      r.acknowledge(ackm, null);

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));

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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();
      
      
      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.commit();
      
      assertEquals(NUMBER_OF_MESSAGES, queue.getDeliveringCount());
      assertEqualSets(refs, r.getMessages());

      for(Iterator i = r.getMessages().iterator(); i.hasNext();)
      {
         Message ackm = (Message)i.next();
         // non-transacted acknowledgment
         r.acknowledge(ackm, null);
      }

      assertTrue(queue.browse(null).isEmpty());
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
      assertTrue(queue.getLocalDistributor().add(r));


      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, non-reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());


      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));

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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      Transaction tx = tr.createTransaction();

      // transacted send, reliable message, one message
      // for a transactional send, handle() return value is unspecified
      queue.handle(observer, ref, tx);

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertTrue(queue.getLocalDistributor().add(r));


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
      assertEquals(0, queue.browse(null).size());

      // no message at the receiver
      assertTrue(r.getMessages().isEmpty());

      tx.rollback();

      // no messages in the channel
      assertEquals(0, queue.browse(null).size());

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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = queue.handle(observer, ref, null);

      List stored = queue.browse(null);
      assertEquals(1, stored.size());


      SimpleReceiver receiver = new SimpleReceiver("BrokenReceiver", SimpleReceiver.BROKEN);
      assertTrue(queue.getLocalDistributor().add(receiver));

      stored = queue.browse(null);
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = queue.handle(observer, ref, null);

      List stored = queue.browse(null);
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("ACKINGReceiver", SimpleReceiver.ACKING, queue);
      assertTrue(queue.getLocalDistributor().add(receiver));

      assertEquals(1, queue.browse(null).size());

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertTrue(queue.browse(null).isEmpty());

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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, false, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = queue.handle(observer, ref, null);

      List stored = queue.browse(null);
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("NACKINGReceiver", SimpleReceiver.ACCEPTING, queue);
      assertTrue(queue.getLocalDistributor().add(receiver));

      assertEquals(1, queue.browse(null).size());

      // receiver explicitely asks for message
      receiver.requestMessages();

      int deliveringCount = queue.getDeliveringCount();
      assertEquals(1, deliveringCount);

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertFalse(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      receiver.acknowledge(sm, null);

      assertTrue(queue.browse(null).isEmpty());

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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = queue.handle(observer, ref, null);

      List stored = queue.browse(null);
      assertEquals(1, stored.size());


      SimpleReceiver receiver = new SimpleReceiver("BrokenReceiver", SimpleReceiver.BROKEN);
      assertTrue(queue.getLocalDistributor().add(receiver));

      stored = queue.browse(null);
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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = queue.handle(observer, ref, null);

      List stored = queue.browse(null);
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("ACKINGReceiver", SimpleReceiver.ACKING, queue);
      assertTrue(queue.getLocalDistributor().add(receiver));

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertTrue(queue.browse(null).isEmpty());

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
      assertFalse(queue.getLocalDistributor().iterator().hasNext());

      MessageReference ref = createReference(0, true, "payload");
      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();

      // non-recoverable channel, non-reliable message
      Delivery delivery = queue.handle(observer, ref, null);

      List stored = queue.browse(null);
      assertEquals(1, stored.size());

      SimpleReceiver receiver =
            new SimpleReceiver("NACKINGReceiver", SimpleReceiver.ACCEPTING, queue);
      assertTrue(queue.getLocalDistributor().add(receiver));

      assertEquals(1, queue.browse(null).size());

      // receiver explicitely asks for message
      receiver.requestMessages();

      assertEquals(1, queue.getDeliveringCount());

      List messages = receiver.getMessages();
      assertEquals(1, messages.size());
      Message sm = (Message)messages.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());

      receiver.acknowledge(sm, null);

      assertTrue(queue.browse(null).isEmpty());

      messages = receiver.getMessages();
      assertEquals(1, messages.size());
      sm = (Message)messages.iterator().next();
      assertTrue(sm.isReliable());
      assertEquals(0, sm.getMessageID());
   }

   // Distributor tests ---------------------------------------------

   public void testAddOneReceiver()
   {
      Receiver r = new SimpleReceiver("ONE");

      assertTrue(queue.getLocalDistributor().add(r));
      assertFalse(queue.getLocalDistributor().add(r));

      assertTrue(queue.getLocalDistributor().contains(r));

      Iterator i = queue.getLocalDistributor().iterator();
      assertEquals(r, i.next());
      assertFalse(i.hasNext());

      queue.getLocalDistributor().clear();
      assertFalse(queue.getLocalDistributor().iterator().hasNext());
   }

   public void testRemoveInexistentReceiver()
   {
      assertFalse(queue.getLocalDistributor().remove(new SimpleReceiver("INEXISTENT")));
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

//   protected abstract void crashChannel() throws Exception;
//
//   protected abstract void recoverChannel() throws Exception;

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
