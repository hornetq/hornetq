/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.base;

import java.util.List;

import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.State;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.persistence.JDBCPersistenceManager;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;


/**
 * The State test strategy is to try as many combination as it makes sense of the following
 * variables:
 *
 * 1. State can be non-recoverable (does not have access to a PersistenceManager) or
 *    recoverable. A non-recoverable state can accept reliable messages or not.
 * 2. Messages can be added non-transactionally or transactionally (and then can commit or rollback
 *    transaction).
 * 3. A message can be non-reliable or reliable.
 * 4. One or multiple messages can be added.
 * 5. Deliveries can be added non-transactionally or transactionally (and then can commit or
 *    rollback transaction).
 * 6. A Delivery can be non-reliable or reliable.
 * 7. One or multiple deliveries can be added.
 * 8. A recoverable channel may be crashed and tested if it successfully recovers.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class StateTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   public static final int NUMBER_OF_MESSAGES = 10;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServiceContainer sc;
   protected TransactionRepository tr;
   protected PersistenceManager pm;
   protected MessageStore ms;
   protected State state;
   protected Channel channel;

   // Constructors --------------------------------------------------

   public StateTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      sc = new ServiceContainer("all,-aop,-remoting,-security");
      sc.start();

      pm = new JDBCPersistenceManager();
      pm.start();
      tr = new TransactionRepository(pm);

      // message store to be initialized by subclasses
      // state to be initialized by subclasses
   }

   public void tearDown() throws Exception
   {
      tr = null;
      pm = null;
      sc.stop();
      sc = null;
      ms = null;

      super.tearDown();
   }

   //
   // Non-recoverable state
   //

   ////
   //// Does not accept reliable messages
   ////

   //////
   ////// Message non-transactional add
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableState_1() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));

      // non-recoverable state, unreliable message, non-transactional add
      state.add(ref, null);

      assertTrue(state.delivering(null).isEmpty());

      List l = state.undelivered(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }


   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableState_2() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }


      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));

         // non-recoverable state, unreliable message, non-transactional add
         state.add(refs[i], null);
      }

      assertTrue(state.delivering(null).isEmpty());

      ChannelTestBase.assertEqualSets(refs, state.undelivered(null));
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }


   //////
   ////// Message transactional add and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableState_3() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));

      Transaction tx = tr.createTransaction();

      // non-recoverable state, unreliable message, transactional add
      state.add(ref, tx);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());

      List l = state.undelivered(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableState_4() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));

         // non-recoverable state, unreliable message, transactional add
         state.add(refs[i], tx);
      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.undelivered(null));
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   //////
   ////// Message transactional add and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   public void testNonRecoverableState_5() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));

      Transaction tx = tr.createTransaction();

      // non-recoverable state, unreliable message, transactional add
      state.add(ref, tx);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.rollback();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableState_6() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));

         // non-recoverable state, unreliable message, transactional add
         state.add(refs[i], tx);
      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.rollback();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   //////
   ////// remove()
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableState_remove_1() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      state.add(ref, null);


      MessageReference rref = state.remove();
      assertEquals("message0", rref.getMessageID());

      assertNull(state.remove());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableState_remove_2() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         state.add(ref, null);
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref = state.remove();
         assertEquals("message" + i, ref.getMessageID());
      }

      assertNull(state.remove());

      log.info("ok");
   }

   //////
   ////// Delivery add (can only be non-transactional)
   //////

   ////////
   //////// Non-reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testNonRecoverableState_7() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);

      // non-recoverable state, unreliable delivery, non-transactional add
      state.add(d);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }


   //////////
   ////////// Multiple deliveries
   //////////

   public void testNonRecoverableState_8() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         Delivery d = new SimpleDelivery(channel, refs[i], false);

         // non-recoverable state, unreliable delivery, non-transactional add
         state.add(d);
      }

      ChannelTestBase.assertEqualSets(refs, state.delivering(null));
      assertTrue(state.undelivered(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   //////
   ////// Non-transactional delivery remove
   //////

   ////////
   //////// Non-reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testNonRecoverableState_9() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);

      state.add(d);

      // non-recoverable state, unreliable delivery, non-transactional remove
      state.remove(d, null);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }


   //////////
   ////////// Multiple deliveries
   //////////

   public void testNonRecoverableState_10() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }


      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, ref, false);
         state.add(deliveries[i]);
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // non-recoverable state, unreliable delivery, non-transactional remove
         state.remove(deliveries[i], null);

      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }


   //////
   ////// Transactional delivery remove and commit
   //////

   ////////
   //////// Non-reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testNonRecoverableState_11() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);
      state.add(d);

      Transaction tx = tr.createTransaction();

      // non-recoverable state, unreliable delivery, transactional remove
      state.remove(d, tx);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   //////////
   ////////// Multiple deliveries
   //////////

   public void testNonRecoverableState_12() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }


      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, ref, false);
         state.add(deliveries[i]);
      }

      Transaction tx = tr.createTransaction();

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // non-recoverable state, unreliable delivery, transactional remove
         state.remove(deliveries[i], tx);

      }

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }


   //////
   ////// Transactional delivery remove and rollback
   //////

   ////////
   //////// Non-reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testNonRecoverableState_13() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);
      state.add(d);

      Transaction tx = tr.createTransaction();

      // non-recoverable state, unreliable delivery, transactional remove
      state.remove(d, tx);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      tx.rollback();

      l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }

   //////////
   ////////// Multiple deliveries
   //////////

   public void testNonRecoverableState_14() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (state.acceptReliableMessages())
      {
         // only state that does not accept reliable messages is tested now
         return;
      }


      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] =  ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, refs[i], false);
         state.add(deliveries[i]);
      }

      Transaction tx = tr.createTransaction();

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // non-recoverable state, unreliable delivery, transactional remove
         state.remove(deliveries[i], tx);

      }

      tx.rollback();

      ChannelTestBase.assertEqualSets(refs, state.delivering(null));
      assertTrue(state.undelivered(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   ////
   //// Accepts reliable messages
   ////

   //////
   ////// Message non-transactional add
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableState_15() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));

      // non-recoverable state, unreliable message, non-transactional add
      state.add(ref, null);

      assertTrue(state.delivering(null).isEmpty());

      List l = state.undelivered(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }


   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableState_16() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));

         // non-recoverable state, unreliable message, non-transactional add
         state.add(refs[i], null);
      }

      assertTrue(state.delivering(null).isEmpty());

      ChannelTestBase.assertEqualSets(refs, state.undelivered(null));
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableState_17() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));

      // non-recoverable state, reliable message, non-transactional add
      state.add(ref, null);

      assertTrue(state.delivering(null).isEmpty());

      List l = state.undelivered(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }


   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableState_18() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));

         // non-recoverable state, reliable message, non-transactional add
         state.add(refs[i], null);
      }

      assertTrue(state.delivering(null).isEmpty());

      ChannelTestBase.assertEqualSets(refs, state.undelivered(null));
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   //////
   ////// Message transactional add and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableState_19() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));

      Transaction tx = tr.createTransaction();

      // non-recoverable state, unreliable message, transactional add
      state.add(ref, tx);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());

      List l = state.undelivered(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableState_20() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));

         // non-recoverable state, unreliable message, transactional add
         state.add(refs[i], tx);
      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.undelivered(null));
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableState_21() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));

      Transaction tx = tr.createTransaction();

      // non-recoverable state, reliable message, transactional add
      state.add(ref, tx);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());

      List l = state.undelivered(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableState_22() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));

         // non-recoverable state, reliable message, transactional add
         state.add(refs[i], tx);
      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.undelivered(null));
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }


   //////
   ////// Message transactional add and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   public void testNonRecoverableState_23() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));

      Transaction tx = tr.createTransaction();

      // non-recoverable state, unreliable message, transactional add
      state.add(ref, tx);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.rollback();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableState_24() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));

         // non-recoverable state, unreliable message, transactional add
         state.add(refs[i], tx);
      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.rollback();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   ////////
   //////// Reliable message
   ////////

   public void testNonRecoverableState_25() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));

      Transaction tx = tr.createTransaction();

      // non-recoverable state, reliable message, transactional add
      state.add(ref, tx);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.rollback();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableState_26() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));

         // non-recoverable state, reliable message, transactional add
         state.add(refs[i], tx);
      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.rollback();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }


   //////
   ////// remove()
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableState_remove_3() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      state.add(ref, null);


      MessageReference rref = state.remove();
      assertEquals("message0", rref.getMessageID());

      assertNull(state.remove());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableState_remove_4() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         state.add(ref, null);
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref = state.remove();
         assertEquals("message" + i, ref.getMessageID());
      }

      assertNull(state.remove());

      log.info("ok");
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testNonRecoverableState_remove_5() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));
      state.add(ref, null);


      MessageReference rref = state.remove();
      assertEquals("message0", rref.getMessageID());

      assertNull(state.remove());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testNonRecoverableState_remove_6() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));
         state.add(ref, null);
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref = state.remove();
         assertEquals("message" + i, ref.getMessageID());
      }

      assertNull(state.remove());

      log.info("ok");
   }

   public void testNonRecoverableState_remove_6_mixed() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, i % 2 == 0, "payload" + i));
         state.add(ref, null);
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref = state.remove();
         assertEquals("message" + i, ref.getMessageID());
      }

      assertNull(state.remove());

      log.info("ok");
   }


   //////
   ////// Delivery add (can only be non-transactional)
   //////

   ////////
   //////// Non-reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testNonRecoverableState_27() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);

      // non-recoverable state, unreliable delivery, non-transactional add
      state.add(d);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }


   //////////
   ////////// Multiple deliveries
   //////////

   public void testNonRecoverableState_28() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         Delivery d = new SimpleDelivery(channel, refs[i], false);

         // non-recoverable state, unreliable delivery, non-transactional add
         state.add(d);
      }

      ChannelTestBase.assertEqualSets(refs, state.delivering(null));
      assertTrue(state.undelivered(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   ////////
   //////// Reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testNonRecoverableState_29() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);

      // non-recoverable state, reliable delivery, non-transactional add
      state.add(d);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }


   //////////
   ////////// Multiple deliveries
   //////////

   public void testNonRecoverableState_30() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));
         Delivery d = new SimpleDelivery(channel, refs[i], false);

         // non-recoverable state, reliable delivery, non-transactional add
         state.add(d);
      }

      ChannelTestBase.assertEqualSets(refs, state.delivering(null));
      assertTrue(state.undelivered(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }


   //////
   ////// Non-transactional delivery remove
   //////

   ////////
   //////// Non-reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testNonRecoverableState_31() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);

      state.add(d);

      // non-recoverable state, unreliable delivery, non-transactional remove
      state.remove(d, null);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }


   //////////
   ////////// Multiple deliveries
   //////////

   public void testNonRecoverableState_32() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }


      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, ref, false);
         state.add(deliveries[i]);
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // non-recoverable state, unreliable delivery, non-transactional remove
         state.remove(deliveries[i], null);

      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   ////////
   //////// Reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testNonRecoverableState_33() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);

      state.add(d);

      // non-recoverable state, reliable delivery, non-transactional remove
      state.remove(d, null);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }


   //////////
   ////////// Multiple deliveries
   //////////

   public void testNonRecoverableState_34() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }


      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, ref, false);
         state.add(deliveries[i]);
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // non-recoverable state, reliable delivery, non-transactional remove
         state.remove(deliveries[i], null);

      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }



   //////
   ////// Transactional delivery remove and commit
   //////

   ////////
   //////// Non-reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testNonRecoverableState_35() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);
      state.add(d);

      Transaction tx = tr.createTransaction();

      // non-recoverable state, unreliable delivery, transactional remove
      state.remove(d, tx);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   //////////
   ////////// Multiple deliveries
   //////////

   public void testNonRecoverableState_36() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }


      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, ref, false);
         state.add(deliveries[i]);
      }

      Transaction tx = tr.createTransaction();

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // non-recoverable state, unreliable delivery, transactional remove
         state.remove(deliveries[i], tx);

      }

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   ////////
   //////// Reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testNonRecoverableState_37() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);
      state.add(d);

      Transaction tx = tr.createTransaction();

      // non-recoverable state, reliable delivery, transactional remove
      state.remove(d, tx);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   //////////
   ////////// Multiple deliveries
   //////////

   public void testNonRecoverableState_38() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }


      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, ref, false);
         state.add(deliveries[i]);
      }

      Transaction tx = tr.createTransaction();

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // non-recoverable state, reliable delivery, transactional remove
         state.remove(deliveries[i], tx);

      }

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }



   //////
   ////// Transactional delivery remove and rollback
   //////

   ////////
   //////// Non-reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testNonRecoverableState_39() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);
      state.add(d);

      Transaction tx = tr.createTransaction();

      // non-recoverable state, unreliable delivery, transactional remove
      state.remove(d, tx);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      tx.rollback();

      l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }

   //////////
   ////////// Multiple deliveries
   //////////

   public void testNonRecoverableState_40() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }


      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] =  ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, refs[i], false);
         state.add(deliveries[i]);
      }

      Transaction tx = tr.createTransaction();

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // non-recoverable state, unreliable delivery, transactional remove
         state.remove(deliveries[i], tx);

      }

      tx.rollback();

      ChannelTestBase.assertEqualSets(refs, state.delivering(null));
      assertTrue(state.undelivered(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   ////////
   //////// Reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testNonRecoverableState_41() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);
      state.add(d);

      Transaction tx = tr.createTransaction();

      // non-recoverable state, reliable delivery, transactional remove
      state.remove(d, tx);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      tx.rollback();

      l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }

   //////////
   ////////// Multiple deliveries
   //////////

   public void testNonRecoverableState_42() throws Throwable
   {
      if (state.isRecoverable())
      {
         // only non-recoverable state is tested now
         return;
      }

      if (!state.acceptReliableMessages())
      {
         // only state that accepts reliable messages is tested now
         return;
      }


      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] =  ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, refs[i], false);
         state.add(deliveries[i]);
      }

      Transaction tx = tr.createTransaction();

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // non-recoverable state, reliable delivery, transactional remove
         state.remove(deliveries[i], tx);

      }

      tx.rollback();

      ChannelTestBase.assertEqualSets(refs, state.delivering(null));
      assertTrue(state.undelivered(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }




   //
   // Recoverable state
   //

   //////
   ////// Message non-transactional add
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableState_43() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));

      // recoverable state, unreliable message, non-transactional add
      state.add(ref, null);

      assertTrue(state.delivering(null).isEmpty());

      List l = state.undelivered(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }


   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableState_44() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));

         // recoverable state, unreliable message, non-transactional add
         state.add(refs[i], null);
      }

      assertTrue(state.delivering(null).isEmpty());

      ChannelTestBase.assertEqualSets(refs, state.undelivered(null));
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableState_45() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));

      // recoverable state, reliable message, non-transactional add
      state.add(ref, null);

      assertTrue(state.delivering(null).isEmpty());

      List l = state.undelivered(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }


   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableState_46() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));

         // recoverable state, reliable message, non-transactional add
         state.add(refs[i], null);
      }

      assertTrue(state.delivering(null).isEmpty());

      ChannelTestBase.assertEqualSets(refs, state.undelivered(null));
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   //////
   ////// Message transactional add and commit
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableState_47() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));

      Transaction tx = tr.createTransaction();

      // recoverable state, unreliable message, transactional add
      state.add(ref, tx);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());

      List l = state.undelivered(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableState_48() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));

         // recoverable state, unreliable message, transactional add
         state.add(refs[i], tx);
      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.undelivered(null));
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableState_49() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));

      Transaction tx = tr.createTransaction();

      // recoverable state, reliable message, transactional add
      state.add(ref, tx);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());

      List l = state.undelivered(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableState_50() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));

         // recoverable state, reliable message, transactional add
         state.add(refs[i], tx);
      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.undelivered(null));
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }


   //////
   ////// Message transactional add and rollback
   //////

   ////////
   //////// Non-reliable message
   ////////

   public void testRecoverableState_51() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));

      Transaction tx = tr.createTransaction();

      // recoverable state, unreliable message, transactional add
      state.add(ref, tx);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.rollback();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableState_52() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));

         // recoverable state, unreliable message, transactional add
         state.add(refs[i], tx);
      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.rollback();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   ////////
   //////// Reliable message
   ////////

   public void testRecoverableState_53() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));

      Transaction tx = tr.createTransaction();

      // recoverable state, reliable message, transactional add
      state.add(ref, tx);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.rollback();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableState_54() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }


      Transaction tx = tr.createTransaction();

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));

         // recoverable state, reliable message, transactional add
         state.add(refs[i], tx);
      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      tx.rollback();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   //////
   ////// remove()
   //////

   ////////
   //////// Non-reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableState_remove_7() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      state.add(ref, null);


      MessageReference rref = state.remove();
      assertEquals("message0", rref.getMessageID());

      assertNull(state.remove());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableState_remove_8() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         state.add(ref, null);
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref = state.remove();
         assertEquals("message" + i, ref.getMessageID());
      }

      assertNull(state.remove());

      log.info("ok");
   }

   ////////
   //////// Reliable message
   ////////

   //////////
   ////////// One message
   //////////

   public void testRecoverableState_remove_9() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));
      state.add(ref, null);


      MessageReference rref = state.remove();
      assertEquals("message0", rref.getMessageID());

      assertNull(state.remove());

      log.info("ok");
   }

   //////////
   ////////// Multiple messages
   //////////

   public void testRecoverableState_remove_10() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));
         state.add(ref, null);
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref = state.remove();
         assertEquals("message" + i, ref.getMessageID());
      }

      assertNull(state.remove());

      log.info("ok");
   }


   public void testRecoverableState_remove_10_mixed() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, i % 2 == 0, "payload" + i));
         state.add(ref, null);
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref = state.remove();
         assertEquals("message" + i, ref.getMessageID());
      }

      assertNull(state.remove());

      log.info("ok");
   }


   //////
   ////// Delivery add (can only be non-transactional)
   //////

   ////////
   //////// Non-reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testRecoverableState_55() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);

      // recoverable state, unreliable delivery, non-transactional add
      state.add(d);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }


   //////////
   ////////// Multiple deliveries
   //////////

   public void testRecoverableState_56() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         Delivery d = new SimpleDelivery(channel, refs[i], false);

         // recoverable state, unreliable delivery, non-transactional add
         state.add(d);
      }

      ChannelTestBase.assertEqualSets(refs, state.delivering(null));
      assertTrue(state.undelivered(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   ////////
   //////// Reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testRecoverableState_57() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);

      // recoverable state, reliable delivery, non-transactional add
      state.add(d);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }


   //////////
   ////////// Multiple deliveries
   //////////

   public void testRecoverableState_58() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] = ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));
         Delivery d = new SimpleDelivery(channel, refs[i], false);

         // recoverable state, reliable delivery, non-transactional add
         state.add(d);
      }

      ChannelTestBase.assertEqualSets(refs, state.delivering(null));
      assertTrue(state.undelivered(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }


   //////
   ////// Non-transactional delivery remove
   //////

   ////////
   //////// Non-reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testRecoverableState_59() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);

      state.add(d);

      // recoverable state, unreliable delivery, non-transactional remove
      state.remove(d, null);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }


   //////////
   ////////// Multiple deliveries
   //////////

   public void testRecoverableState_60() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }


      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, ref, false);
         state.add(deliveries[i]);
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // recoverable state, unreliable delivery, non-transactional remove
         state.remove(deliveries[i], null);

      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   ////////
   //////// Reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testRecoverableState_61() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);

      state.add(d);

      // recoverable state, reliable delivery, non-transactional remove
      state.remove(d, null);

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }


   //////////
   ////////// Multiple deliveries
   //////////

   public void testRecoverableState_62() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, ref, false);
         state.add(deliveries[i]);
      }

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // recoverable state, reliable delivery, non-transactional remove
         state.remove(deliveries[i], null);

      }

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }



   //////
   ////// Transactional delivery remove and commit
   //////

   ////////
   //////// Non-reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testRecoverableState_63() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);
      state.add(d);

      Transaction tx = tr.createTransaction();

      // recoverable state, unreliable delivery, transactional remove
      state.remove(d, tx);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   //////////
   ////////// Multiple deliveries
   //////////

   public void testRecoverableState_64() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, ref, false);
         state.add(deliveries[i]);
      }

      Transaction tx = tr.createTransaction();

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // recoverable state, unreliable delivery, transactional remove
         state.remove(deliveries[i], tx);

      }

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   ////////
   //////// Reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testRecoverableState_65() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);
      state.add(d);

      Transaction tx = tr.createTransaction();

      // recoverable state, reliable delivery, transactional remove
      state.remove(d, tx);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }

   //////////
   ////////// Multiple deliveries
   //////////

   public void testRecoverableState_66() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         MessageReference ref =
               ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, ref, false);
         state.add(deliveries[i]);
      }

      Transaction tx = tr.createTransaction();

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // recoverable state, reliable delivery, transactional remove
         state.remove(deliveries[i], tx);

      }

      tx.commit();

      assertTrue(state.delivering(null).isEmpty());
      assertTrue(state.undelivered(null).isEmpty());
      assertTrue(state.browse(null).isEmpty());

      log.info("ok");
   }



   //////
   ////// Transactional delivery remove and rollback
   //////

   ////////
   //////// Non-reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testRecoverableState_67() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", false, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);
      state.add(d);

      Transaction tx = tr.createTransaction();

      // recoverable state, unreliable delivery, transactional remove
      state.remove(d, tx);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      tx.rollback();

      l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }

   //////////
   ////////// Multiple deliveries
   //////////

   public void testRecoverableState_68() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] =  ms.reference(MessageFactory.createMessage("message" + i, false, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, refs[i], false);
         state.add(deliveries[i]);
      }

      Transaction tx = tr.createTransaction();

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // recoverable state, unreliable delivery, transactional remove
         state.remove(deliveries[i], tx);

      }

      tx.rollback();

      ChannelTestBase.assertEqualSets(refs, state.delivering(null));
      assertTrue(state.undelivered(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   ////////
   //////// Reliable delivery
   ////////

   //////////
   ////////// One delivery
   //////////

   public void testRecoverableState_69() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference ref = ms.reference(MessageFactory.createMessage("message0", true, "payload0"));
      Delivery d = new SimpleDelivery(channel, ref, false);
      state.add(d);

      Transaction tx = tr.createTransaction();

      // recoverable state, reliable delivery, transactional remove
      state.remove(d, tx);

      List l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      tx.rollback();

      l = state.delivering(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      assertTrue(state.undelivered(null).isEmpty());

      l = state.browse(null);
      assertEquals(1, l.size());
      assertEquals("message0", ((MessageReference)l.get(0)).getMessageID());

      log.info("ok");
   }

   //////////
   ////////// Multiple deliveries
   //////////

   public void testRecoverableState_70() throws Throwable
   {
      if (!state.isRecoverable())
      {
         // only recoverable state is tested now
         return;
      }

      MessageReference[] refs = new MessageReference[NUMBER_OF_MESSAGES];
      Delivery[] deliveries = new Delivery[NUMBER_OF_MESSAGES];
      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         refs[i] =  ms.reference(MessageFactory.createMessage("message" + i, true, "payload" + i));
         deliveries[i] = new SimpleDelivery(channel, refs[i], false);
         state.add(deliveries[i]);
      }

      Transaction tx = tr.createTransaction();

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         // recoverable state, reliable delivery, transactional remove
         state.remove(deliveries[i], tx);

      }

      tx.rollback();

      ChannelTestBase.assertEqualSets(refs, state.delivering(null));
      assertTrue(state.undelivered(null).isEmpty());
      ChannelTestBase.assertEqualSets(refs, state.browse(null));

      log.info("ok");
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected abstract void crashState() throws Exception;

   protected abstract void recoverState() throws Exception;

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
