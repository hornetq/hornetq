/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.util.AcknowledgmentImpl;
import org.jboss.messaging.core.util.ChannelNACK;
import org.jboss.messaging.core.util.StateImpl;
import org.jboss.messaging.core.util.NonCommitted;
import org.jboss.messaging.core.MutableState;
import org.jboss.messaging.core.Acknowledgment;

import java.util.Collections;
import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MutableStateTest extends MessagingTestCase
{
    protected MutableState state;

   // Constructors --------------------------------------------------

   public MutableStateTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testNewState()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testAcknowledge()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      state.acknowledge("receiver1", null);

      assertFalse(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(1, state.ackCount());
      Set acks = state.getACK();
      assertEquals(1, acks.size());
      Acknowledgment a = (Acknowledgment)acks.iterator().next();
      assertEquals("receiver1", a.getReceiverID());
      assertTrue(a.isPositive());
      assertEquals(1, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testUpdateNACKOverChannelNACK()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      StateImpl nackState =
            new StateImpl(Collections.singleton(new AcknowledgmentImpl("receiver1", false)), null);

      state.update(nackState);

      // NACK should be retained

      assertFalse(state.isChannelNACK());
      assertEquals(1, state.nackCount());
      Set nacks = state.getNACK();
      assertEquals(1, nacks.size());
      Acknowledgment a = (Acknowledgment)nacks.iterator().next();
      assertEquals("receiver1", a.getReceiverID());
      assertTrue(a.isNegative());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(1, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testUpdateNACKOverNACK()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      StateImpl nackState =
            new StateImpl(Collections.singleton(new AcknowledgmentImpl("receiver1", false)), null);

      state.update(nackState);

      state.update(nackState);

      // NACK should be retained

      assertFalse(state.isChannelNACK());
      assertEquals(1, state.nackCount());
      Set nacks = state.getNACK();
      assertEquals(1, nacks.size());
      assertEquals("receiver1", ((Acknowledgment)nacks.iterator().next()).getReceiverID());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(1, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testUpdateNACKOverACK()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      state.acknowledge("receiver1", null);

      StateImpl nackState =
            new StateImpl(Collections.singleton(new AcknowledgmentImpl("receiver1", false)), null);

      state.update(nackState);

      // the acknowledgments should cancel each other

      assertFalse(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testUpdateACKOverChannelNACK()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      StateImpl ackState =
            new StateImpl(Collections.singleton(new AcknowledgmentImpl("receiver1", true)), null);

      state.update(ackState);

      // ACK should cancel the ChannelNACK

      assertFalse(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testUpdateACKOverNACK()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      StateImpl nackState =
            new StateImpl(Collections.singleton(new AcknowledgmentImpl("receiver1", false)), null);

      state.update(nackState);

      StateImpl ackState =
            new StateImpl(Collections.singleton(new AcknowledgmentImpl("receiver1", true)), null);

      state.update(ackState);

      // the acknowledgments should cancel each other

      assertFalse(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());

   }

   public void testUpdateACKOverACK()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      state.acknowledge("receiver1", null);

      StateImpl ackState =
            new StateImpl(Collections.singleton(new AcknowledgmentImpl("receiver1", true)), null);

      state.update(ackState);

      // the extra ACK should be ignored

      assertFalse(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(1, state.ackCount());
      Set acks = state.getACK();
      assertEquals(1, acks.size());
      assertEquals("receiver1", ((Acknowledgment)acks.iterator().next()).getReceiverID());
      assertEquals(1, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testUpdateChannelNACKOverChannelNACK()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      state.update(new ChannelNACK());
      assertTrue(state.isChannelNACK());
   }

   public void testUpdateChannelNACKOverNACKs()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      StateImpl nackState =
            new StateImpl(Collections.singleton(new AcknowledgmentImpl("receiver1", false)), null);
      state.update(nackState);

      state.update(new ChannelNACK());

      assertFalse(state.isChannelNACK());
      assertEquals(1, state.nackCount());
      Set nacks = state.getNACK();
      assertEquals(1, nacks.size());
      assertEquals("receiver1", ((Acknowledgment)nacks.iterator().next()).getReceiverID());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(1, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testUpdateChannelNACKOverACKs()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      state.acknowledge("receiver1", null);

      state.update(new ChannelNACK());

      assertFalse(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(1, state.ackCount());
      Set acks = state.getACK();
      assertEquals(1, acks.size());
      assertEquals("receiver1", ((Acknowledgment)acks.iterator().next()).getReceiverID());
      assertEquals(1, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testUpdateChannelNACKOverNonCommitted()
   {
      if (state == null) { return; }

      StateImpl txState = new StateImpl(null, Collections.singleton(new NonCommitted("TX1")));
      state.update(txState);

      state.update(new ChannelNACK());

      assertTrue(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(1, state.nonCommittedCount());
      Set ncs = state.getNonCommitted();
      assertEquals(1, ncs.size());
      assertEquals("TX1", ((NonCommitted)ncs.iterator().next()).getTxID());
   }

   public void testAcknowledgeOverNACK()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      StateImpl nackState =
            new StateImpl(Collections.singleton(new AcknowledgmentImpl("receiver1", false)), null);
      state.update(nackState);

      state.acknowledge("receiver1", null);

      assertFalse(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   //
   // transactional handling
   //

   public void testUpdateTXOverChannelNACK()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      StateImpl txState = new StateImpl(null, Collections.singleton(new NonCommitted("TX1")));
      state.update(txState);

      assertTrue(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(1, state.nonCommittedCount());
      Set s = state.getNonCommitted();
      assertEquals(1, s.size());
      assertEquals("TX1", ((NonCommitted)s.iterator().next()).getTxID());
   }

   public void testUpdateTXOverNACK()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      StateImpl nackState =
            new StateImpl(Collections.singleton(new AcknowledgmentImpl("receiver1", false)), null);
      state.update(nackState);

      StateImpl txState = new StateImpl(null, Collections.singleton(new NonCommitted("TX1")));

      try
      {
         // cannot transact a message that has already been delivered
         state.update(txState);
         fail("Should have thrown IllegalStateException");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }

   public void testUpdateTXOverACK()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      state.acknowledge("receiver1", null);

      StateImpl txState = new StateImpl(null, Collections.singleton(new NonCommitted("TX1")));

      try
      {
         // cannot transact a message that has already been delivered
         state.update(txState);
         fail("Should have thrown IllegalStateException");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }

   public void testCommit()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      StateImpl txState = new StateImpl(null, Collections.singleton(new NonCommitted("TX1")));
      state.update(txState);

      // commit inexistent transaction
      state.commit("TX2");

      assertTrue(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(1, state.nonCommittedCount());
      Set s = state.getNonCommitted();
      assertEquals(1, s.size());
      assertEquals("TX1", ((NonCommitted)s.iterator().next()).getTxID());

      state.commit("TX1");

      assertTrue(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testRollback()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      StateImpl txState = new StateImpl(null, Collections.singleton(new NonCommitted("TX1")));
      state.update(txState);

      // rollback inexistent transaction
      state.rollback("TX2");

      assertTrue(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(1, state.nonCommittedCount());
      Set s = state.getNonCommitted();
      assertEquals(1, s.size());
      assertEquals("TX1", ((NonCommitted)s.iterator().next()).getTxID());

      state.rollback("TX1");

      assertFalse(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testRollbackOnEmptyState()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      state.rollback("noSuchTX");

      assertTrue(state.isChannelNACK());
   }


   //
   // transactional acknowledgment
   //

   public void testTxAcknowledge()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      state.acknowledge("receiver1", "TX1");

      assertTrue(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());

      state.acknowledge("receiver1", "TX2");

      assertTrue(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testTxAcknowledgeCommit()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      state.acknowledge("receiver1", "TX1");
      state.acknowledge("receiver2", "TX1");
      state.acknowledge("receiver3", "TX2");

      assertTrue(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());

      state.commit("TX1");

      assertFalse(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(2, state.ackCount());
      Set s = state.getACK();
      assertEquals(2, s.size());
      Set recIds = new HashSet();
      for(Iterator i = s.iterator(); i.hasNext();)
      {
         Acknowledgment a = (Acknowledgment)i.next();
         assertTrue(a.isPositive());
         recIds.add(a.getReceiverID());
      }
      assertTrue(recIds.contains("receiver1"));
      assertTrue(recIds.contains("receiver2"));
      assertEquals(2, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());

      state.commit("TX2");

      assertFalse(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(3, state.ackCount());
      s = state.getACK();
      assertEquals(3, s.size());
      recIds = new HashSet();
      for(Iterator i = s.iterator(); i.hasNext();)
      {
         Acknowledgment a = (Acknowledgment)i.next();
         assertTrue(a.isPositive());
         recIds.add(a.getReceiverID());
      }
      assertTrue(recIds.contains("receiver1"));
      assertTrue(recIds.contains("receiver2"));
      assertTrue(recIds.contains("receiver3"));
      assertEquals(3, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }

   public void testNACKandTxAcknowledgeCommit()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      // NACK the message

      StateImpl nackState =
            new StateImpl(Collections.singleton(new AcknowledgmentImpl("receiver1", false)), null);

      state.update(nackState);

      // NACK should be retained

      assertFalse(state.isChannelNACK());
      assertEquals(1, state.nackCount());
      Set nacks = state.getNACK();
      assertEquals(1, nacks.size());
      Acknowledgment a = (Acknowledgment)nacks.iterator().next();
      assertEquals("receiver1", a.getReceiverID());
      assertTrue(a.isNegative());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(1, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());

      // transactionally ACK

      state.acknowledge("receiver1", "TX1");

      assertFalse(state.isChannelNACK());
      assertEquals(1, state.nackCount());
      nacks = state.getNACK();
      assertEquals(1, nacks.size());
      a = (Acknowledgment)nacks.iterator().next();
      assertEquals("receiver1", a.getReceiverID());
      assertTrue(a.isNegative());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(1, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());

      // commit

      state.commit("TX1");

      assertFalse(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }


   public void testTxAcknowledgeRollback()
   {
      if (state == null) { return; }

      assertTrue(state.isChannelNACK());

      state.acknowledge("receiver1", "TX1");

      assertTrue(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());

      state.rollback("TX1");

      assertTrue(state.isChannelNACK());
      assertEquals(0, state.nackCount());
      assertTrue(state.getNACK().isEmpty());
      assertEquals(0, state.ackCount());
      assertTrue(state.getACK().isEmpty());
      assertEquals(0, state.size());
      assertEquals(0, state.nonCommittedCount());
      assertTrue(state.getNonCommitted().isEmpty());
   }
}
