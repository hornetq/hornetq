/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.messaging.core.util.AcknowledgmentImpl;
import org.jboss.messaging.core.util.MultipleReceiverAcknowledgmentSet;
import org.jboss.messaging.core.Acknowledgment;

import java.util.Set;
import java.util.HashSet;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MultipleReceiverAcknowledgmentSetTest extends AcknowledgmentSetTest
{

   // Constructors --------------------------------------------------

   public MultipleReceiverAcknowledgmentSetTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      acknowledgmentSet = new MultipleReceiverAcknowledgmentSet();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
      acknowledgmentSet = null;
      
   }

   public void testUpdateNegativeAck() throws Exception
   {
      if (acknowledgmentSet == null) { return; }

      Set s = new HashSet();
      s.add(new AcknowledgmentImpl("receiverID", false));
      acknowledgmentSet.update(s);

      // the negative ack should be retained

      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(1, acknowledgmentSet.nackCount());
      Set nacks = acknowledgmentSet.getNACK();
      assertEquals(1, nacks.size());
      Acknowledgment a = (Acknowledgment)nacks.iterator().next();
      assertEquals("receiverID", a.getReceiverID());
      assertTrue(a.isNegative());
      assertEquals(0, acknowledgmentSet.ackCount());
      assertTrue(acknowledgmentSet.getACK().isEmpty());
      assertEquals(1, acknowledgmentSet.size());
   }


   public void testACK() throws Exception
   {
      if (acknowledgmentSet == null) { return; }

      // add an ACK

      acknowledgmentSet.acknowledge("receiver1");

      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(1, acknowledgmentSet.ackCount());
      Set acks = acknowledgmentSet.getACK();
      assertEquals(1, acks.size());
      Acknowledgment a = (Acknowledgment)acks.iterator().next();
      assertEquals("receiver1", a.getReceiverID());
      assertTrue(a.isPositive());
      assertEquals(0, acknowledgmentSet.nackCount());
      assertTrue(acknowledgmentSet.getNACK().isEmpty());
      assertEquals(1, acknowledgmentSet.size());

      // cancel it

      Set s = new HashSet();
      s.add(new AcknowledgmentImpl("receiver1", false));
      acknowledgmentSet.update(s);

      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(0, acknowledgmentSet.ackCount());
      assertTrue(acknowledgmentSet.getACK().isEmpty());
      assertEquals(0, acknowledgmentSet.nackCount());
      assertTrue(acknowledgmentSet.getNACK().isEmpty());
      assertEquals(0, acknowledgmentSet.size());

      // add a NACK


      s = new HashSet();
      s.add(new AcknowledgmentImpl("receiver2", false));
      acknowledgmentSet.update(s);

      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(0, acknowledgmentSet.ackCount());
      assertTrue(acknowledgmentSet.getACK().isEmpty());
      assertEquals(1, acknowledgmentSet.nackCount());
      Set nacks = acknowledgmentSet.getNACK();
      assertEquals(1, nacks.size());
      a = (Acknowledgment)nacks.iterator().next();
      assertEquals("receiver2", a.getReceiverID());
      assertTrue(a.isNegative());
      assertEquals(1, acknowledgmentSet.size());

      // cancel it

      acknowledgmentSet.acknowledge("receiver2");

      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(0, acknowledgmentSet.ackCount());
      assertTrue(acknowledgmentSet.getACK().isEmpty());
      assertEquals(0, acknowledgmentSet.nackCount());
      assertTrue(acknowledgmentSet.getNACK().isEmpty());
      assertEquals(0, acknowledgmentSet.size());
   }
}
