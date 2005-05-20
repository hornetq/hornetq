/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.messaging.core.util.SingleReceiverAcknowledgmentSet;
import org.jboss.messaging.core.Acknowledgment;



/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class SingleReceiverAcknowledgmentSetTest extends AcknowledgmentSetTest
{

   // Constructors --------------------------------------------------

   public SingleReceiverAcknowledgmentSetTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      acknowledgmentSet = new SingleReceiverAcknowledgmentSet();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
      acknowledgmentSet = null;
   }

   public void testUpdateNegativeAck() throws Exception
   {
      acknowledgmentSet.update(Acknowledgment.NACKSet);

      // the negative ack should be retained

      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(1, acknowledgmentSet.nackCount());
      assertTrue(acknowledgmentSet.getNACK().isEmpty());
      assertEquals(0, acknowledgmentSet.ackCount());
      assertTrue(acknowledgmentSet.getACK().isEmpty());
      assertEquals(1, acknowledgmentSet.size());
   }


   public void testACK() throws Exception
   {
      // add an ACK

      acknowledgmentSet.ACK(null);

      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(1, acknowledgmentSet.ackCount());
      assertTrue(acknowledgmentSet.getACK().isEmpty());
      assertEquals(0, acknowledgmentSet.nackCount());
      assertTrue(acknowledgmentSet.getNACK().isEmpty());
      assertEquals(1, acknowledgmentSet.size());

      // cancel it

      acknowledgmentSet.update(Acknowledgment.NACKSet);

      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(0, acknowledgmentSet.ackCount());
      assertTrue(acknowledgmentSet.getACK().isEmpty());
      assertEquals(0, acknowledgmentSet.nackCount());
      assertTrue(acknowledgmentSet.getNACK().isEmpty());
      assertEquals(0, acknowledgmentSet.size());

      // add a NACK


      acknowledgmentSet.update(Acknowledgment.NACKSet);

      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(0, acknowledgmentSet.ackCount());
      assertTrue(acknowledgmentSet.getACK().isEmpty());
      assertEquals(1, acknowledgmentSet.nackCount());
      assertTrue(acknowledgmentSet.getNACK().isEmpty());
      assertEquals(1, acknowledgmentSet.size());

      // cancel it

      acknowledgmentSet.ACK(null);

      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(0, acknowledgmentSet.ackCount());
      assertTrue(acknowledgmentSet.getACK().isEmpty());
      assertEquals(0, acknowledgmentSet.nackCount());
      assertTrue(acknowledgmentSet.getNACK().isEmpty());
      assertEquals(0, acknowledgmentSet.size());
   }
}
