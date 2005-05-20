/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.util.AcknowledgmentImpl;
import org.jboss.messaging.core.util.AcknowledgmentSet;

import java.util.Collections;
import java.util.Set;
import java.util.HashSet;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class AcknowledgmentSetTest extends MessagingTestCase
{

    protected AcknowledgmentSet acknowledgmentSet;

   // Constructors --------------------------------------------------

   public AcknowledgmentSetTest(String name)
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

   public void testNewAcknowledgmentSet() throws Exception
   {
      if (acknowledgmentSet == null) { return; }

      assertFalse(acknowledgmentSet.isDeliveryAttempted());
   }

   public void testUpdateEmptySet() throws Exception
   {
      if (acknowledgmentSet == null) { return; }

      acknowledgmentSet.update(Collections.EMPTY_SET);
      assertFalse(acknowledgmentSet.isDeliveryAttempted());
   }

   public void testUpdatePositiveAck() throws Exception
   {
      if (acknowledgmentSet == null) { return; }

      Set s = Collections.singleton(new AcknowledgmentImpl("receiverID", true));
      acknowledgmentSet.update(s);

      // the positive ack should be ignored

      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(0, acknowledgmentSet.nackCount());
      assertTrue(acknowledgmentSet.getNACK().isEmpty());
      assertEquals(0, acknowledgmentSet.ackCount());
      assertTrue(acknowledgmentSet.getACK().isEmpty());
      assertEquals(0, acknowledgmentSet.size());
   }

   public void testChannelNACK() throws Exception
   {
      if (acknowledgmentSet == null) { return; }

      acknowledgmentSet.update(null);

      assertFalse(acknowledgmentSet.isDeliveryAttempted());

      // store a NACK

      Set s = Collections.singleton(new AcknowledgmentImpl("receiverID", false));
      acknowledgmentSet.update(s);
      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(1, acknowledgmentSet.nackCount());
      assertEquals(0, acknowledgmentSet.ackCount());
      assertEquals(1, acknowledgmentSet.size());

      // resent the Channel NACK

      acknowledgmentSet.update(null);

      assertTrue(acknowledgmentSet.isDeliveryAttempted());
      assertEquals(1, acknowledgmentSet.nackCount());
      assertEquals(0, acknowledgmentSet.ackCount());
      assertEquals(1, acknowledgmentSet.size());
      
   }
}
