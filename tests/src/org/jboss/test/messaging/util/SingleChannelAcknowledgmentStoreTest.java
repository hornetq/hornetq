/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.messaging.core.util.SingleChannelAcknowledgmentStore;
import org.jboss.messaging.core.util.AcknowledgmentImpl;

import java.util.Set;
import java.util.HashSet;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class SingleChannelAcknowledgmentStoreTest extends SingleReceiverAcknowledgmentStoreTest
{

   // Constructors --------------------------------------------------

   public SingleChannelAcknowledgmentStoreTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      acknowledgmentStore = new SingleChannelAcknowledgmentStore("SingleChannelStoreID");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
      acknowledgmentStore = null;
      
   }

   //
   // It also runs all tests from AcknowledgmentStoreTest
   //


   public void testGetNACKForMultipleReceivers() throws Throwable
   {
      Set acks = new HashSet();
      acks.add(new AcknowledgmentImpl("r1", false));
      acks.add(new AcknowledgmentImpl("r2", false));
      acknowledgmentStore.update("c1", "m1", acks);

      Set s = acknowledgmentStore.getNACK("c1", "m1");
      assertEquals(2, s.size());
      assertTrue(s.contains("r1"));
      assertTrue(s.contains("r2"));
   }


   public void testGetACKForMultipleReceivers() throws Throwable
   {
      acknowledgmentStore.acknowledge("c1", "m1", "r1");
      acknowledgmentStore.acknowledge("c1", "m1", "r2");

      Set s = acknowledgmentStore.getACK("c1", "m1");
      assertEquals(2, s.size());
      assertTrue(s.contains("r1"));
      assertTrue(s.contains("r2"));
   }

}

