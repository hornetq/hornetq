/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.AcknowledgmentStore;
import org.jboss.messaging.core.util.AcknowledgmentImpl;

import java.util.Collections;
import java.util.Set;



/**
 * Contains tests for the lowest amount of functionality that has to be available in a
 * InMemoryAcknowledgmentStore, SingleChannelAcknowledgmentStore, etc
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class AcknowledgmentStoreTest extends MessagingTestCase
{

    protected AcknowledgmentStore acknowledgmentStore;

   // Constructors --------------------------------------------------

   public AcknowledgmentStoreTest(String name)
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

   public void testChannelNACK() throws Throwable
   {
      if (acknowledgmentStore == null) { return; }

      acknowledgmentStore.update("c1", "m1", Collections.EMPTY_SET);

      Set s = acknowledgmentStore.getUnacknowledged("c1");
      assertEquals(1, s.size());
      assertTrue(s.contains("m1"));
      assertTrue(acknowledgmentStore.hasNACK("c1", "m1"));
      s = acknowledgmentStore.getNACK("c1", "m1");
      assertNull(s);
      s = acknowledgmentStore.getACK("c1", "m1");
      assertTrue(s.isEmpty());
   }

   public void testIgnoreACKonUpdate() throws Throwable
   {
      if (acknowledgmentStore == null) { return; }

      acknowledgmentStore.update("c1", "m1",
                                 Collections.singleton(new AcknowledgmentImpl("r1", true)));

      Set s = acknowledgmentStore.getUnacknowledged("c1");
      assertEquals(0, s.size());
      assertFalse(acknowledgmentStore.hasNACK("c1", "m1"));
      s = acknowledgmentStore.getNACK("c1", "m1");
      assertTrue(s.isEmpty());
      s = acknowledgmentStore.getACK("c1", "m1");
      assertTrue(s.isEmpty());

   }

   public void testUpdateNACK() throws Throwable
   {
      if (acknowledgmentStore == null) { return; }

      acknowledgmentStore.update("c1", "m1",
                                 Collections.singleton(new AcknowledgmentImpl("r1", false)));

      Set s = acknowledgmentStore.getUnacknowledged("c1");
      assertEquals(1, s.size());
      assertTrue(s.contains("m1"));
      assertTrue(acknowledgmentStore.hasNACK("c1", "m1"));
      s = acknowledgmentStore.getNACK("c1", "m1");
      assertEquals(1, s.size());
      s = acknowledgmentStore.getACK("c1", "m1");
      assertTrue(s.isEmpty());
   }

   public void testUpdateNACKandThenACK() throws Throwable
   {
      if (acknowledgmentStore == null) { return; }

      acknowledgmentStore.update("c1", "m1",
                                 Collections.singleton(new AcknowledgmentImpl("r1", false)));

      acknowledgmentStore.update("c1", "m1",
                                 Collections.singleton(new AcknowledgmentImpl("r1", true)));

      Set s = acknowledgmentStore.getUnacknowledged("c1");
      assertEquals(0, s.size());
      assertFalse(acknowledgmentStore.hasNACK("c1", "m1"));
      s = acknowledgmentStore.getNACK("c1", "m1");
      assertTrue(s.isEmpty());
      s = acknowledgmentStore.getACK("c1", "m1");
      assertTrue(s.isEmpty());

   }

   public void testAcknowledge() throws Throwable
   {
      if (acknowledgmentStore == null) { return; }

      acknowledgmentStore.acknowledge("c1", "m1", "r1");

      Set s = acknowledgmentStore.getUnacknowledged("c1");
      assertEquals(0, s.size());
      assertFalse(acknowledgmentStore.hasNACK("c1", "m1"));
      s = acknowledgmentStore.getNACK("c1", "m1");
      assertTrue(s.isEmpty());
      s = acknowledgmentStore.getACK("c1", "m1");
      assertEquals(1, s.size());

      // acknowledge() twice
      acknowledgmentStore.acknowledge("c1", "m1", "r1");

      s = acknowledgmentStore.getUnacknowledged("c1");
      assertEquals(0, s.size());
      assertFalse(acknowledgmentStore.hasNACK("c1", "m1"));
      s = acknowledgmentStore.getNACK("c1", "m1");
      assertTrue(s.isEmpty());
      s = acknowledgmentStore.getACK("c1", "m1");
      assertEquals(1, s.size());
   }

   public void testAcknowledgeAndUpdateACK() throws Throwable
   {
      if (acknowledgmentStore == null) { return; }

      acknowledgmentStore.acknowledge("c1", "m1", "r1");

   }

   public void testAcknowledgeAndUpdateNACK() throws Throwable
   {
      if (acknowledgmentStore == null) { return; }

      acknowledgmentStore.acknowledge("c1", "m1", "r1");

      Set s = acknowledgmentStore.getUnacknowledged("c1");
      assertEquals(0, s.size());
      assertFalse(acknowledgmentStore.hasNACK("c1", "m1"));
      s = acknowledgmentStore.getNACK("c1", "m1");
      assertTrue(s.isEmpty());
      s = acknowledgmentStore.getACK("c1", "m1");
      assertEquals(1, s.size());

      acknowledgmentStore.update("c1", "m1",
                                 Collections.singleton(new AcknowledgmentImpl("r1", false)));

      s = acknowledgmentStore.getUnacknowledged("c1");
      assertEquals(0, s.size());
      assertFalse(acknowledgmentStore.hasNACK("c1", "m1"));
      s = acknowledgmentStore.getNACK("c1", "m1");
      assertTrue(s.isEmpty());
      s = acknowledgmentStore.getACK("c1", "m1");
      assertTrue(s.isEmpty());
   }

}
