/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.interfaces.Channel;
import org.jboss.messaging.interfaces.Receiver;
import org.jboss.messaging.core.RoutableSupport;
import org.jboss.messaging.core.MessageSupport;
import org.jboss.messaging.core.MessageReferenceSupport;
import org.jboss.messaging.core.LocalPipe;
import org.jboss.messaging.core.AbstractDestination;

import java.util.List;
import java.util.Set;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class AbstractDestinationTest extends ChannelSupportTest
{
   // Attributes ----------------------------------------------------

   protected AbstractDestination abstractDestination;

   // Constructors --------------------------------------------------

   public AbstractDestinationTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      super.setUp();
      abstractDestination = (AbstractDestination)channel;
   }

   public void tearDown() throws Exception
   {
      abstractDestination = null;
      super.tearDown();
   }

   public void testDeliveryAttemptTriggeredByAddingReceiver()
   {
      if (abstractDestination == null) { return; }

      assertTrue(abstractDestination.setSynchronous(false));

      assertTrue(abstractDestination.handle(new RoutableSupport("routableID1", false)));
      assertTrue(abstractDestination.handle(new RoutableSupport("routableID2", false)));
      assertTrue(abstractDestination.handle(new RoutableSupport("routableID3", false)));

      assertEquals(3, abstractDestination.getUnacknowledged().size());

      assertFalse(abstractDestination.deliver());
      assertEquals(3, abstractDestination.getUnacknowledged().size());

      // this should trigger asynchronous delivery attempt
      abstractDestination.add(receiverOne);

      assertFalse(abstractDestination.hasMessages());

      assertEquals(3, receiverOne.getMessages().size());
      assertTrue(receiverOne.contains("routableID1"));
      assertTrue(receiverOne.contains("routableID2"));
      assertTrue(receiverOne.contains("routableID3"));
   }
}