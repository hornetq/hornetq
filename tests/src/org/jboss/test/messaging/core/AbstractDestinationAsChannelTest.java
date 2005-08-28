/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;



/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class AbstractDestinationAsChannelTest extends TransactionalChannelSupportTest
{
//   // Attributes ----------------------------------------------------
//
//   protected AbstractDestination abstractDestination;
//
//   // Constructors --------------------------------------------------
//
   public AbstractDestinationAsChannelTest(String name)
   {
      super(name);
   }

//   public void setUp() throws Exception
//   {
//      abstractDestination = (AbstractDestination)channel;
//
//      super.setUp();
//   }
//
//   public void tearDown() throws Exception
//   {
//      abstractDestination = null;
//      super.tearDown();
//   }
//
//   public void testDeliveryAttemptTriggeredByAddingReceiver()
//   {
//      if (abstractDestination == null) { return; }
//
//      assertTrue(abstractDestination.setSynchronous(false));
//
//      assertTrue(abstractDestination.handle(new RoutableSupport("routableID1", false)));
//      assertTrue(abstractDestination.handle(new RoutableSupport("routableID2", false)));
//      assertTrue(abstractDestination.handle(new RoutableSupport("routableID3", false)));
//
//      assertEquals(3, abstractDestination.getUndelivered().size());
//
//      assertFalse(abstractDestination.deliver());
//      assertEquals(3, abstractDestination.getUndelivered().size());
//
//      // this should trigger asynchronous delivery attempt
//      abstractDestination.add(receiverOne);
//
//      assertFalse(abstractDestination.hasMessages());
//
//      assertEquals(3, receiverOne.getMessages().size());
//      assertTrue(receiverOne.contains("routableID1"));
//      assertTrue(receiverOne.contains("routableID2"));
//      assertTrue(receiverOne.contains("routableID3"));
//   }

}