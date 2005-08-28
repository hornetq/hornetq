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
public class LocalPipeAsChannelTest extends SingleOutputChannelSupportTest
{
   // Constructors --------------------------------------------------

   public LocalPipeAsChannelTest(String name)
   {
      super(name);
   }

//   public void setUp() throws Exception
//   {
//      // Create a receiver and a LocalPipe to be testes by the superclass tests
//
//      receiverOne = new ReceiverImpl("ReceiverOne", ReceiverImpl.HANDLING);
//      channel = new LocalPipe("LocalPipeID", receiverOne);
//
//      // important, I need the channel set at this point
//      super.setUp();
//   }
//
//   public void tearDown()throws Exception
//   {
//      ((LocalPipe)channel).setReceiver(null);
//      channel = null;
//      receiverOne = null;
//      super.tearDown();
//   }
//
//   //
//   // This test also runs all ChannelSupportTest's tests
//   //
//
//   public void testDefaultSynchronous()
//   {
//      assertTrue(channel.isSynchronous());
//   }
//
//   public void testDeliveryAttemptTriggeredByAddingReceiver()
//   {
//      LocalPipe pipe = (LocalPipe)channel;
//      pipe.setReceiver(null);
//      assertTrue(pipe.setSynchronous(false));
//
//      assertTrue(pipe.handle(new RoutableSupport("routableID1", false)));
//      assertTrue(pipe.handle(new RoutableSupport("routableID2", false)));
//      assertTrue(pipe.handle(new RoutableSupport("routableID3", false)));
//
//      assertEquals(3, pipe.getUndelivered().size());
//
//      assertFalse(pipe.deliver());
//      assertEquals(3, pipe.getUndelivered().size());
//
//      // this should trigger asynchronous delivery attempt
//      pipe.setReceiver(receiverOne);
//
//      assertFalse(pipe.hasMessages());
//
//      assertEquals(3, receiverOne.getMessages().size());
//      assertTrue(receiverOne.contains("routableID1"));
//      assertTrue(receiverOne.contains("routableID2"));
//      assertTrue(receiverOne.contains("routableID3"));
//   }

}
