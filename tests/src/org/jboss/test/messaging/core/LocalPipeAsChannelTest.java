/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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
