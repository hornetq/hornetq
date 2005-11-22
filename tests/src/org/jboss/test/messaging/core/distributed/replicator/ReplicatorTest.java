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
package org.jboss.test.messaging.core.distributed.replicator;

import org.jboss.messaging.core.distributed.Distributed;
import org.jboss.messaging.core.distributed.replicator.Replicator;
import org.jboss.messaging.core.distributed.replicator.ReplicatorOutput;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.message.InMemoryMessageStore;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.MessageStore;
import org.jboss.test.messaging.core.distributed.base.PeerTestBase;
import org.jboss.test.messaging.core.distributed.SimpleViewKeeper;
import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jgroups.blocks.RpcDispatcher;

import java.util.Set;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ReplicatorTest extends PeerTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected SimpleViewKeeper viewKeeper;
   protected MessageStore ms;

   protected Replicator replicator, replicator2, replicator3;

   // Constructors --------------------------------------------------

   public ReplicatorTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      viewKeeper = new SimpleViewKeeper("replicator0");
      ms = new InMemoryMessageStore("in-memory-ms0");

      super.setUp();

      replicator = (Replicator)distributed;
      replicator2 = (Replicator)distributed2;
      replicator3 = (Replicator)distributed3;

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      replicator = null;
      replicator2 = null;
      replicator3 = null;
      viewKeeper.clear();
      viewKeeper = null;
      ms = null;
      super.tearDown();
   }

   public void testHandleReplicatorDidNotJoin() throws Exception
   {
      assertFalse(peer.hasJoined());
      assertNull(replicator.handle(null, MessageFactory.createMessage("message0"), null));
   }

   //
   // One replicator
   //

   ////
   //// No output
   ////

   //////
   ////// One message
   //////

   public void testReplicator_1() throws Exception
   {
      assertTrue(jchannel.isConnected());

      replicator.join();

      log.debug("replicator has joined");

      assertTrue(replicator.hasJoined());
      assertTrue(replicator.getOutputs().isEmpty());

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      Message m = MessageFactory.createMessage("message0", true, "payload");
      Delivery delivery = replicator.handle(observer, m, null);

      assertNull(delivery);
   }

   ////
   //// One output per dispatcher
   ////

   //////
   ////// One message
   //////

   public void testReplicator_2() throws Exception
   {
      assertTrue(jchannel.isConnected());

      replicator.join();
      log.debug("replicator has joined");

      assertTrue(replicator.hasJoined());
      assertTrue(replicator.getOutputs().isEmpty());

      ReplicatorOutput output =
            new ReplicatorOutput(replicator.getReplicatorID(), dispatcher, null);

      output.join();
      log.debug("output has joined");

      assertTrue(output.hasJoined());

      Set identities = replicator.getOutputs();
      assertEquals(1, identities.size());
      assertTrue(identities.contains(output.getPeerIdentity()));

      identities = replicator.getView();
      assertEquals(2, identities.size());
      assertTrue(identities.contains(output.getPeerIdentity()));
      assertTrue(identities.contains(replicator.getPeerIdentity()));

      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
      Message m = MessageFactory.createMessage("message0", true, "payload");
      Delivery delivery = replicator.handle(observer, m, null);

      assertTrue(delivery.isCancelled());
   }


   //////
   ////// No receiver
   //////

   ////////
   //////// One message
   ////////

//   public void testReplicator_2() throws Exception
//   {
//      assertTrue(jchannel.isConnected());
//
//      replicator.join();
//
//      log.debug("replicator has joined");
//
//      assertTrue(replicator.hasJoined());
//
//      ReplicatorOutput output = new ReplicatorOutput(replicator.getGroupID());
//
//      assertFalse(output.iterator().hasNext());
//
//      output.join();
//      assertTrue(output.hasJoined());
//
//      assertEquals(output.getReplicatorID(), replicator.getGroupID());
//
//      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
//      Message m = MessageFactory.createMessage("message0", true, "payload");
//      Delivery delivery = replicator.handle(observer, m, null);
//
//      observer.waitForCancellation(1, 3000);
//
//      assertTrue(delivery.isCancelled());
//   }

   ////////
   //////// Multiple messages
   ////////

   //////
   ////// ACKING receiver
   //////

   //////
   ////// NACKING receiver
   //////

   ////
   //// Two outputs per dispatcher
   ////

   //
   // Two replicators
   //

   //
   // Three replicators
   //

//   public void testOneInputPeerOneJChannel() throws Exception
//   {
//      inputJChannel.connect("testGroup");
//
//      Replicator replicatorPeer = new Replicator(inputDispatcher, "ReplicatorID");
//      replicatorPeer.start();
//      assertTrue(replicatorPeer.isStarted());
//
//
//      ReplicatorOutput output = new ReplicatorOutput(inputDispatcher, "ReplicatorID");
//      output.start();
//      assertTrue(output.isStarted());
//
//      ReceiverImpl r = new ReceiverImpl();
//      output.setReceiver(r);
//
//      assertTrue(replicatorPeer.handle(new MessageSupport("someid")));
//
//      r.waitForHandleInvocations(1);
//
//      Iterator i = r.iterator();
//      assertEquals(new MessageSupport("someid"), i.next());
//      assertFalse(i.hasNext());
//
//      // the messages should be acknowledged in a resonable time, otherwise the test will timeout
//      while(true)
//      {
//         if (!replicatorPeer.hasMessages())
//         {
//            return;
//         }
//         log.info("Thread sleeping for 500 ms");
//         Thread.sleep(500);
//      }
//   }
//

//   public void testOneInputPeerOneJChannelTwoMessages() throws Exception
//   {
//      inputJChannel.connect("testGroup");
//
//      Replicator peer = new Replicator(inputDispatcher, "ReplicatorID");
//      peer.start();
//      assertTrue(peer.isStarted());
//
//      ReplicatorOutput output = new ReplicatorOutput(inputDispatcher, "ReplicatorID");
//      output.start();
//      assertTrue(output.isStarted());
//
//      ReceiverImpl r = new ReceiverImpl();
//      r.resetInvocationCount();
//      output.setReceiver(r);
//      MessageSupport m1 = new MessageSupport("messageID1"), m2 = new MessageSupport("messageID2");
//
//      assertTrue(peer.handle(m1));
//      assertTrue(peer.handle(m2));
//
//      r.waitForHandleInvocations(2);
//
//      List messages = r.getMessages();
//      assertEquals(2, messages.size());
//      assertTrue(messages.contains(m1));
//      assertTrue(messages.contains(m2));
//
//      // the messages should be acknowledged in a resonable time, otherwise the test will timeout
//      while(true)
//      {
//         if (!peer.hasMessages())
//         {
//            return;
//         }
//         log.info("Thread sleeping for 500 ms");
//         Thread.sleep(500);
//      }
//   }
//
//   public void testTwoInputPeersOneJChannel() throws Exception
//   {
//      inputJChannel.connect("testGroup");
//
//      Replicator inputPeerOne = new Replicator(inputDispatcher, "ReplicatorID");
//      Replicator inputPeerTwo = new Replicator(inputDispatcher, "ReplicatorID");
//      inputPeerOne.start();
//      inputPeerTwo.start();
//      assertTrue(inputPeerOne.isStarted());
//      assertTrue(inputPeerTwo.isStarted());
//
//      ReplicatorOutput output = new ReplicatorOutput(inputDispatcher, "ReplicatorID");
//      output.start();
//      assertTrue(output.isStarted());
//
//      ReceiverImpl r = new ReceiverImpl();
//      r.resetInvocationCount();
//      output.setReceiver(r);
//      MessageSupport
//            m1 = new MessageSupport("messageID1"),
//            m2 = new MessageSupport("messageID2"),
//            m3 = new MessageSupport("messageID3"),
//            m4 = new MessageSupport("messageID4");
//
//      assertTrue(inputPeerOne.handle(m1));
//      assertTrue(inputPeerTwo.handle(m2));
//      assertTrue(inputPeerOne.handle(m3));
//      assertTrue(inputPeerTwo.handle(m4));
//
//      r.waitForHandleInvocations(4);
//
//      List messages = r.getMessages();
//      assertEquals(4, messages.size());
//      assertTrue(messages.contains(m1));
//      assertTrue(messages.contains(m2));
//      assertTrue(messages.contains(m3));
//      assertTrue(messages.contains(m4));
//
//      // the messages should be acknowledged in a resonable time, otherwise the test will timeout
//      while(true)
//      {
//         if (!inputPeerOne.hasMessages())
//         {
//            break;
//         }
//         log.info("Thread sleeping for 500 ms");
//         Thread.sleep(500);
//      }
//      while(true)
//      {
//         if (!inputPeerTwo.hasMessages())
//         {
//            break;
//         }
//         log.info("Thread sleeping for 500 ms");
//         Thread.sleep(500);
//      }
//   }
//
//
//
//   public void testTwoJChannels() throws Exception
//   {
//      inputJChannel.connect("testGroup");
//      outputJChannel.connect("testGroup");
//
//      Replicator inputPeer = new Replicator(inputDispatcher, "ReplicatorID");
//      inputPeer.start();
//      assertTrue(inputPeer.isStarted());
//
//      ReplicatorOutput output = new ReplicatorOutput(outputDispatcher, "ReplicatorID");
//      output.start();
//      assertTrue(output.isStarted());
//
//      ReceiverImpl r = new ReceiverImpl();
//      r.resetInvocationCount();
//      output.setReceiver(r);
//      MessageSupport
//            m1 = new MessageSupport("messageID1"),
//            m2 = new MessageSupport("messageID2"),
//            m3 = new MessageSupport("messageID3"),
//            m4 = new MessageSupport("messageID4");
//
//      assertTrue(inputPeer.handle(m1));
//      assertTrue(inputPeer.handle(m2));
//      assertTrue(inputPeer.handle(m3));
//      assertTrue(inputPeer.handle(m4));
//
//      r.waitForHandleInvocations(4);
//
//      List messages = r.getMessages();
//      assertEquals(4, messages.size());
//      assertTrue(messages.contains(m1));
//      assertTrue(messages.contains(m2));
//      assertTrue(messages.contains(m3));
//      assertTrue(messages.contains(m4));
//
//      // the messages should be acknowledged in a resonable time, otherwise the test will timeout
//      while(true)
//      {
//         if (!inputPeer.hasMessages())
//         {
//            break;
//         }
//         log.info("Thread sleeping for 500 ms");
//         Thread.sleep(500);
//      }
//   }
//
//   public void testTwoJChannelsOneInputTwoOutputs() throws Exception
//   {
//      inputJChannel.connect("testGroup");
//      outputJChannel.connect("testGroup");
//
//      Replicator inputPeer = new Replicator(inputDispatcher, "ReplicatorID");
//      inputPeer.start();
//      assertTrue(inputPeer.isStarted());
//
//      ReplicatorOutput outputOne = new ReplicatorOutput(inputDispatcher, "ReplicatorID");
//      outputOne.start();
//      assertTrue(outputOne.isStarted());
//
//      ReceiverImpl rOne = new ReceiverImpl();
//      rOne.resetInvocationCount();
//      outputOne.setReceiver(rOne);
//
//      ReplicatorOutput outputTwo = new ReplicatorOutput(outputDispatcher, "ReplicatorID");
//      outputTwo.start();
//      assertTrue(outputTwo.isStarted());
//
//      ReceiverImpl rTwo = new ReceiverImpl();
//      rTwo.resetInvocationCount();
//      outputTwo.setReceiver(rTwo);
//
//      MessageSupport
//            m1 = new MessageSupport("messageID1"),
//            m2 = new MessageSupport("messageID2"),
//            m3 = new MessageSupport("messageID3"),
//            m4 = new MessageSupport("messageID4");
//
//      assertTrue(inputPeer.handle(m1));
//      assertTrue(inputPeer.handle(m2));
//      assertTrue(inputPeer.handle(m3));
//      assertTrue(inputPeer.handle(m4));
//
//      rOne.waitForHandleInvocations(4);
//      List messages = rOne.getMessages();
//      assertEquals(4, messages.size());
//      assertTrue(messages.contains(m1));
//      assertTrue(messages.contains(m2));
//      assertTrue(messages.contains(m3));
//      assertTrue(messages.contains(m4));
//
//      rTwo.waitForHandleInvocations(4);
//      messages = rTwo.getMessages();
//      assertEquals(4, messages.size());
//      assertTrue(messages.contains(m1));
//      assertTrue(messages.contains(m2));
//      assertTrue(messages.contains(m3));
//      assertTrue(messages.contains(m4));
//
//      // the messages should be acknowledged in a resonable time, otherwise the test will timeout
//      while(true)
//      {
//         if (!inputPeer.hasMessages())
//         {
//            break;
//         }
//         log.info("Thread sleeping for 500 ms");
//         Thread.sleep(500);
//      }
//   }
//
//
//   // Note: if you want the testing process to exit with 1 on a failed test, use
//   //       java junit.textui.TestRunner fully.qualified.test.class.name
//   public static void main(String[] args) throws Exception
//   {
//      TestRunner.run(ReplicatorTest.class);
//   }
//




   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected Distributed createDistributedDestination(String name, RpcDispatcher d)
   {
      return new Replicator(viewKeeper, d, ms);
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
