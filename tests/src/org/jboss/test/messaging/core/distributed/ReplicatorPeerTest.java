/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.distributed;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.ReceiverImpl;
import org.jboss.messaging.util.RpcServer;
import org.jboss.messaging.core.distributed.ReplicatorOutput;
import org.jboss.messaging.core.distributed.ReplicatorPeer;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.CoreMessage;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.JChannel;

import junit.textui.TestRunner;

import java.util.Iterator;
import java.util.List;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReplicatorPeerTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   private String props =
         "UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=32):"+
         "PING(timeout=3050;num_initial_members=6):"+
         "FD(timeout=3000):"+
         "VERIFY_SUSPECT(timeout=1500):"+
         "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800):"+
         "UNICAST(timeout=600,1200,2400,4800):"+
         "pbcast.STABLE(desired_avg_gossip=10000):"+
         "FRAG:"+
         "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=true;print_local_addr=true)";

   // Attributes ----------------------------------------------------

   private JChannel jChannelOne, jChannelTwo;
   private RpcDispatcher dispatcherOne, dispatcherTwo;

   // Constructors --------------------------------------------------

   public ReplicatorPeerTest(String name)
   {
      super(name);
   }

   // Protected -----------------------------------------------------

   // Public --------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      jChannelOne = new JChannel(props);
      dispatcherOne = new RpcDispatcher(jChannelOne, null, null, new RpcServer());

      jChannelTwo = new JChannel(props);
      dispatcherTwo = new RpcDispatcher(jChannelTwo, null, null, new RpcServer());

      // Don't connect the channels yet.
   }

   protected void tearDown() throws Exception
   {
      jChannelOne.close();
      jChannelTwo.close();
      super.tearDown();
   }


   public void testNullRpcServer() throws Exception
   {
      JChannel channel = new JChannel();
      RpcDispatcher dispatcher = new RpcDispatcher(channel, null, null, null);

      try
      {
         new ReplicatorPeer(dispatcher, "doesntmatter");
         fail("Should have thrown IllegalStateException");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }

   public void testNoRpcServer() throws Exception
   {
      JChannel channel = new JChannel();
      RpcDispatcher dispatcher = new RpcDispatcher(channel, null, null, new Object());

      try
      {
         new ReplicatorPeer(dispatcher, "doesntmatter");
         fail("Should have thrown IllegalStateException");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }


   public void testConnectWithJChannelNotConnected() throws Exception
   {
      JChannel jChannel = new JChannel();
      RpcDispatcher dispatcher = new RpcDispatcher(jChannel, null, null, new RpcServer());
      assertFalse(jChannel.isConnected());
      ReplicatorPeer peerOne = new ReplicatorPeer(dispatcher, "doesntmatter");

      try
      {
         peerOne.start();
         fail("Should have thrown DistributedException");
      }
      catch(DistributedException e)
      {
         // Ok
      }
   }

   public void testHandleInputPeerNotConnected() throws Exception
   {
      ReplicatorPeer input = new ReplicatorPeer(dispatcherOne, "ReplicatorID");
      assertFalse(input.handle(new CoreMessage("messageID")));

   }

   public void testOneInputPeerOneJChannel() throws Exception
   {
      jChannelOne.connect("testGroup");

      ReplicatorPeer replicatorPeer = new ReplicatorPeer(dispatcherOne, "ReplicatorID");
      replicatorPeer.start();
      assertTrue(replicatorPeer.isStarted());


      ReplicatorOutput output = new ReplicatorOutput(dispatcherOne, "ReplicatorID");
      output.start();
      assertTrue(output.isStarted());

      ReceiverImpl r = new ReceiverImpl();
      output.setReceiver(r);

      assertTrue(replicatorPeer.handle(new CoreMessage("someid")));

      r.waitForHandleInvocations(1);

      Iterator i = r.iterator();
      assertEquals(new CoreMessage("someid"), i.next());
      assertFalse(i.hasNext());

      // the messages should be acknowledged in a resonable time, otherwise the test will timeout
      while(true)
      {
         if (!replicatorPeer.hasMessages())
         {
            return;
         }
         log.info("Thread sleeping for 500 ms");
         Thread.sleep(500);
      }
   }

   public void testOneInputPeerOneJChannelTwoMessages() throws Exception
   {
      jChannelOne.connect("testGroup");

      ReplicatorPeer peer = new ReplicatorPeer(dispatcherOne, "ReplicatorID");
      peer.start();
      assertTrue(peer.isStarted());

      ReplicatorOutput output = new ReplicatorOutput(dispatcherOne, "ReplicatorID");
      output.start();
      assertTrue(output.isStarted());

      ReceiverImpl r = new ReceiverImpl();
      r.resetInvocationCount();
      output.setReceiver(r);
      CoreMessage m1 = new CoreMessage("messageID1"), m2 = new CoreMessage("messageID2");

      assertTrue(peer.handle(m1));
      assertTrue(peer.handle(m2));

      r.waitForHandleInvocations(2);

      List messages = r.getMessages();
      assertEquals(2, messages.size());
      assertTrue(messages.contains(m1));
      assertTrue(messages.contains(m2));

      // the messages should be acknowledged in a resonable time, otherwise the test will timeout
      while(true)
      {
         if (!peer.hasMessages())
         {
            return;
         }
         log.info("Thread sleeping for 500 ms");
         Thread.sleep(500);
      }
   }

   public void testTwoInputPeersOneJChannel() throws Exception
   {
      jChannelOne.connect("testGroup");

      ReplicatorPeer inputPeerOne = new ReplicatorPeer(dispatcherOne, "ReplicatorID");
      ReplicatorPeer inputPeerTwo = new ReplicatorPeer(dispatcherOne, "ReplicatorID");
      inputPeerOne.start();
      inputPeerTwo.start();
      assertTrue(inputPeerOne.isStarted());
      assertTrue(inputPeerTwo.isStarted());

      ReplicatorOutput output = new ReplicatorOutput(dispatcherOne, "ReplicatorID");
      output.start();
      assertTrue(output.isStarted());

      ReceiverImpl r = new ReceiverImpl();
      r.resetInvocationCount();
      output.setReceiver(r);
      CoreMessage
            m1 = new CoreMessage("messageID1"),
            m2 = new CoreMessage("messageID2"),
            m3 = new CoreMessage("messageID3"),
            m4 = new CoreMessage("messageID4");

      assertTrue(inputPeerOne.handle(m1));
      assertTrue(inputPeerTwo.handle(m2));
      assertTrue(inputPeerOne.handle(m3));
      assertTrue(inputPeerTwo.handle(m4));

      r.waitForHandleInvocations(4);

      List messages = r.getMessages();
      assertEquals(4, messages.size());
      assertTrue(messages.contains(m1));
      assertTrue(messages.contains(m2));
      assertTrue(messages.contains(m3));
      assertTrue(messages.contains(m4));

      // the messages should be acknowledged in a resonable time, otherwise the test will timeout
      while(true)
      {
         if (!inputPeerOne.hasMessages())
         {
            break;
         }
         log.info("Thread sleeping for 500 ms");
         Thread.sleep(500);
      }
      while(true)
      {
         if (!inputPeerTwo.hasMessages())
         {
            break;
         }
         log.info("Thread sleeping for 500 ms");
         Thread.sleep(500);
      }
   }



   public void testTwoJChannels() throws Exception
   {
      jChannelOne.connect("testGroup");
      jChannelTwo.connect("testGroup");

      ReplicatorPeer inputPeer = new ReplicatorPeer(dispatcherOne, "ReplicatorID");
      inputPeer.start();
      assertTrue(inputPeer.isStarted());

      ReplicatorOutput output = new ReplicatorOutput(dispatcherTwo, "ReplicatorID");
      output.start();
      assertTrue(output.isStarted());

      ReceiverImpl r = new ReceiverImpl();
      r.resetInvocationCount();
      output.setReceiver(r);
      CoreMessage
            m1 = new CoreMessage("messageID1"),
            m2 = new CoreMessage("messageID2"),
            m3 = new CoreMessage("messageID3"),
            m4 = new CoreMessage("messageID4");

      assertTrue(inputPeer.handle(m1));
      assertTrue(inputPeer.handle(m2));
      assertTrue(inputPeer.handle(m3));
      assertTrue(inputPeer.handle(m4));

      r.waitForHandleInvocations(4);

      List messages = r.getMessages();
      assertEquals(4, messages.size());
      assertTrue(messages.contains(m1));
      assertTrue(messages.contains(m2));
      assertTrue(messages.contains(m3));
      assertTrue(messages.contains(m4));

      // the messages should be acknowledged in a resonable time, otherwise the test will timeout
      while(true)
      {
         if (!inputPeer.hasMessages())
         {
            break;
         }
         log.info("Thread sleeping for 500 ms");
         Thread.sleep(500);
      }
   }

   public void testTwoJChannelsOneInputTwoOutputs() throws Exception
   {
      jChannelOne.connect("testGroup");
      jChannelTwo.connect("testGroup");

      ReplicatorPeer inputPeer = new ReplicatorPeer(dispatcherOne, "ReplicatorID");
      inputPeer.start();
      assertTrue(inputPeer.isStarted());

      ReplicatorOutput outputOne = new ReplicatorOutput(dispatcherOne, "ReplicatorID");
      outputOne.start();
      assertTrue(outputOne.isStarted());

      ReceiverImpl rOne = new ReceiverImpl();
      rOne.resetInvocationCount();
      outputOne.setReceiver(rOne);

      ReplicatorOutput outputTwo = new ReplicatorOutput(dispatcherTwo, "ReplicatorID");
      outputTwo.start();
      assertTrue(outputTwo.isStarted());

      ReceiverImpl rTwo = new ReceiverImpl();
      rTwo.resetInvocationCount();
      outputTwo.setReceiver(rTwo);

      CoreMessage
            m1 = new CoreMessage("messageID1"),
            m2 = new CoreMessage("messageID2"),
            m3 = new CoreMessage("messageID3"),
            m4 = new CoreMessage("messageID4");

      assertTrue(inputPeer.handle(m1));
      assertTrue(inputPeer.handle(m2));
      assertTrue(inputPeer.handle(m3));
      assertTrue(inputPeer.handle(m4));

      rOne.waitForHandleInvocations(4);
      List messages = rOne.getMessages();
      assertEquals(4, messages.size());
      assertTrue(messages.contains(m1));
      assertTrue(messages.contains(m2));
      assertTrue(messages.contains(m3));
      assertTrue(messages.contains(m4));

      rTwo.waitForHandleInvocations(4);
      messages = rTwo.getMessages();
      assertEquals(4, messages.size());
      assertTrue(messages.contains(m1));
      assertTrue(messages.contains(m2));
      assertTrue(messages.contains(m3));
      assertTrue(messages.contains(m4));

      // the messages should be acknowledged in a resonable time, otherwise the test will timeout
      while(true)
      {
         if (!inputPeer.hasMessages())
         {
            break;
         }
         log.info("Thread sleeping for 500 ms");
         Thread.sleep(500);
      }
   }


   // Note: if you want the testing process to exit with 1 on a failed test, use
   //       java junit.textui.TestRunner fully.qualified.test.class.name
   public static void main(String[] args) throws Exception
   {
      TestRunner.run(ReplicatorPeerTest.class);
   }

}
