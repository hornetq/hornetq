/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.distributed;

import org.jboss.test.messaging.core.ReceiverImpl;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.util.RpcServer;
import org.jboss.messaging.core.distributed.ReplicatorOutput;
import org.jboss.messaging.core.distributed.Replicator;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.message.MessageSupport;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.JChannel;

import junit.textui.TestRunner;

import java.util.Iterator;
import java.util.List;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReplicatorTest extends MessagingTestCase
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

   // used by the local test cases
   private JChannel inputJChannel, outputJChannel;
   private RpcDispatcher inputDispatcher, outputDispatcher;

   // Constructors --------------------------------------------------

   public ReplicatorTest(String name)
   {
      super(name);
   }

   // Protected -----------------------------------------------------

   // Public --------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      inputJChannel = new JChannel(props);
      inputDispatcher = new RpcDispatcher(inputJChannel, null, null, new RpcServer());

      outputJChannel = new JChannel(props);
      outputDispatcher = new RpcDispatcher(outputJChannel, null, null, new RpcServer());

      // do not connect yet the channels
   }

   protected void tearDown() throws Exception
   {
      Thread.sleep(500);
      inputJChannel.close();
      outputJChannel.close();
      super.tearDown();
   }

   //
   // This test class also runs all ChannelSupportTest's tests
   //

   public void testNullRpcServer() throws Exception
   {
      JChannel jChannel = new JChannel();
      RpcDispatcher dispatcher = new RpcDispatcher(jChannel, null, null, null);

      try
      {
         new Replicator(dispatcher, "doesntmatter");
         fail("Should have thrown IllegalStateException");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }

   public void testNoRpcServer() throws Exception
   {
      JChannel jChannel = new JChannel();
      RpcDispatcher dispatcher = new RpcDispatcher(jChannel, null, null, new Object());

      try
      {
         new Replicator(dispatcher, "doesntmatter");
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
      Replicator peerOne = new Replicator(dispatcher, "doesntmatter");

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
      Replicator input = new Replicator(inputDispatcher, "ReplicatorID");
      assertFalse(input.handle(new MessageSupport("messageID")));
   }

   public void testOneInputPeerOneJChannel() throws Exception
   {
      inputJChannel.connect("testGroup");

      Replicator replicatorPeer = new Replicator(inputDispatcher, "ReplicatorID");
      replicatorPeer.start();
      assertTrue(replicatorPeer.isStarted());


      ReplicatorOutput output = new ReplicatorOutput(inputDispatcher, "ReplicatorID");
      output.start();
      assertTrue(output.isStarted());

      ReceiverImpl r = new ReceiverImpl();
      output.setReceiver(r);

      assertTrue(replicatorPeer.handle(new MessageSupport("someid")));

      r.waitForHandleInvocations(1);

      Iterator i = r.iterator();
      assertEquals(new MessageSupport("someid"), i.next());
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
      inputJChannel.connect("testGroup");

      Replicator peer = new Replicator(inputDispatcher, "ReplicatorID");
      peer.start();
      assertTrue(peer.isStarted());

      ReplicatorOutput output = new ReplicatorOutput(inputDispatcher, "ReplicatorID");
      output.start();
      assertTrue(output.isStarted());

      ReceiverImpl r = new ReceiverImpl();
      r.resetInvocationCount();
      output.setReceiver(r);
      MessageSupport m1 = new MessageSupport("messageID1"), m2 = new MessageSupport("messageID2");

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
      inputJChannel.connect("testGroup");

      Replicator inputPeerOne = new Replicator(inputDispatcher, "ReplicatorID");
      Replicator inputPeerTwo = new Replicator(inputDispatcher, "ReplicatorID");
      inputPeerOne.start();
      inputPeerTwo.start();
      assertTrue(inputPeerOne.isStarted());
      assertTrue(inputPeerTwo.isStarted());

      ReplicatorOutput output = new ReplicatorOutput(inputDispatcher, "ReplicatorID");
      output.start();
      assertTrue(output.isStarted());

      ReceiverImpl r = new ReceiverImpl();
      r.resetInvocationCount();
      output.setReceiver(r);
      MessageSupport
            m1 = new MessageSupport("messageID1"),
            m2 = new MessageSupport("messageID2"),
            m3 = new MessageSupport("messageID3"),
            m4 = new MessageSupport("messageID4");

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
      inputJChannel.connect("testGroup");
      outputJChannel.connect("testGroup");

      Replicator inputPeer = new Replicator(inputDispatcher, "ReplicatorID");
      inputPeer.start();
      assertTrue(inputPeer.isStarted());

      ReplicatorOutput output = new ReplicatorOutput(outputDispatcher, "ReplicatorID");
      output.start();
      assertTrue(output.isStarted());

      ReceiverImpl r = new ReceiverImpl();
      r.resetInvocationCount();
      output.setReceiver(r);
      MessageSupport
            m1 = new MessageSupport("messageID1"),
            m2 = new MessageSupport("messageID2"),
            m3 = new MessageSupport("messageID3"),
            m4 = new MessageSupport("messageID4");

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
      inputJChannel.connect("testGroup");
      outputJChannel.connect("testGroup");

      Replicator inputPeer = new Replicator(inputDispatcher, "ReplicatorID");
      inputPeer.start();
      assertTrue(inputPeer.isStarted());

      ReplicatorOutput outputOne = new ReplicatorOutput(inputDispatcher, "ReplicatorID");
      outputOne.start();
      assertTrue(outputOne.isStarted());

      ReceiverImpl rOne = new ReceiverImpl();
      rOne.resetInvocationCount();
      outputOne.setReceiver(rOne);

      ReplicatorOutput outputTwo = new ReplicatorOutput(outputDispatcher, "ReplicatorID");
      outputTwo.start();
      assertTrue(outputTwo.isStarted());

      ReceiverImpl rTwo = new ReceiverImpl();
      rTwo.resetInvocationCount();
      outputTwo.setReceiver(rTwo);

      MessageSupport
            m1 = new MessageSupport("messageID1"),
            m2 = new MessageSupport("messageID2"),
            m3 = new MessageSupport("messageID3"),
            m4 = new MessageSupport("messageID4");

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
      TestRunner.run(ReplicatorTest.class);
   }

}
