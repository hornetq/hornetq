/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.distributed;

import org.jboss.test.messaging.MessagingTestCase;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReplicatorOutputTest extends MessagingTestCase
{
//   // Constants -----------------------------------------------------
//
//   private String props =
//         "UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=32):"+
//         "PING(timeout=3050;num_initial_members=6):"+
//         "FD(timeout=3000):"+
//         "VERIFY_SUSPECT(timeout=1500):"+
//         "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800):"+
//         "UNICAST(timeout=600,1200,2400,4800):"+
//         "pbcast.STABLE(desired_avg_gossip=10000):"+
//         "FRAG:"+
//         "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=true;print_local_addr=true)";
//
//   // Attributes ----------------------------------------------------
//
//   private JChannel jChannelOne, jChannelTwo;
//   private RpcDispatcher dispatcherOne, dispatcherTwo;
//
   // Constructors --------------------------------------------------

   public ReplicatorOutputTest(String name)
   {
      super(name);
   }

//   // Protected -----------------------------------------------------
//
//   // Public --------------------------------------------------------
//
//   protected void setUp() throws Exception
//   {
//      super.setUp();
//
//      jChannelOne = new JChannel(props);
//      dispatcherOne = new RpcDispatcher(jChannelOne, null, null, new RpcServer());
//
//      jChannelTwo = new JChannel(props);
//      dispatcherTwo = new RpcDispatcher(jChannelTwo, null, null, new RpcServer());
//
//      // Don't connect the channels yet.
//   }
//
//   protected void tearDown() throws Exception
//   {
//      jChannelOne.close();
//      jChannelTwo.close();
//      super.tearDown();
//   }
//
//
//   public void testNullRpcServer() throws Exception
//   {
//      JChannel channel = new JChannel();
//      RpcDispatcher dispatcher = new RpcDispatcher(channel, null, null, null);
//
//      try
//      {
//         new ReplicatorOutput(dispatcher, "doesntmatter");
//         fail("Should have thrown IllegalStateException");
//      }
//      catch(IllegalStateException e)
//      {
//         // OK
//      }
//   }
//
//   public void testNoRpcServer() throws Exception
//   {
//      JChannel channel = new JChannel();
//      RpcDispatcher dispatcher = new RpcDispatcher(channel, null, null, new Object());
//
//      try
//      {
//         new ReplicatorOutput(dispatcher, "doesntmatter");
//         fail("Should have thrown IllegalStateException");
//      }
//      catch(IllegalStateException e)
//      {
//         // OK
//      }
//   }
//
//   //
//   // connect tests
//   //
//
//
//   public void testConnectWithTheJChannelNotConnected() throws Exception
//   {
//      JChannel jChannel = new JChannel();
//      RpcDispatcher dispatcher = new RpcDispatcher(jChannel, null, null, new RpcServer());
//      assertFalse(jChannel.isConnected());
//      ReplicatorOutput peerOne = new ReplicatorOutput(dispatcher, "doesntmatter");
//
//      try
//      {
//         peerOne.start();
//         fail("Should have thrown DistributedException");
//      }
//      catch(DistributedException e)
//      {
//         // Ok
//      }
//   }
//
//   public void testOneOutputReplicatorPeerOneJChannelNoInput() throws Exception
//   {
//      jChannelOne.connect("testGroup");
//
//      ReplicatorOutput peer = new ReplicatorOutput(dispatcherOne, "Replicator");
//      peer.start();
//      assertTrue(peer.isStarted());
//   }
//
//   public void testTwoOutputReplicatorPeersOneJChannelNoInput() throws Exception
//   {
//      jChannelOne.connect("testGroup");
//
//      ReplicatorOutput peerOne = new ReplicatorOutput(dispatcherOne, "Replicator");
//      ReplicatorOutput peerTwo = new ReplicatorOutput(dispatcherOne, "Replicator");
//      peerOne.start();
//      peerTwo.start();
//      assertTrue(peerOne.isStarted());
//      assertTrue(peerTwo.isStarted());
//   }
//
//   public void testOneOutputReplicatorPeerOneJChannel() throws Exception
//   {
//      jChannelOne.connect("testGroup");
//
//      Replicator input = new Replicator(dispatcherOne, "Replicator");
//      input.start();
//
//      ReplicatorOutput peer = new ReplicatorOutput(dispatcherOne, "Replicator");
//      peer.start();
//      assertTrue(peer.isStarted());
//   }
//
//   public void testTwoOutputReplicatorPeersOneJChannel() throws Exception
//   {
//      jChannelOne.connect("testGroup");
//
//      Replicator input = new Replicator(dispatcherOne, "Replicator");
//      input.start();
//
//      ReplicatorOutput outputOne = new ReplicatorOutput(dispatcherOne, "Replicator");
//      ReplicatorOutput outputTwo = new ReplicatorOutput(dispatcherOne, "Replicator");
//      outputOne.start();
//      outputTwo.start();
//      assertTrue(outputOne.isStarted());
//      assertTrue(outputTwo.isStarted());
//   }
//
//   // Note: if you want the testing process to exit with 1 on a failed test, use
//   //       java junit.textui.TestRunner fully.qualified.test.class.name
//   public static void main(String[] args) throws Exception
//   {
//      TestRunner.run(ReplicatorOutputTest.class);
//   }
   public void testNoop()
   {
   }
   


}
