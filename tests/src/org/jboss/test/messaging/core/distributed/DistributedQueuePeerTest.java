/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.distributed;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.util.RpcServer;
import org.jboss.messaging.core.CoreMessage;
import org.jboss.messaging.core.distributed.DistributedQueuePeer;
import org.jboss.test.messaging.core.ReceiverImpl;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.JChannel;

import java.util.Iterator;

import junit.textui.TestRunner;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class DistributedQueuePeerTest extends MessagingTestCase
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

   public DistributedQueuePeerTest(String name)
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


   public void testOneQueuePeerOneJChannel() throws Exception
   {
      jChannelOne.connect("testGroup");

      DistributedQueuePeer queue = new DistributedQueuePeer(dispatcherOne, "QueueOne");
      queue.connect();
      assertTrue(queue.isConnected());

      ReceiverImpl r = new ReceiverImpl();
      queue.add(r);

      assertTrue(queue.handle(new CoreMessage("someid")));
      Iterator i = r.iterator();
      assertEquals(new CoreMessage("someid"), i.next());
      assertFalse(i.hasNext());
   }

//   public void testTwoQueuePeersOneJChannel() throws Exception
//   {
//      jChannelOne.connect("testGroup");
//
//      DistributedQueuePeer queuePeerOne = new DistributedQueuePeer(dispatcherOne, "QueueOne");
//      DistributedQueuePeer queuePeerTwo = new DistributedQueuePeer(dispatcherOne, "QueueOne");
//      queuePeerOne.connect();
//      queuePeerTwo.connect();
//      assertTrue(queuePeerOne.isConnected());
//      assertTrue(queuePeerTwo.isConnected());
//
//      ReceiverImpl r = new ReceiverImpl();
//      queuePeerOne.add(r);
//
//      assertTrue(queuePeerTwo.handle(new CoreMessage("someid")));
//      Iterator i = r.iterator();
//      assertEquals(new CoreMessage("someid"), i.next());
//      assertFalse(i.hasNext());
//   }


   public void testOneQueuePeerTwoJChannels() throws Exception
   {
      jChannelOne.connect("testGroup");
      jChannelTwo.connect("testGroup");

      DistributedQueuePeer queue = new DistributedQueuePeer(dispatcherOne, "QueueOne");
      queue.connect();
      assertTrue(queue.isConnected());

      ReceiverImpl r = new ReceiverImpl();
      queue.add(r);

      assertTrue(queue.handle(new CoreMessage("someid")));
      Iterator i = r.iterator();
      assertEquals(new CoreMessage("someid"), i.next());
      assertFalse(i.hasNext());
   }

//   public void testTwoQueuePeersOnSeparateChannels() throws Exception
//   {
//      jChannelOne.connect("testGroup");
//      jChannelTwo.connect("testGroup");
//
//      DistributedQueuePeer queuePeerOne = new DistributedQueuePeer(dispatcherOne, "AQueue");
//      DistributedQueuePeer queuePeerTwo = new DistributedQueuePeer(dispatcherTwo, "AQueue");
//      queuePeerOne.connect();
//      queuePeerTwo.connect();
//
//      assertTrue(queuePeerOne.isConnected());
//      assertTrue(queuePeerTwo.isConnected());
//
//      ReceiverImpl r = new ReceiverImpl();
//      queuePeerTwo.add(r);
//
//      assertTrue(queuePeerOne.handle(new CoreMessage("someid")));
//      Iterator i = r.iterator();
//      assertEquals(new CoreMessage("someid"), i.next());
//      assertFalse(i.hasNext());
//   }



   public static void main(String[] args) throws Exception
   {
      TestRunner.run(DistributedQueuePeerTest.class);
   }

}
