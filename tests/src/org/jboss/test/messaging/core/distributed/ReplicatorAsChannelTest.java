/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.distributed;

import org.jboss.messaging.core.tools.ReceiverImpl;
import org.jboss.test.messaging.core.TransactionalChannelSupportTest;
import org.jboss.messaging.core.util.RpcServer;
import org.jboss.messaging.core.distributed.ReplicatorOutput;
import org.jboss.messaging.core.distributed.Replicator;
import org.jboss.messaging.core.tools.ReceiverImpl;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.JChannel;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReplicatorAsChannelTest extends TransactionalChannelSupportTest
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
   private JChannel jChannelOne, jChannelTwo;
   private RpcDispatcher dispatcherOne, dispatcherTwo;

   // Constructors --------------------------------------------------

   public ReplicatorAsChannelTest(String name)
   {
      super(name);
   }

   // Protected -----------------------------------------------------

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      jChannelOne = new JChannel(props);
      dispatcherOne = new RpcDispatcher(jChannelOne, null, null, new RpcServer());

      jChannelTwo = new JChannel(props);
      dispatcherTwo = new RpcDispatcher(jChannelTwo, null, null, new RpcServer());

      jChannelOne.connect("ReplicatorTestGroup");
      jChannelTwo.connect("ReplicatorTestGroup");

      // Create a receiver and a Replicator to be tested by the superclass tests
      receiverOne = new ReceiverImpl("ReceiverOne", ReceiverImpl.HANDLING);
      ReplicatorOutput output = new ReplicatorOutput(dispatcherTwo, "ReplicatorID", receiverOne);
      output.start();
      channel = new Replicator(dispatcherOne, "ReplicatorID");
      ((Replicator)channel).start();

      super.setUp();
   }

   public void tearDown() throws Exception
   {
      channel = null;
      receiverOne = null;

      Thread.sleep(500);

      jChannelOne.close();
      jChannelTwo.close();
      super.tearDown();
   }

   //
   // This test class runs all ChannelSupportTest tests
   //

}
