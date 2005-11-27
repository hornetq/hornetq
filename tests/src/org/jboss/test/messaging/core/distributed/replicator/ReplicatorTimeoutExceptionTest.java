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

import org.jboss.test.messaging.MessagingTestCase;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReplicatorTimeoutExceptionTest extends MessagingTestCase
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
   // Constructors --------------------------------------------------

   public ReplicatorTimeoutExceptionTest(String name)
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
//   }
//
//   protected void tearDown() throws Exception
//   {
//      super.tearDown();
//   }
//
//   /**
//    * This test can be used to trigger a deadlock and the TimeoutException that shows up in a
//    * race condition when the delivery of a yet nacked message is attempted before the output had
//    * a chance to ACK the message.  Run it several times ....
//    *
//    * TODO: fix the condition, I suspect is because of a problem in JGroups
//    */
//   public void testRPCTimeout() throws Exception
//   {
//      JChannel inputJChannel = new JChannel(props);
//      JChannel outputJChannel = new JChannel(props);
//      RpcDispatcher inputDispatcher = new RpcDispatcher(inputJChannel, null, null, new RpcServer());
//      RpcDispatcher outputDispatcher = new RpcDispatcher(outputJChannel, null, null, new RpcServer());
//
//      inputJChannel.connect("ReplicatorTestGroup");
//      outputJChannel.connect("ReplicatorTestGroup");
//
//      ReceiverImpl receiver = new ReceiverImpl("ReceiverID", ReceiverImpl.NACKING);
//      ReplicatorOutput output = new ReplicatorOutput(outputDispatcher, "ReplicatorID", receiver);
//      output.start();
//
//      Replicator input = new Replicator(inputDispatcher, "ReplicatorID");
//      input.start();
//
//      assertTrue(input.setSynchronous(false));
//      assertTrue(input.handle(new RoutableSupport("routableID1")));
//      assertTrue(input.hasMessages());
//
//      receiver.setState(ReceiverImpl.HANDLING);
//      assertTrue(input.deliver());
//
//      assertEquals(1, receiver.getMessages().size());
//      assertTrue(receiver.contains("routableID1"));
//
//      Thread.sleep(500);
//
//      inputJChannel.close();
//      outputJChannel.close();
//
//   }
//

   public void testNoop()
   {
   }
   
}
