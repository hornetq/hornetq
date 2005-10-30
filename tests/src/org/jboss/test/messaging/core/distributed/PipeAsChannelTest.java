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
package org.jboss.test.messaging.core.distributed;

import org.jboss.test.messaging.core.SingleOutputChannelSupportTest;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class PipeAsChannelTest extends SingleOutputChannelSupportTest
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
//   private JChannel inputJChannel, outputJChannel;
//   private RpcDispatcher inputDispatcher, outputDispatcher;
//   private PipeOutput pipeOutput;
//   private Address outputAddress;
//
   // Constructors --------------------------------------------------

   public PipeAsChannelTest(String name)
   {
      super(name);
   }
//
//   public void setUp() throws Exception
//   {
//      inputJChannel = new JChannel(props);
//      inputDispatcher = new RpcDispatcher(inputJChannel, null, null, null);
//
//      outputJChannel = new JChannel(props);
//      outputDispatcher = new RpcDispatcher(outputJChannel, null, null, new RpcServer());
//
//      inputJChannel.connect("testGroup");
//      outputJChannel.connect("testGroup");
//      outputAddress = outputJChannel.getLocalAddress();
//
//      // Create a receiver and a Pipe to be tested by the superclass tests
//      receiverOne = new ReceiverImpl("ReceiverOne", ReceiverImpl.HANDLING);
//      pipeOutput = new PipeOutput("DistributedPipeID", receiverOne);
//      pipeOutput.register((RpcServer)outputDispatcher.getServerObject());
//      channel = new Pipe(true, inputDispatcher, outputAddress, "DistributedPipeID");
//
//      // important, I need the channel set at this point
//      super.setUp();
//   }
//
//   public void tearDown()throws Exception
//   {
//      channel = null;
//      receiverOne = null;
//      inputJChannel.close();
//      outputJChannel.close();
//      super.tearDown();
//   }
//
//   //
//   // This test class also runs all ChannelSupportTest's tests
//   //
//
//   public void testChannelNotConnected() throws Exception
//   {
//      inputJChannel.close();
//      assertFalse(inputJChannel.isOpen());
//
//      Pipe inputPipe = new Pipe(true, inputDispatcher, outputAddress, "testPipe");
//      PipeOutput outputPipe = new PipeOutput("testPipe", new ReceiverImpl());
//      outputPipe.register((RpcServer)outputDispatcher.getServerObject());
//
//      assertFalse(inputPipe.handle(new MessageSupport("")));
//   }
//
//
//   public void testDoNotHandleRemoteMessages() throws Exception
//   {
//      assertTrue(inputJChannel.isConnected());
//
//      Pipe inputPipe = new Pipe(true, inputDispatcher, null, "testPipe");
//      Routable m = new MessageSupport("");
//      m.putHeader(Routable.REMOTE_ROUTABLE, "");
//
//      assertFalse(inputPipe.handle(m));
//   }
//
//   public void testNullOutputAddress() throws Exception
//   {
//      assertTrue(inputJChannel.isConnected());
//
//      Pipe inputPipe = new Pipe(true, inputDispatcher, null, "testPipe");
//      Routable m = new MessageSupport("");
//      assertFalse(inputPipe.handle(m));
//   }
//
//   public void testValidDistributedPipe() throws Exception
//   {
//      assertTrue(inputJChannel.isConnected());
//      assertTrue(outputJChannel.isConnected());
//      Pipe inputPipe = new Pipe(true, inputDispatcher, outputAddress, "testPipe");
//
//      ReceiverImpl r = new ReceiverImpl();
//      PipeOutput outputPipe = new PipeOutput("testPipe", r);
//      outputPipe.register((RpcServer)outputDispatcher.getServerObject());
//
//
//      Routable m = new MessageSupport("");
//      assertTrue(inputPipe.handle(m));
//
//      Iterator i = r.iterator();
//      Routable received = (Routable)i.next();
//      // TODO ((Message)m).getMessageID() is a hack! Added to pass the tests. Change it!
//      assertEquals("", ((Message)received).getMessageID());
//      // make sure the message was marked as "remote"
//      assertTrue(received.getHeader(Routable.REMOTE_ROUTABLE) != null);
//      assertFalse(i.hasNext());
//   }
//
//   public void testNackingReceiver() throws Exception
//   {
//      assertTrue(inputJChannel.isConnected());
//      assertTrue(outputJChannel.isConnected());
//      Pipe inputPipe = new Pipe(true, inputDispatcher, outputAddress, "testPipe");
//
//      ReceiverImpl nacking = new ReceiverImpl(ReceiverImpl.NACKING);
//      PipeOutput outputPipe = new PipeOutput("testPipe", nacking);
//      outputPipe.register((RpcServer)outputDispatcher.getServerObject());
//
//      Routable m = new MessageSupport("");
//      assertFalse(inputPipe.handle(m));
//
//      Iterator i = nacking.iterator();
//      assertFalse(i.hasNext());
//   }
//
//   public void testBrokenReceiver() throws Exception
//   {
//      assertTrue(inputJChannel.isConnected());
//      assertTrue(outputJChannel.isConnected());
//      Pipe inputPipe = new Pipe(true, inputDispatcher, outputAddress, "testPipe");
//
//      ReceiverImpl r = new ReceiverImpl(ReceiverImpl.BROKEN);
//      PipeOutput outputPipe = new PipeOutput("testPipe", r);
//      outputPipe.register((RpcServer)outputDispatcher.getServerObject());
//
//
//      Routable m = new MessageSupport("");
//      assertFalse(inputPipe.handle(m));
//
//      Iterator i = r.iterator();
//      assertFalse(i.hasNext());
//   }
//
//   public static void main(String[] args) throws Exception
//   {
//      TestRunner.run(ReplicatorTest.class);
//   }
//

   public void testNoop()
   {
   }
   
}
