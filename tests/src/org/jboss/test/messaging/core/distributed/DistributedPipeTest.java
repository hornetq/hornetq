/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.distributed;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.util.RpcServer;
import org.jboss.messaging.core.Pipe;
import org.jboss.messaging.core.CoreMessage;
import org.jboss.messaging.core.distributed.DistributedPipeInput;
import org.jboss.messaging.core.distributed.DistributedPipeOutput;
import org.jboss.test.messaging.core.ReceiverImpl;
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.interfaces.Receiver;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.core.ReceiverImpl;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.Channel;
import org.jgroups.MessageListener;
import org.jgroups.MembershipListener;
import org.jgroups.JChannel;
import org.jgroups.Address;


import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class DistributedPipeTest extends MessagingTestCase
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

   private JChannel inputChannel, outputChannel;
   private RpcDispatcher inputDispatcher, outputDispatcher;
   private Address outputAddress;


   // Constructors --------------------------------------------------

   public DistributedPipeTest(String name)
   {
      super(name);
   }

   // Protected -----------------------------------------------------

   // Public --------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      inputChannel = new JChannel(props);
      inputDispatcher = new RpcDispatcher(inputChannel, null, null, null);

      outputChannel = new JChannel(props);
      RpcServer rpcServer = new RpcServer();
      outputDispatcher = new RpcDispatcher(outputChannel, null, null, rpcServer);

      inputChannel.connect("testGroup");
      outputChannel.connect("testGroup");
      outputAddress = outputChannel.getLocalAddress();
   }

   protected void tearDown() throws Exception
   {
      inputChannel.close();
      outputChannel.close();
      super.tearDown();
   }


   public void testDoNotHandleRemoteMessages() throws Exception
   {
      assertTrue(inputChannel.isConnected());

      DistributedPipeInput inputPipe = new DistributedPipeInput(true, inputDispatcher, "testPipe");
      Message m = new CoreMessage("");
      m.putHeader(Message.REMOTE_MESSAGE_HEADER, "");

      assertFalse(inputPipe.handle(m));
   }

   public void testNullOutputAddress() throws Exception
   {
      assertTrue(inputChannel.isConnected());

      DistributedPipeInput inputPipe = new DistributedPipeInput(true, inputDispatcher, "testPipe");

      Message m = new CoreMessage("");
      assertFalse(inputPipe.handle(m));
   }

   public void testValidDistributedPipe() throws Exception
   {
      assertTrue(inputChannel.isConnected());
      assertTrue(outputChannel.isConnected());
      DistributedPipeInput inputPipe =
            new DistributedPipeInput(true, inputDispatcher, outputAddress, "testPipe");

      ReceiverImpl r = new ReceiverImpl();
      DistributedPipeOutput outputPipe = new DistributedPipeOutput(outputDispatcher, "testPipe", r);

      Message m = new CoreMessage("");
      assertTrue(inputPipe.handle(m));

      Iterator i = r.iterator();
      assertEquals("", ((Message)i.next()).getMessageID());
      assertFalse(i.hasNext());
   }

   public void testDenyingReceiver() throws Exception
   {
      assertTrue(inputChannel.isConnected());
      assertTrue(outputChannel.isConnected());
      DistributedPipeInput inputPipe =
            new DistributedPipeInput(true, inputDispatcher, outputAddress, "testPipe");

      ReceiverImpl r = new ReceiverImpl(ReceiverImpl.DENYING);
      DistributedPipeOutput outputPipe = new DistributedPipeOutput(outputDispatcher, "testPipe", r);

      Message m = new CoreMessage("");
      assertFalse(inputPipe.handle(m));

      Iterator i = r.iterator();
      assertFalse(i.hasNext());
   }

   public void testBrokenReceiver() throws Exception
   {
      assertTrue(inputChannel.isConnected());
      assertTrue(outputChannel.isConnected());
      DistributedPipeInput inputPipe =
            new DistributedPipeInput(true, inputDispatcher, outputAddress, "testPipe");

      ReceiverImpl r = new ReceiverImpl(ReceiverImpl.BROKEN);
      DistributedPipeOutput outputPipe = new DistributedPipeOutput(outputDispatcher, "testPipe", r);

      Message m = new CoreMessage("");
      assertFalse(inputPipe.handle(m));

      Iterator i = r.iterator();
      assertFalse(i.hasNext());
   }


}
