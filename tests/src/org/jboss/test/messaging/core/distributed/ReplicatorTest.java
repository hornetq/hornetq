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
import org.jboss.messaging.core.distributed.PipeInput;
import org.jboss.messaging.core.distributed.PipeOutput;
import org.jboss.test.messaging.core.ReceiverImpl;
import org.jboss.messaging.interfaces.Message;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.JChannel;
import org.jgroups.Address;


import java.util.Iterator;

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

   private JChannel inputChannel, outputChannel;
   private RpcDispatcher outputDispatcher;

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

      inputChannel = new JChannel(props);

      outputChannel = new JChannel(props);
      RpcServer rpcServer = new RpcServer();
      outputDispatcher = new RpcDispatcher(outputChannel, null, null, rpcServer);

      inputChannel.connect("testGroup");
      outputChannel.connect("testGroup");
   }

   protected void tearDown() throws Exception
   {
      inputChannel.close();
      outputChannel.close();
      super.tearDown();
   }


   public void testChannelNotConnected() throws Exception
   {
      inputChannel.close();
      assertFalse(inputChannel.isOpen());

//      PipeInput inputPipe =
//            new PipeInput(true, inputDispatcher, outputAddress, "testPipe");
//      PipeOutput outputPipe =
//            new PipeOutput("testPipe", new ReceiverImpl());
//      outputPipe.register((RpcServer)outputDispatcher.getServerObject(), "testPipe");

//      assertFalse(inputPipe.handle(new CoreMessage("")));
   }


//   public void testDoNotHandleRemoteMessages() throws Exception
//   {
//      assertTrue(inputChannel.isConnected());
//
//      PipeInput inputPipe =
//            new PipeInput(true, inputDispatcher, null, "testPipe");
//      Message m = new CoreMessage("");
//      m.putHeader(Message.REMOTE_MESSAGE, "");
//
//      assertFalse(inputPipe.handle(m));
//   }
//
//   public void testNullOutputAddress() throws Exception
//   {
//      assertTrue(inputChannel.isConnected());
//
//      PipeInput inputPipe =
//            new PipeInput(true, inputDispatcher, null, "testPipe");
//
//      Message m = new CoreMessage("");
//      assertFalse(inputPipe.handle(m));
//   }
//
//   public void testValidDistributedPipe() throws Exception
//   {
//      assertTrue(inputChannel.isConnected());
//      assertTrue(outputChannel.isConnected());
//      PipeInput inputPipe =
//            new PipeInput(true, inputDispatcher, outputAddress, "testPipe");
//
//      ReceiverImpl r = new ReceiverImpl();
//      PipeOutput outputPipe =
//            new PipeOutput("testPipe", r);
//      outputPipe.register((RpcServer)outputDispatcher.getServerObject(), "testPipe");
//
//
//      Message m = new CoreMessage("");
//      assertTrue(inputPipe.handle(m));
//
//      Iterator i = r.iterator();
//      Message received = (Message)i.next();
//      assertEquals("", received.getID());
//      // make sure the message was marked as "remote"
//      assertTrue(received.getHeader(Message.REMOTE_MESSAGE) != null);
//      assertFalse(i.hasNext());
//   }
//
//   public void testDenyingReceiver() throws Exception
//   {
//      assertTrue(inputChannel.isConnected());
//      assertTrue(outputChannel.isConnected());
//      PipeInput inputPipe =
//            new PipeInput(true, inputDispatcher, outputAddress, "testPipe");
//
//      ReceiverImpl r = new ReceiverImpl(ReceiverImpl.DENYING);
//      PipeOutput outputPipe =
//            new PipeOutput("testPipe", r);
//      outputPipe.register((RpcServer)outputDispatcher.getServerObject(), "testPipe");
//
//      Message m = new CoreMessage("");
//      assertFalse(inputPipe.handle(m));
//
//      Iterator i = r.iterator();
//      assertFalse(i.hasNext());
//   }
//
//   public void testBrokenReceiver() throws Exception
//   {
//      assertTrue(inputChannel.isConnected());
//      assertTrue(outputChannel.isConnected());
//      PipeInput inputPipe =
//            new PipeInput(true, inputDispatcher, outputAddress, "testPipe");
//
//      ReceiverImpl r = new ReceiverImpl(ReceiverImpl.BROKEN);
//      PipeOutput outputPipe =
//            new PipeOutput("testPipe", r);
//      outputPipe.register((RpcServer)outputDispatcher.getServerObject(), "testPipe");
//
//
//      Message m = new CoreMessage("");
//      assertFalse(inputPipe.handle(m));
//
//      Iterator i = r.iterator();
//      assertFalse(i.hasNext());
//   }
}
