/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tools;

import org.jboss.messaging.core.Pipe;
import org.jboss.messaging.core.CoreMessage;
import org.jboss.messaging.core.distributed.DistributedPipeInput;
import org.jboss.messaging.core.distributed.DistributedPipeOutput;
import org.jboss.messaging.core.distributed.DistributedQueuePeer;
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.interfaces.Channel;
import org.jboss.messaging.util.RpcServer;
import org.jgroups.JChannel;
import org.jgroups.stack.IpAddress;
import org.jgroups.blocks.RpcDispatcher;

/**
 * Class that provides a command line interface to build distributed Queues. Run it with Clester.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class DistributedQueueClient extends RpcDispatcherClient
{

   // Attributes ----------------------------------------------------
   
   private DistributedQueuePeer queuePeer;
   private int counter = 0;

   // Constructors --------------------------------------------------

   public DistributedQueueClient() throws Exception
   {
      super(new RpcServer());
      queuePeer = new DistributedQueuePeer(rpcDispatcher, "DistributedQueueID");
   }

   // Public --------------------------------------------------------


   public void connect() throws Exception
   {
      queuePeer.connect();
   }

   public void send()
   {
       Message m = new CoreMessage(new Integer(counter++));
       System.out.println("Sending "+m+" to the pipe input: "+queuePeer.handle(m));
   }

   public String dump()
   {
      return queuePeer.dump();
   }

   public void exit() {
      System.exit(0);
   }
}
