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
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.interfaces.Channel;
import org.jboss.messaging.util.RpcServer;
import org.jgroups.JChannel;
import org.jgroups.stack.IpAddress;
import org.jgroups.blocks.RpcDispatcher;

/**
 * Class that provides a command line interface to a DistributedPipeInput or a DistributedPipeOutput
 * but never to both, they are mutually exclusive. Run it with Clester.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class DistributedPipeClient extends RpcDispatcherClient
{

   // Attributes ----------------------------------------------------
   
   private DistributedPipeInput pipeInput;
   private DistributedPipeOutput pipeOutput;
   private int counter = 0;

   // Constructors --------------------------------------------------

   public DistributedPipeClient() throws Exception
   {
      super(new RpcServer());
   }

   // Public --------------------------------------------------------

   public void setAsInput(String pipeID) throws Exception
   {
      if (pipeOutput != null)
      {
         throw new Exception("Pipe output for " + pipeOutput.getPipeID() +
                             "already instantiated, cannot use this client as pipe input");
      }

      pipeInput = new DistributedPipeInput(true, rpcDispatcher, pipeID);

   }

   public void setOutputAddress(String host, int port)
   {
      pipeInput.setOutputAddress(new IpAddress(host, port));
   }


   public void setAsOutput(String pipeID) throws Exception
   {
      if (pipeInput != null)
      {
         throw new Exception("Pipe input for " + pipeOutput.getPipeID() +
                             "already instantiated, cannot use this client as pipe output");
      }

      pipeOutput = new DistributedPipeOutput(rpcDispatcher, pipeID,
                                             new ReceiverImpl("Default Output Receiver"));
   }

   public void send()
   {
       Message m = new CoreMessage(new Integer(counter++));
       System.out.println("Sending "+m+" to the pipe input: "+pipeInput.handle(m));
   }

   public String dump()
   {
      return pipeInput.dump();
   }


   public void exit() {
      System.exit(0);
   }
}
