/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tools;

import org.jboss.messaging.core.MessageSupport;
import org.jboss.messaging.core.distributed.Pipe;
import org.jboss.messaging.core.distributed.PipeOutput;
import org.jboss.messaging.interfaces.Routable;
import org.jboss.messaging.util.RpcServer;
import org.jgroups.stack.IpAddress;

/**
 * Class that provides a command line interface to a Pipe or a PipeOutput
 * but never to both, they are mutually exclusive. Run it with Clester.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class PipeClient extends RpcDispatcherClient
{

   // Attributes ----------------------------------------------------
   
   private Pipe pipeInput;
   private PipeOutput pipeOutput;
   private int counter = 0;

   // Constructors --------------------------------------------------

   public PipeClient() throws Exception
   {
      super(new RpcServer());
   }

   // Public --------------------------------------------------------

   public void setAsInput(String pipeID) throws Exception
   {
      if (pipeOutput != null)
      {
         throw new Exception("LocalPipe output for " + pipeOutput.getID() +
                             "already instantiated, cannot use this client as pipe input");
      }

      pipeInput = new Pipe(true, rpcDispatcher, null, pipeID);

   }

   public void setOutputAddress(String host, int port)
   {
      pipeInput.setOutputAddress(new IpAddress(host, port));
   }


   public void setAsOutput(String pipeID) throws Exception
   {
      if (pipeInput != null)
      {
         throw new Exception("LocalPipe input for " + pipeOutput.getID() +
                             "already instantiated, cannot use this client as pipe output");
      }

      pipeOutput = new PipeOutput(pipeID, new ReceiverImpl("Default Output Receiver"));
      pipeOutput.register((RpcServer)rpcDispatcher.getServerObject(), pipeID);
   }

   public void send()
   {
       Routable m = new MessageSupport(new Integer(counter++));
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
