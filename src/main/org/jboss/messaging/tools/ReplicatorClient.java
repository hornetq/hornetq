/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tools;

import org.jboss.messaging.core.CoreMessage;
import org.jboss.messaging.core.distributed.DistributedQueuePeer;
import org.jboss.messaging.core.distributed.PipeInput;
import org.jboss.messaging.core.distributed.PipeOutput;
import org.jboss.messaging.core.distributed.Replicator;
import org.jboss.messaging.core.distributed.ReplicatorOutput;
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.util.RpcServer;
import org.jgroups.stack.IpAddress;

/**
 * Class that provides a command line interface to build distributed Queues. Run it with Clester.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReplicatorClient extends RpcDispatcherClient
{

   // Attributes ----------------------------------------------------

   private Replicator replicator;
   private ReplicatorOutput replicatorOutput;
   private int counter = 0;

   // Constructors --------------------------------------------------

   public ReplicatorClient() throws Exception
   {
      // no need for a RpcServer
      super(null);
   }

   // Public --------------------------------------------------------

   public void setAsReplicator(String replicatorID) throws Exception
   {
      if (replicatorOutput != null)
      {
         throw new Exception("Replicator output for " + replicatorOutput.getID() +
                             "already instantiated, cannot use this client as replicator");
      }
      replicator = new Replicator(true, rpcDispatcher, null);
   }

   public void setAsOutput(String replicatorID) throws Exception
   {
      if (replicator != null)
      {
         throw new Exception("Replicator for " + replicator.getID() +
                             "already instantiated, cannot use this client as replicator output");
      }
      replicatorOutput = new ReplicatorOutput(replicatorID,
                                              new ReceiverImpl("Default Receiver"));
      replicatorOutput.attach(rpcDispatcher);
   }

   public void send()
   {
       Message m = new CoreMessage(new Integer(counter++));
       System.out.println("Sending "+m+" to the replicator: " + replicator.handle(m));
   }

}
