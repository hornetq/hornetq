/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tools;

import org.jboss.messaging.core.MessageSupport;
import org.jboss.messaging.core.distributed.Replicator;
import org.jboss.messaging.core.distributed.ReplicatorOutput;
import org.jboss.messaging.interfaces.Routable;
import org.jboss.messaging.util.NotYetImplementedException;

/**
 * Class that provides a command line interface to build distributed Queues. Run it with Clester.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReplicatorClient extends RpcDispatcherClient
{

   // Attributes ----------------------------------------------------

   private Replicator input;
   private ReplicatorOutput output;
   private int counter = 0;

   // Constructors --------------------------------------------------

   public ReplicatorClient() throws Exception
   {
      // no need for a RpcServer
      super(null);
      throw new NotYetImplementedException("Incomplete");
   }

   // Public --------------------------------------------------------

   public void setAsInput(String replicatorID) throws Exception
   {
      if (output != null)
      {
         throw new Exception("Replicator output for " + output.getID() +
                             "already instantiated, cannot use this client as replicatorPeer");
      }
      input = new Replicator(rpcDispatcher, replicatorID);
   }

   public void setAsOutput(String replicatorID) throws Exception
   {
      if (input != null)
      {
         throw new Exception("Replicator for " + input.getPeerID() +
                             "already instantiated, cannot use this client as replicatorPeer output");
      }
      output =
         new ReplicatorOutput(rpcDispatcher, replicatorID, new ReceiverImpl("Default Receiver"));
   }

   public void send()
   {
       Routable m = new MessageSupport(new Integer(counter++));
       System.out.println("Sending "+m+" to the replicatorPeer: " + input.handle(m));
   }

}
