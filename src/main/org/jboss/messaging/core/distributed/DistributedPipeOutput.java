/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.interfaces.Receiver;
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.util.RpcServer;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;


/**
 * The "receiving end" of a distributed pipe.
 * <p>
 * "Listens" on a RpcDispatcher and synchronously/asynchronously handles messages sent by the input
 * end of the distributed pipe.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class DistributedPipeOutput
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DistributedPipeOutput.class);


   // Attributes ----------------------------------------------------

   protected RpcDispatcher dispatcher;
   protected Serializable pipeID;
   protected Receiver receiver;

   // Constructors --------------------------------------------------

   /**
    * @param dispatcher - the dispatcher to listen on.
    * @param pipeID - the id of the distributed pipe. It must match the id used to instantiate the
    *        input end of the pipe.
    * @exception IllegalStateException - thrown if the RpcDispatcher is not configured with an
    *            RpcServer, so this instance cannot register itself to field distributed calls.
    */
   public DistributedPipeOutput(RpcDispatcher dispatcher, Serializable pipeID, Receiver receiver)
   {
      this.dispatcher = dispatcher;
      this.pipeID = pipeID;
      this.receiver = receiver;
      init();
   }

   // Receiver implementation ---------------------------------------

   // Public --------------------------------------------------------

   /**
    * @return the receiver connected to the pipe or null if there is no Receiver.
    */
   public Receiver getReceiver()
   {
      return receiver;
   }

   /**
    * Connect a receiver to the output end of the distributed pipe.
    */
   public void setReceiver(Receiver r)
   {
       receiver = r;
   }

   public Serializable getPipeID()
   {
      return pipeID;
   }

   /**
    * The metohd to be called remotely by the input end of the distributed pipe.
    *
    * @return the acknowledgement as returned by the associated receiver.
    */
   public boolean handle(Message m)
   {
      try
      {
         return receiver.handle(m);
      }
      catch(Exception e)
      {
         log.error("The receiver connected to "+this+" is unable to handle the message: "+e);
         return false;
      }
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("DistributedPipeOutput[");
      sb.append(pipeID);
      sb.append("]");
      return sb.toString();
   }

   // Private -------------------------------------------------------

   private void init()
   {
      Object serverObject = dispatcher.getServerObject();
      if (!(serverObject instanceof RpcServer))
      {
         throw new IllegalStateException("The RpcDispatcher does not have an RpcServer installed");
      }
      RpcServer rpcServer = (RpcServer)serverObject;
      rpcServer.register(pipeID, this);
   }

   // TODO: unregister myself from the rpcServer when I am decomissioned.
}


