/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Acknowledgment;
import org.jboss.messaging.core.util.RpcServer;
import org.jboss.messaging.core.util.AcknowledgmentImpl;

import java.io.Serializable;


/**
 * The "receiving end" of a distributed pipe.
 * <p>
 * Listens on a RpcDispatcher and synchronously/asynchronously handles messages sent by the input
 * end of the distributed pipe.
 * <p>
 * Multiple distributed pipes can share the same PipeOutput instance (and implicitly the
 * pipeID), as long the DistributedPipeIntput instances are different.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class PipeOutput implements PipeOutputServerDelegate
{
   // Constants -----------------------------------------------------

//   private static final Logger log = Logger.getLogger(PipeOutput.class);


   // Attributes ----------------------------------------------------

   protected Serializable pipeID;
   protected Receiver receiver;

   // Constructors --------------------------------------------------

   /**
    * @param pipeID - the id of the distributed pipe. It must match the id used to instantiate the
    *        input end of the pipe.
    */
   public PipeOutput(Serializable pipeID, Receiver receiver)
   {
      this.pipeID = pipeID;
      this.receiver = receiver;
   }

   // PipeOutputServerDelegate implementation -----------------------

   public Serializable getID()
   {
      return pipeID;
   }

   public Acknowledgment handle(Routable m)
   {
      // Mark the message as being received from a remote endpoint
      m.putHeader(Routable.REMOTE_ROUTABLE, Routable.REMOTE_ROUTABLE);
      boolean result = receiver.handle(m);
      return new AcknowledgmentImpl(receiver.getReceiverID(), result);
   }

   public Serializable getOutputID()
   {
      if (receiver == null)
      {
         return null;
      }
      return receiver.getReceiverID();      
   }


   // Public --------------------------------------------------------

   public boolean register(RpcServer rpcServer)
   {
      return rpcServer.register(pipeID, this);
   }

   // TODO unregister myself from the rpcServer when I am decomissioned.

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

   public String toString()
   {
      StringBuffer sb = new StringBuffer("PipeOutput[");
      sb.append(pipeID);
      sb.append("]");
      return sb.toString();
   }

   // Private -------------------------------------------------------

   // Static --------------------------------------------------------

   /** access it only from getUniqueID() */
   private static int sequence = 0;

   /**
    * Returns runtime PipeOutput IDs that are unique per classloading domain.
    * @return an unique Integer.
    */
   synchronized static Integer getUniqueID()
   {
      return new Integer(sequence++);
   }

}


