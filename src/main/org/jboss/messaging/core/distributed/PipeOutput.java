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

   private static final Logger log = Logger.getLogger(PipeOutput.class);


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

   // PipeOutputServerDelegate implementation --------------

   public Serializable getID()
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
         // Mark the message as being received from a remote endpoint
         m.putHeader(Message.REMOTE_MESSAGE, Message.REMOTE_MESSAGE);
         return receiver.handle(m);
      }
      catch(Exception e)
      {
         log.error("The receiver connected to " + this + " is unable to handle the message: " + e);
         return false;
      }
   }

   // Public --------------------------------------------------------

   public boolean register(RpcServer rpcServer, Serializable category)
   {
      return rpcServer.register(category, this);
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


