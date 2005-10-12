/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.distributed.pipe;

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Receiver;
import org.jboss.logging.Logger;

import java.io.Serializable;

/**
 * The "receiving end" of a distributed pipe.
 * <p>
 * Listens on a RpcDispatcher and synchronously handles messages sent by the input end of the
 * distributed pipe. Multiple distributed pipes can share the same DistributedPipeOutput instance
 * (and implicitly the pipeID), as long input instances are different.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DistributedPipeOutput implements PipeOutputFacade
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DistributedPipeOutput.class);


   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable pipeID;
   protected Receiver receiver;

   // Constructors --------------------------------------------------

   /**
    * @param pipeID - the id of the distributed pipe. It must match the id used to instantiate the
    *        input end of the pipe.
    */
   public DistributedPipeOutput(Serializable pipeID, Receiver receiver)
   {
      this.pipeID = pipeID;
      this.receiver = receiver;
   }

   // PipeOutputFacade implementation -------------------------------

   public Serializable getID()
   {
      return pipeID;
   }

   public Delivery handle(Routable r)
   {
      // Mark the message as being received from a remote endpoint
      r.putHeader(Routable.REMOTE_ROUTABLE, Routable.REMOTE_ROUTABLE);

      Delivery d = receiver.handle(null, r, null);

      // we only accept synchronous deliveries at this point
      if (d != null && !d.isDone())
      {
         throw new IllegalStateException("Cannot handle asynchronous deliveries at this end");
      }

      return d;
   }

   // Public --------------------------------------------------------

   public Receiver getReceiver()
   {
      return receiver;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("Output[");
      sb.append(pipeID);
      sb.append("]");
      return sb.toString();
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
