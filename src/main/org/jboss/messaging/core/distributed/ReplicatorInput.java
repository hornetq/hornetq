/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.interfaces.Message;
import org.jboss.logging.Logger;

import java.io.Serializable;

/**
 * The input end of a replicator - a distributed channel that replicates the messages received
 * by its input to all its outputs.
 * <p>
 * The input end and the output ends have the same ID and they usually run in different address
 * spaces.
 * <p>
 * The replicator input is an inherently asynchronous construct.
 *
 * @see org.jboss.messaging.interfaces.Receiver
 * @see org.jboss.messaging.core.distributed.Replicator
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
class ReplicatorInput
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicatorInput.class);

   // Attributes ----------------------------------------------------

   protected org.jgroups.Channel jChannel;
   protected Serializable replicatorID;

   // Constructors --------------------------------------------------

   /**
    * ReplicatorInput constructor.
    *
    * @param jChannel - the JChannel instance to delegate the transport to. The JChannel instance
    *        doesn't necessarily have to be connected at the time the ReplicatorInput is created.
    * @param replicatorID - the identifier of the replicator.
    */
   public ReplicatorInput(org.jgroups.Channel jChannel, Serializable replicatorID)
   {
      this.jChannel = jChannel;
      this.replicatorID = replicatorID;
      log.debug(this + " created");
   }

   // Receiver implementation ----------------------------------------

   public void sendAsychronously(Message m) throws Exception
   {
      m.putHeader(Message.REPLICATOR_ID, replicatorID);
      jChannel.send(null, null, m);
   }

   // Public --------------------------------------------------------

   public Serializable getID()
   {
      return replicatorID;
   }

   public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("ReplicatorInput[");
      sb.append(replicatorID);
      sb.append("]");
      return sb.toString();
   }
}
