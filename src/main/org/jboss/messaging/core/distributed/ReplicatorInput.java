/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.interfaces.Receiver;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;

import java.io.Serializable;

/**
 * The input end of a replicator - a distributed channel that replicates the messages received
 * by its input to all its output ends.  A replicator can have multiple inputs and multiple outputs.
 * Messages sent by each input are replicated to every output.
 * <p>
 * The input end and the output ends have the same ID and they usually run in different address
 * spaces.
 * <p>
 * The asynchronous/synchronous behaviour of the replicator can be configured using
 * setSynchronous().
 * TODO: develop this more. Because of JGroups, it can behave as sycnhronous when in fact is asynch.
 *
 * @see org.jboss.messaging.interfaces.Receiver
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReplicatorInput implements Receiver
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicatorInput.class);

   // Attributes ----------------------------------------------------

   protected boolean synchronous;
   protected org.jgroups.Channel jChannel;
   protected Serializable replicatorID;

   // Constructors --------------------------------------------------

   /**
    * ReplicatorInput constructor.
    *
    * @param mode - true for synchronous behaviour, false otherwise.
    * @param jChannel - the JChannel instance to delegate the transport to. The JChannel instance
    *        doesn't necessarily have to be connected at the time the ReplicatorInput is created.
    * @param replicatorID - the identifier of the replicator.
    */
   public ReplicatorInput(boolean mode, org.jgroups.Channel jChannel, Serializable replicatorID)
   {
      synchronous = mode;
      this.jChannel = jChannel;
      this.replicatorID = replicatorID;
      log.debug(this + " created");
   }

   // Receiver implementation ----------------------------------------

   public boolean handle(Message m)
   {
      if (synchronous)
      {
         return synchronousHandle(m);
      }
      else
      {
         return asynchronousHandle(m);
      }
   }

   // Public --------------------------------------------------------

   public boolean setSynchronous(boolean b)
   {
      synchronous = b;
      // TODO
      return true;
   }

   public boolean isSynchronous()
   {
      return synchronous;
   }

   public Serializable getID()
   {
      return replicatorID;
   }

   public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("ReplicatorInput[");
      sb.append(synchronous?"SYNCH":"ASYNCH");
      sb.append("][");
      sb.append(replicatorID);
      sb.append("]");
      return sb.toString();
   }


   // Private -------------------------------------------------------

   private boolean synchronousHandle(Message m)
   {
      throw new NotYetImplementedException();
   }

   private boolean asynchronousHandle(Message m)
   {
      // Check if the message was sent remotely; in this case, I must not resend it to avoid
      // endless loops among peers or worse

      if (m.getHeader(Message.REMOTE_MESSAGE_HEADER) != null)
      {
         // don't send
         return false;
      }

      try
      {
         m.putHeader(Message.REPLICATOR_ID_HEADER, replicatorID);
         jChannel.send(null, null, m);
         return true;
      }
      catch(Exception e)
      {
         log.error("Sending message to the group " + jChannel.getChannelName() + " failed", e);
         return false;
      }

   }

   // DEBUG ---------------------------------------------------------

   public String dump()
   {
      StringBuffer sb = new StringBuffer();
      sb.append(toString());
      sb.append(", messages: ");
      //sb.append(((MessageSetImpl)messages).dump());
      return sb.toString();
   }
}
