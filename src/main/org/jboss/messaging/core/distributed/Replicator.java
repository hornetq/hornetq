/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.interfaces.Channel;
import org.jboss.messaging.interfaces.Message;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;


/**
 * A distributed channel that replicates a message to multiple receivers living <i>in different
 * address spaces</i> synchronously or asynchronously. A replicator can have multiple inputs and
 * multiple outputs. Messages sent by each input are replicated to every output.
 * <p>
 * When it is configured to be synchronous, the Replicator works pretty much like a distributed
 * PointToMultipointRouter.
 * <p>
 * The Replicator's main reason to exist is to allow to sender to synchronously send a message
 * to different address spaces and be sure that the message was received (and it is acknowledged)
 * when the handle() method returns, all this in an efficient way.
 *
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Replicator implements Channel
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Replicator.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected boolean synchronous = true;
   protected Serializable replicatorID;

   protected RpcDispatcher dispatcher;
   protected ReplicatorInput replicatorInput;
   protected AcknowledgmentCollector ackCollector;

   // Constructors --------------------------------------------------

   /**
    * @param mode - Channel.SYNCHRONOUS or Channel.ASYNCHRONOUS.
    * @param replicatorID
    * @param dispatcher - the RpcDispatcher to delegate the transport to
    */
   public Replicator(boolean mode, RpcDispatcher dispatcher, Serializable replicatorID)
   {
      this.synchronous = mode;
      this.replicatorID = replicatorID;
      this.dispatcher = dispatcher;
      replicatorInput = new ReplicatorInput(dispatcher.getChannel(), replicatorID);
      ackCollector = new AcknowledgmentCollector(dispatcher, replicatorID);
   }

   // Channel implementation ----------------------------------------

   public boolean handle(Message m)
   {
      // Check if the message was sent remotely; in this case, I must not resend it to avoid
      // endless loops among peers

      if (m.getHeader(Message.REMOTE_MESSAGE) != null)
      {
         // don't send but acknowledge
         return true;
      }

      try
      {
         replicatorInput.sendAsychronously(m);
      }
      catch(Exception e)
      {
         log.warn("Failed to send the message", e);
         return false;
      }

      // submit the message to the ackCollector and wait for acknowledgment

      return true;
   }

   public boolean deliver()
   {
      return false;
   }

   public boolean hasMessages()
   {
      return false;
   }

   public boolean setSynchronous(boolean b)
   {
      return false;
   }

   public boolean isSynchronous()
   {
      return false;
   }

   // Public --------------------------------------------------------

   public Serializable getID()
   {
      return replicatorID;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
