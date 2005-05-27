/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Acknowledgment;
import org.jboss.messaging.core.util.AcknowledgmentImpl;

import java.io.Serializable;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;
import java.util.Collections;

/**
 * A local Channel with only one output. Both the input and the output endpoints are in the same
 * address space.
 *
 * <p>
 * Only one receiver can be connected to this channel at a time. Synchronous delivery is attempted,
 * but if it is not possible, the LocalPipe will hold the message.
 *
 * @see org.jboss.messaging.core.Channel
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class LocalPipe extends SingleOutputChannelSupport
{
   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(LocalPipe.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Receiver receiver = null;
   protected Serializable id;

   /** speed optimization, avoid creating unnecessary instances **/
   protected Acknowledgment ACK, NACK;
   protected Set ACKSet, NACKSet;


   // Constructors --------------------------------------------------

   /**
    * The default message handling mode is synchronous.
    */
   public LocalPipe(Serializable id)
   {
      this(id, Channel.SYNCHRONOUS, null);
   }

   /**
    * The default message handling mode is synchronous.
    *
    * @param receiver - the pipe's output receiver.
    */
   public LocalPipe(Serializable id, Receiver receiver)
   {
      this(id, SYNCHRONOUS, receiver);
   }

   /**
    * @param mode - message handling mode. Use true for synchronous handling, false for
    *        asynchronous.
    */
   public LocalPipe(Serializable id, boolean mode)
   {
      this(id, mode, null);
   }

   /**
    * @param mode - message handling mode. Use true for synchronous handling, false for
    *        asynchronous.
    * @param receiver - the pipe's output receiver.
    */
   public LocalPipe(Serializable id, boolean mode, Receiver receiver)
   {
      super(mode);
      this.id = id;
      setReceiver(receiver);
   }

   // Channel implementation ----------------------------------------

   public Serializable getReceiverID()
   {
      return id;
   }

   public boolean deliver()
   {
      lock();

      if (log.isTraceEnabled()) { log.trace("asynchronous delivery triggered on " + getReceiverID()); }

      Set nackedMessages = new HashSet();

      try
      {
         for(Iterator i = localAcknowledgmentStore.getUnacknowledged(null).iterator(); i.hasNext();)
         {
            nackedMessages.add(i.next());
         }
      }
      finally
      {
         unlock();
      }

      // flush unacknowledged messages

      for(Iterator i = nackedMessages.iterator(); i.hasNext(); )
      {
         Serializable messageID = (Serializable)i.next();
         Routable r = (Routable)messages.get(messageID);

         if (r.isExpired())
         {
            removeLocalMessage(messageID);
            i.remove();
            continue;
         }

         if (log.isTraceEnabled()) { log.trace(this + " attempting to redeliver " + r); }

         Set acks = NACKSet;
         try
         {
            if (receiver.handle(r))
            {
               acks = ACKSet;
            }
         }
         catch(Throwable t)
         {
            // most likely the receiver is broken, don't insist
            log.warn("The receiver " + (receiver == null ? "null" : receiver.getReceiverID()) +
                     " failed to handle the message", t);
            break;
         }

         updateLocalAcknowledgments(r, acks);
      }

      return !hasMessages();
   }
   
   // ChannelSupport implementation ---------------------------------

   public boolean nonTransactionalHandle(Routable r)
   {
      Set acks = Collections.EMPTY_SET;
      if (receiver != null)
      {
         Serializable receiverID = receiver.getReceiverID();
         try
         {
            if (log.isTraceEnabled()) { log.trace(this + ": attempting to deliver to " + receiverID); }
            if (receiver.handle(r))
            {
               // successful synchronous delivery
               if (log.isTraceEnabled()) { log.trace(this + ": successful delivery to " + receiverID); }
               return true;
            }
            acks = NACKSet;
         }
         catch(Throwable e)
         {
            log.warn("The receiver " + receiverID + " failed to handle the message", e);
         }
      }
        
      if (log.isTraceEnabled()) { log.trace(this + ": unsuccessful delivery, storing the message"); }
      return updateAcknowledgments(r, acks);
   }

   // SingleOutputChannelSupport overrides --------------------------

   public Serializable getOutputID()
   {
      if (receiver == null)
      {
         return null;
      }
      return receiver.getReceiverID();
   }

   // Public --------------------------------------------------------

   /**
    * @return the receiver connected to the pipe or null if there is no Receiver.
    */
   public Receiver getReceiver()
   {
      lock();

      try
      {
         return receiver;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Connect a receiver to the pipe.
    */
   public void setReceiver(Receiver r)
   {
      lock();

      try
      {
         receiver = r;

         // speed optimization
         if (r != null)
         {
            ACK = new AcknowledgmentImpl(r.getReceiverID(), true);
            NACK = new AcknowledgmentImpl(r.getReceiverID(), false);
            ACKSet = Collections.singleton(ACK);
            NACKSet = Collections.singleton(NACK);
         }

         // adding a Receiver triggers an asynchronous delivery attempt
         // TODO Is this good?
         if (hasMessages())
         {
            deliver();
         }
      }
      finally
      {
         unlock();
      }
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("LocalPipe[");
      sb.append(id);
      sb.append("]");
      return sb.toString();
   }
}
