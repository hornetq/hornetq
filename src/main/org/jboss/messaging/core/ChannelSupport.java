/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.interfaces.MessageStore;
import org.jboss.messaging.interfaces.AcknowledgmentStore;
import org.jboss.messaging.interfaces.Channel;
import org.jboss.messaging.interfaces.Routable;
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.util.Lockable;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.util.Set;

/**
 * Basic functionality that must be available in to all Channel implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class ChannelSupport extends Lockable implements Channel
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ChannelSupport.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   /** true if the channel is synchronous */
   private volatile boolean synchronous;
   private MessageStore messageStore;
   private AcknowledgmentStore acknowledgmentStore;

   // Constructors --------------------------------------------------

   /**
    * The default behavior is SYNCHRONOUS
    */
   public ChannelSupport()
   {
      this(Channel.SYNCHRONOUS);
   }

   /**
    *
    * @param mode - Channel.SYNCHRONOUS/Channel.ASYNCHRONOUS
    */
   public ChannelSupport(boolean mode)
   {
      synchronous = mode;
   }

   // Channel implementation ----------------------------------------

   public boolean setSynchronous(boolean synch)
   {
      lock();

      try
      {
         if (synchronous == synch)
         {
            return true;
         }
         if (synch && hasMessages())
         {
            log.warn("Cannot cannot configure a non-empty channel to be synchronous");
            return false;
         }
         synchronous = synch;
         return true;
      }
      finally
      {
         unlock();
      }
   }

   public boolean isSynchronous()
   {
      return synchronous;
   }

   /**
    * Must acquire the channel's reentrant lock.
    */
   public abstract boolean deliver();

   /**
    * Must acquire the channel's reentrant lock.
    */
   public abstract boolean hasMessages();

   /**
    * Must acquire the channel's reentrant lock.
    */
   public abstract Set getUnacknowledged();


   public void setMessageStore(MessageStore store)
   {
      messageStore = store;
   }

   public MessageStore getMessageStore()
   {
      return messageStore;
   }

   public void setAcknowledgmentStore(AcknowledgmentStore store)
   {
      acknowledgmentStore = store;
   }

   public AcknowledgmentStore getAcknowledgmentStore()
   {
      return acknowledgmentStore;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   /**
    * Method to be implemented by the subclasses to keep the NACKed messages locally in memory.
    * Always called from a synchronized block, so no need to synchronize. It can throw unchecked
    * exceptions, the caller is prepared to deal with them.
    *
    * @param receiverID could be null if the Channel doesn't currently have receivers.
    */
   protected abstract void storeNACKedMessageLocally(Routable r, Serializable receiverID);

   /**
    * This method is called if the channel must keep the message, because the message was NACKed by
    * the receiver. Mustn't throw unchecked exceptions.
    *
    * @param receiverID could be null if the Channel doesn't currently have receivers.
    *
    * @return true if the Channel assumes responsibility for delivery, or false if the channel
    *         NACKs the message too.
    */
   protected boolean storeNACKedMessage(Routable r, Serializable receiverID)
   {
      lock();

      if (log.isTraceEnabled()) { log.trace("store NACK: "+r.getMessageID()+" from "+receiverID); }

      try
      {
         if (synchronous)
         {
            // synchronous channels don't keep messages
            return false;
         }
         try
         {
            if (r.isReliable())
            {
               if (r instanceof Message)
               {
                  // TODO if this succeeds and acknowledgmentStore fails, I add garbage to the message store
                  r = messageStore.store((Message)r);
               }
               acknowledgmentStore.storeNACK(r.getMessageID(), receiverID);
            }
            storeNACKedMessageLocally(r, receiverID);
            return true;
         }
         catch(Throwable t)
         {
            log.warn("Cannot keep NACKed message " + r, t);
            return false;
         }
      }
      finally
      {
         unlock();
      }
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
