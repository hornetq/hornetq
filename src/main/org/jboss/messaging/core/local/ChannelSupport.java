/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.messaging.core.util.Lockable;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.AcknowledgmentStore;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Message;
import org.jboss.logging.Logger;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;


/**
 * Basic functionality that must be made available by all Channel implementation.
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

   private volatile boolean synchronous;

   protected AcknowledgmentStore localAcknowledgmentStore;
   // <messageID - Routable>
   protected Map messages;

   private MessageStore messageStore;
   private AcknowledgmentStore externalAcknowledgmentStore;


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
      messages = new HashMap();

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

   public boolean hasMessages()
   {
      lock();

      try
      {
         return !localAcknowledgmentStore.getUnacknowledged(null).isEmpty();
      }
      finally
      {
         unlock();
      }
   }

   public Set getUndelivered()
   {
      lock();

      try
      {
         return localAcknowledgmentStore.getUnacknowledged(null);
      }
      finally
      {
         unlock();
      }
   }

   public void acknowledge(Serializable messageID, Serializable receiverID)
   {
      try
      {
         localAcknowledgmentStore.acknowledge(null, messageID, receiverID);
         if (externalAcknowledgmentStore != null)
         {
            externalAcknowledgmentStore.acknowledge(getReceiverID(), messageID, receiverID);
         }
      }
      catch(Throwable t)
      {
         log.error("Channel " + getReceiverID() + " failed to handle positive acknowledgment " +
                   " from receiver " + receiverID + " for message " + messageID, t);
         return;
      }
      if (!localAcknowledgmentStore.hasNACK(null, messageID))
      {
         // cleanup the local store, I only keep it if it is NACKed
         messages.remove(messageID);
      }
      // TODO Who cleans the external store?
   }

   /**
    * This is the default behaviour of an asynchronous channel: if there are no receivers, try to
    * store the message and positively acknowledge if storage is successful. Override for topic-like
    * behavior.
    */
   public boolean isStoringUndeliverableMessages()
   {
      return true;
   }

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
      externalAcknowledgmentStore = store;
   }

   public AcknowledgmentStore getAcknowledgmentStore()
   {
      return externalAcknowledgmentStore;
   }

   /**
    * Must acquire the channel's reentrant lock.
    */
   public abstract boolean deliver();

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   /**
    * Helper method that updates acknowledgments and stores the message if necessary.
    *
    * @param acks - Set of Acknowledgments. null means Channel NACK.
    *
    * @return true if the Channel assumes responsibility for delivery, or false if the channel
    *         NACKs the message.
    */
   protected boolean updateAcknowledgments(Routable r, Set acks)
   {
      lock();

      if (log.isTraceEnabled()) { log.trace(this + " updating acknowledgments for " + r.getMessageID()); }

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
               externalAcknowledgmentStore.update(r.getMessageID(), getReceiverID(), acks);
            }

            // always update local NACKs TODO optimization? use external acknowledgment store?
            updateLocalAcknowledgments(r, acks);

            return true;
         }
         catch(Throwable t)
         {
            log.warn(this + ": cannot keep NACKed message " + r, t);
            return false;
         }
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Subclasses could override this method to refresh their local acknowledgment maps.
    *
    * Always called from a synchronized block, no need to synchronize. It can throw unchecked
    * exceptions, the caller is prepared to deal with them.
    *
    * @param acks - Set of Acknowledgments. Empty set (or null) means Channel NACK.
    */
   protected void updateLocalAcknowledgments(Routable r, Set acks)
   {
      if(log.isTraceEnabled()) { log.trace("updating acknowledgments " + r + " locally"); }

      // the channel's lock is already acquired when invoking this method

      Serializable messageID = r.getMessageID();
      try
      {
         localAcknowledgmentStore.update(null, messageID, acks);
         if (localAcknowledgmentStore.hasNACK(getReceiverID(), messageID))
         {
            if (!messages.containsKey(messageID))
            {
               messages.put(messageID, r);
            }
         }
         else
         {
            messages.remove(messageID);
         }
      }
      catch(Throwable t)
      {
         log.error("Cannot update acknowledgments locally", t);
      }
   }



   /**
    * Subclasses  could override this to get rid of a message from local storage (possibly due to
    * expiration).
    *
    * @param messageID
    */
   protected void removeLocalMessage(Serializable messageID)
   {
      if(log.isTraceEnabled()) { log.trace("removing message " + messageID + " locally"); }

      // the channel's lock is already acquired when invoking this method

      try
      {
         localAcknowledgmentStore.remove(getReceiverID(), messageID);
         messages.remove(messageID);
      }
      catch(Throwable t)
      {
         log.error("Cannot remove message locally", t);
      }
   }


   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
