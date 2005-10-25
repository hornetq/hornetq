/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.message.StorageIdentifier;
import org.jboss.messaging.core.tx.Transaction;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class RecoverableState extends NonRecoverableState
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RecoverableState.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private PersistenceManager pm;
   private Serializable channelID;

   private MessageStore ms;

   // Constructors --------------------------------------------------

   public RecoverableState(Channel channel, PersistenceManager pm)
   {
      super(channel, true);
      if (pm == null)
      {
          throw new IllegalArgumentException("RecoverableState requires a non-null persistence manager");
      }
      this.pm = pm;
      this.channelID = channel.getChannelID();
      ms = channel.getMessageStore();

   }

   // NonRecoverableState overrides -------------------------------------

   public boolean isRecoverable()
   {
      return true;
   }

   public void add(MessageReference ref, Transaction tx) throws Throwable
   {
      if (!ref.isReliable())
      {
         //Unreliable message in a recoverable state - handle as unreliable
         super.add(ref, tx);
         return;
      }
      //Reliable message in a recoverable state - just add to db
      pm.add(channelID, ref, tx);

      if (tx != null)
      {
         addAddReferenceTask(tx);
      }
   }

   public boolean remove(MessageReference ref) throws Throwable
   {
      if (ref.isReliable())
      {
         //Reliable message in recoverable state - remove from db
         return (pm.remove(channelID, ref));
      }
      else
      {
         //Unreliable message in recoverable state - handle as unreliable
         return super.remove(ref);
      }
   }

   public void add(Delivery d) throws Throwable
   {
      // Note! Adding of deliveries to the state is NEVER done in a transactional context.
      // The only things that are done in a transactional context are sending of messages and
      // removing deliveries (acking).
            
      if (d.getReference().isReliable())
      {
         //Add delivery to persistent storage - reliable delivery in recoverable state
         pm.add(channelID, d);
      }
      else
      {
         //Unreliable delivery in recoverable state - handle as unreliable
         super.add(d);
      }
   }

   public boolean remove(Delivery d, Transaction tx) throws Throwable
   {
      if (d.getReference().isReliable())
      {
         //Reliable message in recoverable state - removed from db
         return pm.remove(channelID, d, tx);
      }
      else
      {
         //Unreliable message in recoverable state - handle as unreliable
         return super.remove(d, tx);
      }
   }

   public List delivering(Filter filter)
   {
      List delivering = super.delivering(filter);
      
      try
      {
         List persisted = pm.deliveries(channelID);
         for(Iterator i = persisted.iterator(); i.hasNext(); )
         {
            StorageIdentifier id = (StorageIdentifier)i.next();
            if (!id.storeID.equals(ms.getStoreID()))
            {
               // TODO maybe the channel could have access to multiple stores
               throw new IllegalStateException("My current message store (id=" + ms.getStoreID() +
                                               ") cannot reference messages being maintained by " +
                                               "the store with id=" + id.storeID);
            }

            MessageReference ref = ms.getReference(id.messageID);

            // TODO: I need to dereference the message each time I apply the filter. Refactor so the message reference will also contain JMS properties
            if (filter == null || filter.accept(ref.getMessage()))
            {
               delivering.add(ref);
            }
         }
      }
      catch(Throwable t)
      {
         log.error("Cannot get the delivery list from persistence manager", t);
         return null;
      }
      
      return delivering;
   }

   public List undelivered(Filter filter)
   {
      // unreliable messages first
      List undelivered = super.undelivered(filter);

      try
      {
         if (log.isTraceEnabled()) { log.trace("Getting undelivered reliable messages for channel: " + channelID); }

         List persisted = pm.messages(channelID);

         if (log.isTraceEnabled()) { log.trace("Retrieved " + persisted.size() + " messages from persistent storage"); }

         for(Iterator i = persisted.iterator(); i.hasNext(); )
         {
            StorageIdentifier id = (StorageIdentifier)i.next();
            if (!id.storeID.equals(ms.getStoreID()))
            {
               // TODO maybe the channel could have access to multiple stores
               throw new IllegalStateException("My current message store (id=" + ms.getStoreID() +
                                               ") cannot reference messages being maintained by " +
                                               "the store with id=" + id.storeID);
            }

            // TODO filtering could be probably pushed to the database
            if (log.isTraceEnabled()) { log.trace("Looking for reference for message id " + id.messageID); }
            MessageReference ref = ms.getReference(id.messageID);
            
            if (ref == null)
            {
               log.error("Could not find reference for message:" + id.messageID);
               return persisted;
            }

            // TODO: I need to dereference the message each time I apply the filter. Refactor so the message reference will also contain JMS properties
            if (filter == null || filter.accept(ref.getMessage()))
            {
               if (log.isTraceEnabled()) { log.trace("Message accepted by filter so adding to list"); }
               undelivered.add(ref);
            }
            else
            {
               if (log.isTraceEnabled()) { log.trace("Message NOT accepted by filter so not adding to list"); }
            }
         }
      }
      catch(Throwable t)
      {
         log.error("Cannot get message list from persistence manager", t);
         return null;
      }
      return undelivered;
   }

   public void clear()
   {
      super.clear();
      pm = null;
      // the persisted state remains
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
