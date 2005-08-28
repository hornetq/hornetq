/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import org.jboss.logging.Logger;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.messaging.core.message.StorageIdentifier;

import javax.transaction.TransactionManager;
import javax.transaction.Transaction;
import javax.transaction.SystemException;
import java.util.List;
import java.util.Iterator;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
class ReliableState extends TransactionalState
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReliableState.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private PersistenceManager pm;
   private Serializable channelID;

   private MessageStore ms;

   // Constructors --------------------------------------------------

   public ReliableState(Channel channel, PersistenceManager pm, TransactionManager tm)
   {
      super(channel, tm);
      if (pm == null)
      {
          throw new IllegalArgumentException("ReliableState requires a non-null persistence manager");
      }
      this.pm = pm;
      this.channelID = channel.getChannelID();
      ms = channel.getMessageStore();

   }

   // StateSupport overrides -------------------------------------

   public boolean isReliable()
   {
      return true;
   }

   public void add(Delivery d) throws Throwable
   {

      if (!d.getRoutable().isReliable())
      {
         super.add(d);
         return;
      }

      if (d instanceof CompositeDelivery)
      {
         // TODO break CompositeDelivery in components and add them individually
         throw new NotYetImplementedException("Don't know to handle composite deliveries");
      }

      pm.add(channelID, d);
   }


   public boolean remove(Delivery d) throws Throwable
   {
      if (!d.getRoutable().isReliable())
      {
         return super.remove(d);
      }
      return pm.remove(channelID, d);
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
            // TODO filtering could be probably pushed to the database
            if (filter == null || filter.accept(ref))
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


   public void add(Routable r) throws Throwable
   {
      if (!r.isReliable())
      {
         super.add(r);
         return;
      }

      // reliable message

      MessageReference ref = ms.reference(r);

      Transaction tx = tm.getTransaction();

      if (tx == null)
      {
         // no active transaction, handle the message non-transactionally
         pm.add(channelID, ref);
         return;
      }

      // add reliable message transactionally
      String txID = registerAddMessageSynchronization(tx);
      pm.add(channelID, txID, ref);
   }

   public boolean remove(Routable r)
   {
      if (!r.isReliable())
      {
         return super.remove(r);
      }

      try
      {
         // reliable message
         MessageReference ref = ms.reference(r);

         return pm.remove(channelID, ref);
      }
      catch(Throwable t)
      {
         log.error("Cannot remove message from persistence manager", t);
         return false;
      }
   }

   public List undelivered(Filter filter)
   {
      // unreliable messages first
      List undelivered = super.undelivered(filter);
      try
      {
         List persisted = pm.messages(channelID);
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
            MessageReference ref = ms.getReference(id.messageID);

            // TODO very inefficient. Get rid of this when a reference will be able to fully support headers/properties
            Message m = ref.getMessage();

            if (filter == null || filter.accept(m))
            {
               undelivered.add(ref);
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

   protected void enableTransactedMessages(String txID) throws SystemException
   {
      super.enableTransactedMessages(txID);
      pm.enableTransactedMessages(channelID, txID);
   }

   protected void dropTransactedMessages(String txID) throws SystemException
   {
      super.dropTransactedMessages(txID);
      pm.dropTransactedMessages(channelID, txID);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
