/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.message;

import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Message;
import org.jboss.logging.Logger;

import javax.transaction.TransactionManager;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class PersistentMessageStore extends TransactionalMessageStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PersistentMessageStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private PersistenceManager pm;

   // Constructors --------------------------------------------------

   public PersistentMessageStore(Serializable storeID, PersistenceManager pm, TransactionManager tm)
   {
      super(storeID, tm);
      this.pm = pm;
   }

   // MessageStore overrides ----------------------------------------

   public boolean isReliable()
   {
      return true;
   }

   public MessageReference reference(Routable r) throws Throwable
   {

      if (r.isReference() || !r.isReliable())
      {
         return super.reference(r);
      }

      // is reliable

      if (log.isTraceEnabled()) { log.trace("Persisting message " + r); }

      pm.store((Message)r);
      return new SoftMessageReference((Message)r, this);
   }

   public MessageReference getReference(Serializable messageID)
   {
      MessageReference ref = super.getReference(messageID);

      if (ref != null)
      {
         return ref;
      }

      Message m = null;
      try
      {
         m = pm.retrieve(messageID);
      }
      catch(Throwable t)
      {
         log.error("Persistence manager failed", t);
      }

      if (m == null)
      {
         return null;
      }
      return new SoftMessageReference((Message)m, this);
   }




   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   /**
    * Only called by MessageReference implementations whose Message soft reference have expired.
    * Has as a side efect the refreshing of the soft reference.
    */
   protected Message retrieve(Routable r)
   {

      if (!r.isReference())
      {
         return (Message)r;
      }

      if (!r.isReliable())
      {
         return super.retrieve(r);
      }

      SoftMessageReference ref = (SoftMessageReference)r;
      if (!ref.getStoreID().equals(getStoreID()))
      {
         throw new IllegalStateException("This reference is maintained by another store (" +
                                         ref.getStoreID() + ")");
      }

      try
      {

         Message m = pm.retrieve(r.getMessageID());

         if (log.isTraceEnabled()) { log.trace("dereferenced message: " + m); }

         // refresh the soft reference
         ref.refreshReference(m);

         return m;
      }
      catch(Throwable t)
      {
         log.error("Failed to retrieve message with id=" + r.getMessageID(), t);
         return null;
      }
   }


   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
