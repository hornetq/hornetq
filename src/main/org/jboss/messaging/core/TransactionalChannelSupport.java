/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import org.jboss.logging.Logger;

import javax.transaction.TransactionManager;
import javax.transaction.SystemException;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class TransactionalChannelSupport extends ChannelSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TransactionalChannelSupport.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   protected TransactionalChannelSupport(Serializable channelID,
                                         MessageStore ms,
                                         PersistenceManager pm,
                                         TransactionManager tm)
   {
      super(channelID, ms, pm, tm);
   }

   // Channel overrides ---------------------------------------------

   public boolean isTransactional()
   {
      return tm != null;
   }

   public final Delivery handle(DeliveryObserver sender, Routable r)
   {
      try
      {
         MessageReference ref = ref(r);
         
         if (tm == null || tm.getTransaction() == null)
         {
            // handle the message non-transactionally
            return super.handle(sender, ref);
         }

         try
         {
            // handle transactionally
            state.add(ref);

            // I might as well return null, the sender shouldn't care
            return new SimpleDelivery(sender, ref, true);

         }
         catch(Throwable t)
         {
            log.error("Failure to process the message transactionally", t);
            tm.setRollbackOnly();
            return null;
         }
      }
      catch(SystemException e)
      {
         log.error("Transaction system failure", e);
         return null;
      }
   }

   // DeliveryObserver overrides ------------------------------------
   
   public void acknowledge(Delivery d)
   {
      try
      {
         if (tm == null || tm.getTransaction() == null)
         {
            // acknowledge non transactionally
            super.acknowledge(d);
            return;
         }

         if (log.isTraceEnabled()){ log.trace("acknowledging transactionally " + d); }

         try
         {
            // handle transactionally
            state.remove(d);
         }
         catch(Throwable t)
         {
            log.error("Failure to process the message transactionally", t);
            tm.setRollbackOnly();
         }
      }
      catch(SystemException e)
      {
         log.error("Transaction system failure", e);
      }
   }

   // Public --------------------------------------------------------
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
