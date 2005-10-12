/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.tx.Transaction;

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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
                                         PersistenceManager pm)
   {
      super(channelID, ms, pm);
   }

   // Channel overrides ---------------------------------------------


   public final Delivery handle(DeliveryObserver sender, Routable r, Transaction tx)
   {
      if (r == null)
      {
         return null;
      }
      
      MessageReference ref = ref(r);
      
      if (tx == null)
      {
         // handle the message non-transactionally
         return super.handle(sender, ref, tx);
      }

      // handle transactionally
      try
      {
         state.add(ref, tx);         
      }
      catch (Throwable t)
      {
         log.error("Failed to add to state", t);
         return null;
      }

      // I might as well return null, the sender shouldn't care
      return new SimpleDelivery(sender, ref, true);              
   }

   // DeliveryObserver overrides ------------------------------------
   
   public void acknowledge(Delivery d, Transaction tx)
   {
      if (tx == null)
      {
         // acknowledge non transactionally
         super.acknowledge(d, null);
         return;
      }

      if (log.isTraceEnabled()){ log.trace("acknowledging transactionally " + d); }


      // handle transactionally
      try
      {
         state.remove(d, tx);         
      }
      catch (Throwable t)
      {
         log.error("Failed to remove from state", t);
      }
   }

   // Public --------------------------------------------------------
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
