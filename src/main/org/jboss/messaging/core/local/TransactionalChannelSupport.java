/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.TransactionalChannel;
import org.jboss.messaging.core.Routable;
import org.jboss.logging.Logger;

import javax.transaction.TransactionManager;
import javax.transaction.Transaction;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.RollbackException;
import java.util.HashMap;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class TransactionalChannelSupport extends ChannelSupport
      implements TransactionalChannel
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TransactionalChannelSupport.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private TransactionManager transactionManager;

   // Constructors --------------------------------------------------

   public TransactionalChannelSupport()
   {
      this(Channel.SYNCHRONOUS);
   }

   /**
    *
    * @param mode - Channel.SYNCHRONOUS/Channel.ASYNCHRONOUS
    */
   public TransactionalChannelSupport(boolean mode)
   {
      super(mode);
      messages = new HashMap();

   }

   // Channel implementation ----------------------------------------

   public boolean handle(Routable r)
   {
      if (isTransactional())
      {
         return transactionalHandle(r);
      }
      else
      {
         return nonTransactionalHandle(r);
      }
   }

   // TransactionalChannel implementation ---------------------------

   public void setTransactionManager(TransactionManager tm)
   {
      transactionManager = tm;
      log.debug(this + " transaction manager: " + transactionManager);
   }

   public TransactionManager getTransactionManager()
   {
      return transactionManager;
   }

   public boolean isTransactional()
   {
      return transactionManager != null;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected boolean transactionalHandle(Routable r)
   {

      if (log.isTraceEnabled()) { log.trace("handling " + r + " transactionally"); }

      Transaction t = null;

      try
      {
         if (transactionManager == null || (t = transactionManager.getTransaction()) == null)
         {
            return false;
         }

         if (Status.STATUS_MARKED_ROLLBACK == t.getStatus())
         {
            // don't bother with it, it will be rolled back anyway
            // TODO
            return false;
         }

         // TODO
         // register a callback with the transaction only if I am sure the channel will ACK the
         // message when handle() will be called upon it

         t.registerSynchronization(new RoutableSynchronization(r));

      }
      catch(RollbackException e)
      {
         // I shouldn't get this because I've just check whether transaction is marked for rollback
         log.error("Unexpected rollback condition", e);
         return false;
      }
      catch(Exception e)
      {
         log.error(e);
         return false;
      }

      return true;

   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

   private class RoutableSynchronization implements Synchronization
   {
      private Routable routable;

      public RoutableSynchronization(Routable r)
      {
         this.routable = r;
      }

      public void beforeCompletion()
      {
      }

      public void afterCompletion(int status)
      {
         if (status == Status.STATUS_COMMITTED)
         {
            nonTransactionalHandle(routable);
            // TODO what happens if the message its NACKed?
         }
         else if (status == Status.STATUS_ROLLEDBACK)
         {
            // remove routable from persistent storage
            // TODO what happens if the message its NACKed?
         }

      }
   }
}
