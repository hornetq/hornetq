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
import org.jboss.messaging.core.util.StateImpl;
import org.jboss.logging.Logger;

import javax.transaction.TransactionManager;
import javax.transaction.Transaction;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Status;
import javax.transaction.RollbackException;
import java.util.HashMap;
import java.util.Map;
import java.io.Serializable;


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

   private volatile TransactionManager transactionManager;

   // This map <Transaction - ChannelSynchronization> prevents us from registering twice the
   // same synchronization on a transaction.
   private Map synchronizations;

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
      synchronizations = new HashMap();

   }

   // Channel implementation ----------------------------------------

   /**
    * If there is no active transaction, the semantics is similar to Receiver's handle().<p>
    *
    * If there is an active transaction, the channel will either deliver the message on
    * transaction's commit, or, if there is a problem with the message delivery, the whole
    * transaction will be rolled back.
    * In case of transactional handling, the method's return value is irrelevant (most likely it
    * will return true).
    *
    * @exception IllegalStateException - thrown on failure to interact with the transaction manager,
    *            if a transaction manager is available.
    *
    */
   public final boolean handle(Routable r)
   {
      if (transactionManager == null)
      {
         // handle the message non-transactionally
         return handleNoTx(r);
      }

      try
      {
         Transaction transaction = transactionManager.getTransaction();

         if (transaction == null)
         {
            // no active transaction, handle the message non-transactionally
            return handleNoTx(r);
         }

         handleWithTx(r, transaction);
      }
      catch(SystemException e)
      {
         String msg = "Failed to access current transaction";
         log.error(msg, e);
         throw new IllegalStateException(msg);
      }

      return true;
   }

   /**
    * If there is no active transaction, the semantics is similar to Channel's acknowledge().<p>
    *
    * If there is an active transaction, the channel will either enable the acknowledgment on
    * transaction's commit, or, if there is a problem with the acknowledgment delivery, the whole
    * transaction will be rolled back.
    *
    * @exception IllegalStateException - thrown on failure to interact with the transaction manager,
    *            if a transaction manager is available.
    *
    */
   public void acknowledge(Serializable messageID, Serializable receiverID)
   {
      if (transactionManager == null)
      {
         // handle the message non-transactionally
         acknowledge(messageID, receiverID, null);
         return;
      }

      try
      {
         Transaction transaction = transactionManager.getTransaction();

         if (transaction == null)
         {
            // no active transaction, handle the acknowledgment non-transactionally
            acknowledge(messageID, receiverID, null);
            return;
         }

         acknowledgeWithTx(messageID, receiverID, transaction);
      }
      catch(SystemException e)
      {
         String msg = "Failed to access current transaction";
         log.error(msg, e);
         throw new IllegalStateException(msg);
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

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   /**
    * Same semantics as Receiver.handle().
    *
    * @see org.jboss.messaging.core.Receiver#handle(org.jboss.messaging.core.Routable)
    */
   protected abstract boolean handleNoTx(Routable r);


   /**
    * Called on txID's commit. Enable delivery on routables that have been waiting for transaction
    * to commit.
    *
    * @param txID - a channel-specific transaction ID
    */
   protected void commit(String txID)
   {
      // TODO: this doesn't take care of the externalAcknowledgmentStore, if exists, so the NonCommitted will stay there
      localAcknowledgmentStore.commit(null, txID);
   }

   /**
    * Called on txID's rollback. Discards the routables that have been waiting for transaction
    * to commit.
    *
    * @param txID - a channel-specific transaction ID
    */
   protected void rollback(String txID)
   {
      // TODO: this doesn't take care of the externalAcknowledgmentStore, if exists, so the NonCommitted will stay there
      localAcknowledgmentStore.rollback(null, txID);
   }

   // Private -------------------------------------------------------

   /**
    * Loads the message to be delivered by the channel when transaction commits.
    *
    * @param r - the Routable to deliver.
    * @param transaction - a non-null Transaction.
    *
    * @exception SystemException - failed to interact with the transaction
    */
   private void handleWithTx(Routable r, Transaction transaction) throws SystemException
   {
      if (log.isTraceEnabled()) { log.trace("handling " + r + " transactionally"); }

      String txID = registerSynchronization(transaction);
      if (txID == null)
      {
         return;
      }

      // add the message to channel's internal storage, but don't deliver yet
      if (!updateAcknowledgments(r, new StateImpl(txID)))
      {
         // not accepted, rollback
         transaction.setRollbackOnly();
      }
   }

   /**
    * Accumulate acknowledgments until transaction commits.
    *
    * @exception SystemException - failed to interact with the transaction
    */
   private void acknowledgeWithTx(Serializable messageID,
                                  Serializable receiverID,
                                  Transaction transaction) throws SystemException
   {
      if (log.isTraceEnabled()) { log.trace("handling acknowledgment for " + messageID + " transactionally"); }

      String txID = registerSynchronization(transaction);
      if (txID == null)
      {
         return;
      }
      // add the acknowledgments to channel's internal storage, but don't enable them yet
      acknowledge(messageID, receiverID, txID);
   }

   /**
    * @return the txID or null if the transaction is already marked for rollback.
    * @throws SystemException
    */
   private String registerSynchronization(Transaction transaction) throws SystemException
   {
      String txID;
      synchronized(synchronizations)
      {
         // TODO if the same logical transaction is represented by more than one Transaction instances, this won't work correctly
         ChannelSynchronization sync = (ChannelSynchronization)synchronizations.get(transaction);
         if (sync == null)
         {
            txID = generateUniqueTransactionID();
            sync = new ChannelSynchronization(txID);
            try
            {
               transaction.registerSynchronization(sync);
            }
            catch(RollbackException e)
            {
               log.debug("Transaction already marked for rollback", e);
               return null;
            }
            synchronizations.put(transaction, sync);
         }
         txID = sync.getTxID();
      }
      return txID;
   }

   // to be accessed only from generateUniqueTransactionID();
   private int txID = 0;

   /**
    * TODO replace it with a more solid (and fast) algorithm
    *
    * @return a unique ID that will be the identity of a Transaction relative to this channel.
    */
   private synchronized String generateUniqueTransactionID()
   {
      StringBuffer sb = new StringBuffer("TX:");
      sb.append(getReceiverID());
      sb.append(":");
      sb.append(txID++);
      sb.append(":");
      sb.append(System.currentTimeMillis());
      return sb.toString();
   }

   // Inner classes -------------------------------------------------

   private class ChannelSynchronization implements Synchronization
   {
      // the channel-specific ID that identifies the transaction this Synchronization is attached to
      private String txID;

      ChannelSynchronization(String txID)
      {
         this.txID = txID;
      }

      // Synchronization implementation --------------------------------

      public void beforeCompletion()
      {
         try
         {
            // this call is executed with the transaction context of the transaction
            // that is being committed
            Transaction crtTransaction = transactionManager.getTransaction();

            if (crtTransaction.getStatus() == Status.STATUS_MARKED_ROLLBACK)
            {
               return;
            }

            // enable delivery
            commit(txID);
            deliver();

         }
         catch(SystemException e)
         {
            // not much to do, just log the exception and discard the changes
            log.error("Failed to access current transaction", e);
            rollback(txID);
         }
      }

      public void afterCompletion(int status)
      {
         if (status == Status.STATUS_ROLLEDBACK)
         {
            rollback(txID);
         }
      }

      // Public implementation -----------------------------------------

      public String getTxID()
      {
         return txID;
      }

   }
}
