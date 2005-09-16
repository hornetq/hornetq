/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import org.jboss.logging.Logger;
import org.jboss.messaging.util.NotYetImplementedException;

import javax.transaction.TransactionManager;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.Status;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * In memory (non-persistent), transactional state implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
class TransactionalState extends StateSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TransactionalState.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected TransactionManager tm;
   private Map transactedMessages;
   private Map addMessageSynchronizations;
   private Map addDeliverySynchronizations;
   private Map removeDeliverySynchronizations;

   // Constructors --------------------------------------------------

   public TransactionalState(Channel channel, TransactionManager tm)
   {
      super(channel);
      if (tm == null)
      {
         throw new IllegalArgumentException("TransactionalState requires a non-null transaction manager");
      }
      this.tm = tm;

      transactedMessages = new HashMap();
      deliveries = new ArrayList();
      addMessageSynchronizations = new HashMap();
      addDeliverySynchronizations = new HashMap();
      removeDeliverySynchronizations = new HashMap();

   }

   // StateSupport overrides -------------------------------------

   public boolean isTransactional()
   {
      return true;
   }


   public void add(Delivery d) throws Throwable
   {
      Routable r = d.getRoutable();
      
      
      if (r.isReliable())
      {
         //throw new IllegalStateException("Cannot reliably carry out a reliable message delivery");
         
			//FIXME-
         //In the case of a persistent message being sent to a topic then the normal semantics of
         //persistent are lost. This is why I commented out the above exception throw.
         //Topics need to be able to store undelivered persistent messages in memory (not persisted)
         //so they can be redelivered for the lifetime of the non durable subscriber
         //See Jms 1.1 spec. 6.12
         
      }

      if (d instanceof CompositeDelivery)
      {
         // TODO break CompositeDelivery in components and add them individually
         throw new NotYetImplementedException("Don't know to handle composite deliveries");
      }

      Transaction tx = tm.getTransaction();
      if (tx == null)
      {
         throw new IllegalStateException("No active transaction");
      }
      registerAddDeliverySynchronization(tx, d);
   }

   public boolean remove(Delivery d) throws Throwable
   {
      Routable r = d.getRoutable();
      if (r.isReliable())
      {
         //throw new IllegalStateException("Cannot forget a reliable message delivery at this level");
         
         //FIXME-
         //In the case of a persistent message being sent to a topic then the normal semantics of
         //persistent are lost. This is why I commented out the above exception throw.
         //Topics need to be able to store undelivered persistent messages in memory (not persisted)
         //so they can be redelivered for the lifetime of the non durable subscriber.
         //Hence removal is ok here, even for a persistent message in the case of a topic
         //See Jms 1.1 spec. 6.12
         
      }

      if (d instanceof CompositeDelivery)
      {
         throw new NotYetImplementedException("Don't know to handle composite deliveries");
      }

      Transaction tx = tm.getTransaction();
      if (tx == null)
      {
         return super.remove(d);
      }
      registerRemoveDeliverySynchronization(tx, d);
      return true;
   }


   public List delivering(Filter filter)
   {
      List delivering = new ArrayList();
      for(Iterator i = deliveries.iterator(); i.hasNext(); )
      {
         Delivery d = (Delivery)i.next();
         Routable r = d.getRoutable();
         if (filter == null || filter.accept(r))
         {
            delivering.add(r);
         }
      }
      return delivering;
   }


   public void add(Routable r) throws Throwable
   {
      Transaction tx = tm.getTransaction();

      if (tx == null)
      {
         // no active transaction, handle the message non-transactionally
         super.add(r);
         return;
      }

      // add transactionally

      if (r.isReliable())
      {
         //tx.setRollbackOnly();
         //throw new IllegalStateException("Cannot reliably hold a transactional reliable message");
         
         //FIXME-
         //In the case of a persistent message being sent to a topic then the normal semantics of
         //persistent are lost. This is why I commented out the above exception throw.
         //Topics need to be able to store undelivered persistent messages in memory (not persisted)
         //so they can be redelivered for the lifetime of the non durable subscriber
         //See Jms 1.1 spec. 6.12
      }

      String txID = registerAddMessageSynchronization(tx);

      List l = (List)transactedMessages.get(txID);
      if (l == null)
      {
         l = new ArrayList();
         transactedMessages.put(txID, l);
      }
      l.add(r);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected String registerAddMessageSynchronization(Transaction tx) throws SystemException
   {
      String txID;
      synchronized(addMessageSynchronizations)
      {
         // TODO if the same logical transaction is represented by more than one Transaction instances, this won't work correctly
         AddMessageSynchronization sync =
               (AddMessageSynchronization)addMessageSynchronizations.get(tx);
         if (sync == null)
         {
            txID = generateUniqueTransactionID();
            sync = new AddMessageSynchronization(txID);
            try
            {
               tx.registerSynchronization(sync);
            }
            catch(RollbackException e)
            {
               log.error("Transaction already marked for rollback", e);
               return null;
            }

            addMessageSynchronizations.put(tx, sync);
         }

         txID = sync.getTxID();
      }
      return txID;
   }

   protected void registerAddDeliverySynchronization(Transaction tx, Delivery d)
         throws SystemException
   {
      synchronized(addDeliverySynchronizations)
      {
         AddDeliverySynchronization sync =
               (AddDeliverySynchronization)addDeliverySynchronizations.get(tx);
         if (sync == null)
         {
            sync = new AddDeliverySynchronization();
            try
            {
               tx.registerSynchronization(sync);
            }
            catch(RollbackException e)
            {
               log.error("Transaction already marked for rollback", e);
            }
            addDeliverySynchronizations.put(tx, sync);
         }
         sync.add(d);
      }
   }

   protected void registerRemoveDeliverySynchronization(Transaction tx, Delivery d) throws SystemException
   {
      synchronized(removeDeliverySynchronizations)
      {
         RemoveDeliverySynchronization sync =
               (RemoveDeliverySynchronization)removeDeliverySynchronizations.get(tx);
         if (sync == null)
         {
            sync = new RemoveDeliverySynchronization();
            try
            {
               tx.registerSynchronization(sync);
            }
            catch(RollbackException e)
            {
               log.error("Transaction already marked for rollback", e);
            }
            removeDeliverySynchronizations.put(tx, sync);
         }
         sync.add(d);
      }
   }

   protected void enableTransactedMessages(String txID) throws SystemException
   {
      if (log.isTraceEnabled()) { log.trace("Enabling transacted messages"); }
      List l = (List)transactedMessages.remove(txID);
      if (l == null)
      {
         // may happen if all undelivered are reliable
         return;

      }
      messages.addAll(l);
   }

   protected void dropTransactedMessages(String txID) throws SystemException
   {
      List l = (List)transactedMessages.remove(txID);
      if (l == null)
      {
         // may happen if all undelivered are reliable
         return;
      }
      l.clear();
   }


   // Private -------------------------------------------------------

   private void commit(String txID) throws SystemException
   {
      if (log.isTraceEnabled()) { log.trace("Committing tx " + txID); }
      enableTransactedMessages(txID);
      channel.deliver();
      
      //FIXME ???
      //I have added this for the following reason:
      //Consider the following scenario:
      //I send a persistent message in a transaction to a queue.
      //There are no consumers attached to the queue.
      //The transaction commits
      //This results in an entry in the TRANSACTED_MESSAGE_REFERENCES table 
      //but no entry in the MESSAGE_REFERENCES_TABLE
      //since channel.deliver(above) saves the state when the message is not delivered.
      //Then... at some time later, after the tx has completed, a consumer attaches to the
      //queue. Deliver() is then called but because there is no entry in the message_references table
      //then the message is not delivered.
      //Hence I have added the following extra call to enableTransactedMessages to make sure any rows
      //remaining in the TRANSACTED-MESSAGE_REFREENCES table get converted to MESSAGE_REFERENCES before
      //the transaction completes.
      //None of this seems very optimal and if you look in the logs there seem to be a lot of
      //SQL activity just for one message. I imagine most of this can be optimised away.
      //But for now, this seems to work.
      //I have added a test to check for this scenario:
      //MessageConsumerTest.testSendAndReceivePersistentDifferentConnections
      
      enableTransactedMessages(txID);
   }

   private void rollback(String txID) throws SystemException
   {
      dropTransactedMessages(txID);
   }

   // to be accessed only from generateUniqueTransactionID();
   private int txID = 0;

   private synchronized String generateUniqueTransactionID()
   {
      StringBuffer sb = new StringBuffer("TX:");
      sb.append(channel.getChannelID());
      sb.append(":");
      sb.append(txID++);
      sb.append(":");
      sb.append(System.currentTimeMillis());
      return sb.toString();
   }

   // Inner classes -------------------------------------------------

   private class AddMessageSynchronization implements Synchronization
   {
      // the channel-specific ID that identifies the transaction this Synchronization is attached to
      private String txID;

      AddMessageSynchronization(String txID)
      {
         this.txID = txID;
      }

      // Synchronization implementation --------------------------------

      public void beforeCompletion()
      {
         try
         {
            
            if (log.isTraceEnabled()) { log.trace("In AddMessageSynchronization.beforeCompletion()"); }
            
            // this call is executed with the transaction context of the transaction
            // that is being committed
            Transaction crtTransaction = tm.getTransaction();

            if (crtTransaction.getStatus() == Status.STATUS_MARKED_ROLLBACK)
            {
               return;
            }

            // enable delivery
            commit(txID);

         }
         catch(SystemException e)
         {
            // not much to do, just log the exception
            log.error("Current transaction failed", e);

            try
            {
               rollback(txID);
            }
            catch(SystemException se)
            {
               log.error("Rollback failed", se);
            }
         }
      }

      public void afterCompletion(int status)
      {
//         try
//         {
//            rollback(txID);
//         }
//         catch(SystemException se)
//         {
//            log.error("Rollback failed", se);
//         }
      }

      // Public implementation -----------------------------------------

      public String getTxID()
      {
         return txID;
      }
   }


   private class AddDeliverySynchronization implements Synchronization
   {

      private List txDeliveries;

      // Constructors --------------------------------------------------

      public AddDeliverySynchronization()
      {
         txDeliveries = new ArrayList();
      }

      // Synchronization implementation --------------------------------

      public void beforeCompletion()
      {
         try
         {
            Transaction crtTransaction = tm.getTransaction();
            if (crtTransaction.getStatus() == Status.STATUS_MARKED_ROLLBACK)
            {
               txDeliveries.clear();
               txDeliveries = null;
               return;
            }

            // accept all deliveries
            deliveries.addAll(txDeliveries);
         }
         catch(SystemException e)
         {
            log.error("Current transaction failed", e);
         }
      }

      public void afterCompletion(int status)
      {
      }

      // Public --------------------------------------------------------

      public void add(Delivery d)
      {
         deliveries.add(d);
      }
   }


   private class RemoveDeliverySynchronization implements Synchronization
   {

      private List toRemove;

      // Constructors --------------------------------------------------

      public RemoveDeliverySynchronization()
      {
         toRemove = new ArrayList();
      }

      // Synchronization implementation --------------------------------

      public void beforeCompletion()
      {
         try
         {
            Transaction crtTransaction = tm.getTransaction();
            if (crtTransaction.getStatus() == Status.STATUS_MARKED_ROLLBACK)
            {
               toRemove.clear();
               toRemove = null;
               return;
            }

            // remove all deliveries
            deliveries.removeAll(toRemove);
         }
         catch(SystemException e)
         {
            log.error("Current transaction failed", e);
         }
      }

      public void afterCompletion(int status)
      {
      }

      // Public --------------------------------------------------------

      public void add(Delivery d)
      {
         toRemove.add(d);
      }
   }


}
