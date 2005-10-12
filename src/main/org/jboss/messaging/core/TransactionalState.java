/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import org.jboss.logging.Logger;

/**
 * In memory (non-persistent), transactional state implementation.
 * 
 * FIXME remove this class
 * This class is legacy and no longer necessary.
 * Having a hierarchy of non-reliable, transactional, reliable does not
 * fit well into the JMS semantics IMHO
 * 
 * In reality delivery/acknowledgement can be:
 * 
 * Unreliable, non-transactional
 * Unreliable, transactional
 * Reliable, non-transactional
 * Reliable, transactional
 * 
 * I.e. there are four states, not three as would be implied by the previous
 * hierarchy.
 * 
 * So now we just have reliable and non-reliable state, and transactions are added
 * as a cross-cutting concern that is equally valid for both reliable
 * and non -reliable delivery/acks.
 * 
 * Doing this has significantly improved the clarity of the code IMHO
 * 
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
class TransactionalState extends UnreliableState
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TransactionalState.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public TransactionalState(Channel channel)
   {
      super(channel);
   }

   // StateSupport overrides -------------------------------------


//
//   public void add(Delivery d, Transaction tx) throws Throwable
//   {
//      super.add(d, tx);
//      /*
//      MessageReference ref = d.getReference();
//
//      if (d instanceof CompositeDelivery)
//      {
//         // TODO break CompositeDelivery in components and add them individually
//         throw new NotYetImplementedException("Don't know to handle composite deliveries");
//      }
//
//      // TODO This is a kludge, this should change when I start using a non-JTA-related API
//      if (ref.isRedelivered())
//      {
//         super.add(d, tx);
//         return;
//      }
//
//      if (tx == null)
//      {
//         super.add(d, null);
//         return;         
//      }
//
//      if (log.isTraceEnabled()) { log.trace("adding " + d + " transactionally"); }
//
//      Runnable task = new AddDeliveryTask(d);
//      tx.addPostCommitTasks(task);
//      */    
//   }
//
//   public boolean remove(Delivery d, Transaction tx) throws Throwable
//   {
//      return super.remove(d, tx);
//      /*
//      MessageReference ref = d.getReference();
//
//      if (d instanceof CompositeDelivery)
//      {
//         throw new NotYetImplementedException("Don't know to handle composite deliveries");
//      }
//
//      if (tx == null)
//      {
//         return super.remove(d, null);
//      }
//
//      if (log.isTraceEnabled()) { log.trace("removing " + d + " transactionally"); }
//
//      Runnable task = new RemoveDeliveryTask(d);
//      tx.addPostCommitTasks(task); 
//      
//      return true;
//      */
//   }
//
//
//   
//
//
//   public void add(MessageReference ref, Transaction tx) throws Throwable
//   {
//      super.add(ref, tx);
//      /*
//      if (tx == null)
//      {
//         // no active transaction, handle the message non-transactionally
//         super.add(ref, null);
//         return;
//      }
//
//      // add transactionally
//
//      Runnable task = new AddMessageTask(ref);
//      tx.addPostCommitTasks(task);
//      */
//   }
//
//   // Public --------------------------------------------------------
//
//   // Package protected ---------------------------------------------
//   
//   // Protected -----------------------------------------------------
//
//
//   /*
//   protected void enableTransactedMessages(String txID) throws SystemException
//   {
//      if (log.isTraceEnabled()) { log.trace("Enabling transacted messages"); }
//      List l = (List)transactedMessages.remove(txID);
//      if (l == null)
//      {
//         // may happen if all undelivered are reliable
//         return;
//
//      }
//      messageRefs.addAll(l);
//   }
//
//   protected void dropTransactedMessages(String txID) throws SystemException
//   {
//      List l = (List)transactedMessages.remove(txID);
//      if (l == null)
//      {
//         // may happen if all undelivered are reliable
//         return;
//      }
//      l.clear();
//   }
//   */
//
//
//   // Private -------------------------------------------------------
//
//   /*
//   private void commit(String txID) throws SystemException
//   {
//      if (log.isTraceEnabled()) { log.trace("Committing tx " + txID); }
//      enableTransactedMessages(txID);
//      channel.deliver();
//      
//      //FIXME
//      //I have added this for the following reason:
//      //Consider the following scenario:
//      //I send a persistent message in a transaction to a queue.
//      //There are no consumers attached to the queue.
//      //The transaction commits
//      //This results in an entry in the TRANSACTED_MESSAGE_REFERENCES table 
//      //but no entry in the MESSAGE_REFERENCES_TABLE
//      //since channel.deliver(above) saves the state when the message is not delivered.
//      //Then... at some time later, after the tx has completed, a consumer attaches to the
//      //queue. Deliver() is then called but because there is no entry in the message_references table
//      //then the message is not delivered.
//      //Hence I have added the following extra call to enableTransactedMessages to make sure any rows
//      //remaining in the TRANSACTED-MESSAGE_REFREENCES table get converted to MESSAGE_REFERENCES before
//      //the transaction completes.
//      //None of this seems very optimal and if you look in the logs there seem to be a lot of
//      //SQL activity just for one message. I imagine most of this can be optimised away.
//      //But for now, this seems to work.
//      //I have added a test to check for this scenario:
//      //MessageConsumerTest.testSendAndReceivePersistentDifferentConnections
//      //I think this should be improved if we merge the transacted and non-transacted message refs
//      
//      enableTransactedMessages(txID);
//   }
//
//   private void rollback(String txID) throws SystemException
//   {
//      dropTransactedMessages(txID);
//   }
//
//   // to be accessed only from generateUniqueTransactionID();
//   private int txID = 0;
//
//   private synchronized String generateUniqueTransactionID()
//   {
//      StringBuffer sb = new StringBuffer("TX:");
//      sb.append(channel.getChannelID());
//      sb.append(":");
//      sb.append(txID++);
//      sb.append(":");
//      sb.append(System.currentTimeMillis());
//      return sb.toString();
//   }
//   */

   // Inner classes -------------------------------------------------

   
   
}
