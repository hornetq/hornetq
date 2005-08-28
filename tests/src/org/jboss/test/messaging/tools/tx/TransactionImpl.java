/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.tx;

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;

import javax.transaction.SystemException;
import javax.transaction.RollbackException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.Transaction;
import javax.transaction.Synchronization;
import javax.transaction.Status;
import javax.transaction.TransactionRolledbackException;
import javax.transaction.HeuristicCommitException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import javax.resource.spi.work.Work;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.rmi.RemoteException;

/**
 * A transaction implementation used for testing.
 * 
 * @version <tt>$Revision$</tt>
 */
class TransactionImpl implements Transaction
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TransactionImpl.class);

   // Code meaning "no heuristics seen", must not be XAException.XA_HEURxxx
   private static final int HEUR_NONE = XAException.XA_RETRY;

   // Resource states
   private final static int RS_NEW = 0; // not yet enlisted
   private final static int RS_ENLISTED = 1; // enlisted
   private final static int RS_SUSPENDED = 2; // suspended
   private final static int RS_ENDED = 3; // not associated
   private final static int RS_VOTE_READONLY = 4; // voted read-only
   private final static int RS_VOTE_OK = 5; // voted ok
   private final static int RS_FORGOT = 6; // RM has forgotten




   // Static --------------------------------------------------------

   static XidFactory xidFactory = new XidFactory();

   // Attributes ----------------------------------------------------

   // The time when this transaction was started.
   private long start;

   private Timeout timeout;
   private long timeoutPeriod;


   // Flags that we are done with this transaction and that it can be reused.
   private boolean done = false;

   // Mutex for thread-safety. This should only be changed in the lock()/unlock() methods.
   private boolean locked = false;

   // The ID of this transaction.
   private XidImpl xid;

   // Status of this transaction.
   private int status;

   /**
    * True for a foreign transaction that has been imported either through an DTM/OTS transaction
    * propagation context or via JCA transaction inflow; false for a locally started transaction.
    */
   private final boolean foreignTx;


   // The synchronizations to call back.
   private Synchronization[] sync = new Synchronization[3];

   // Size of allocated synchronization array.
   private int syncAllocSize = 3;

   // Count of synchronizations for this transaction.
   private int syncCount = 0;

//   /**
//    * This transaction's parent coordinator, or null if this transaction does not have a parent
//    * coordinator. A transaction with no parent coodinator is either a (locally started) root
//    * transaction or a foreign transaction that has been imported via JCA transaction inflow.
//    */
//   private Coordinator parentCoordinator = null;
//
//   /**
//    * This transaction's recovery coordinator, or null this transaction did not register itself as
//    * a remote resource with the parent coordinator.
//    */
//   private RecoveryCoordinator recoveryCoordinator = null;

   // A list of the XAResources that have participated in this transaction.
   private ArrayList xaResources = new ArrayList(3);

   // A list of the remote resources that have participated in this transaction.
   private ArrayList remoteResources = new ArrayList(3);

   // The XAResource used in the last resource gambit.
   private EnlistedXAResource lastResource;

   // Flags that it is too late to enlist new resources.
   private boolean resourcesEnded = false;

   // Last branch id used.
   private long lastBranchId = 0;

   private HashSet threads = new HashSet(1);

   private HashMap transactionLocalMap = new HashMap();

   private Throwable cause;

   // The heuristics status of this transaction.
   private int heuristicCode = HEUR_NONE;

   /**
    * Any current work associated with the transaction
    */
   private Work work;

   // Constructors --------------------------------------------------

   public TransactionImpl(long timeout)
   {
      foreignTx = false;

      xid = xidFactory.newXid();

      status = Status.STATUS_ACTIVE;

      start = System.currentTimeMillis();

      // this transaction will never timeout
      //
      // this.timeout = TimeoutFactory.createTimeout(start + timeout, this);
      //

      timeoutPeriod = timeout;

   }

   // Transaction implementation ------------------------------------

   public void commit() throws RollbackException, HeuristicMixedException,
                               HeuristicRollbackException, SecurityException, SystemException
   {
      lock();
      try
      {
         beforePrepare();

         if (status == Status.STATUS_ACTIVE)
         {
            switch (getCommitStrategy())
            {
               case 0:
                  if (log.isTraceEnabled()) { log.trace("Zero phase commit " + this + ": No resources."); }
                  status = Status.STATUS_COMMITTED;
                  break;

               case 1:
                  if (log.isTraceEnabled()) { log.trace("One phase commit " + this + ": One resource."); }
                  commitResources(true, null);
                  break;

               default:
                  if (log.isTraceEnabled()) { log.trace("Two phase commit " + this + ": Many resources."); }

                  if (!prepareResources())
                  {
                     boolean commitDecision =
                           status == Status.STATUS_PREPARED &&
                           (heuristicCode == HEUR_NONE ||
                            heuristicCode == XAException.XA_HEURCOM);

                     if (commitDecision)
                     {
                        RecoveryLogger logger = TransactionManagerImpl.getInstance().getRecovery();
                        RecoveryLogTerminator terminator = null;
                        try
                        {
                           if (logger != null)
                           {
                              terminator = logger.committing(xid);
                           }
                           commitResources(false, terminator);
                        }
                        catch (Throwable e)
                        {
                           if (e instanceof RecoveryTestingException)
                           {
                              throw (RecoveryTestingException) e;
                           }
                           log.warn("FAILED DURING RECOVERY LOG COMMITTING RECORD."
                                    + " Rolling back now.");
                        }
                     }
                  }
                  else
                  {
                     status = Status.STATUS_COMMITTED; // all was read-only
                  }
            }
         }

         if (status != Status.STATUS_COMMITTED)
         {
//            Throwable causedByThrowable = cause;
            rollbackResources();
            completeTransaction();

            // throw jboss rollback exception with the saved off cause
            throw new RollbackException("Unable to commit, tx=" + toString() + " status=" +
                                        getStringStatus(status));
         }

         completeTransaction();
         checkHeuristics();

         if (log.isTraceEnabled())
         {
            log.trace("Committed OK, tx=" + this);
         }
      }
      finally
      {
         unlock();
      }

   }


   public void rollback() throws IllegalStateException, SystemException
   {
      lock();
      try
      {
         checkWork();

         switch (status)
         {
            case Status.STATUS_ACTIVE:
               status = Status.STATUS_MARKED_ROLLBACK;
               // fall through..
            case Status.STATUS_MARKED_ROLLBACK:
               endResources();
               rollbackResources();
               completeTransaction();
               // Cannot throw heuristic exception, so we just have to
               // clear the heuristics without reporting.
               heuristicCode = HEUR_NONE;
               return;
            case Status.STATUS_PREPARING:
               // Set status to avoid race with prepareResources().
               status = Status.STATUS_MARKED_ROLLBACK;
               return; // commit() will do rollback.
            default:
               throw new IllegalStateException("Cannot rollback(), tx=" +
                                               toString() + " status=" +
                                               getStringStatus(status));
         }
      }
      finally
      {
         Thread.interrupted();// clear timeout that did an interrupt
         unlock();
      }
   }


   public void setRollbackOnly() throws IllegalStateException, SystemException
   {
      lock();
      try
      {
         if (log.isTraceEnabled()) { log.trace("setRollbackOnly(): Entered, tx=" + toString() + " status=" + getStringStatus(status)); }

         switch (status)
         {
            case Status.STATUS_ACTIVE:
            case Status.STATUS_PREPARING:
            case Status.STATUS_PREPARED:
               status = Status.STATUS_MARKED_ROLLBACK;
               // fall through..
            case Status.STATUS_MARKED_ROLLBACK:
            case Status.STATUS_ROLLING_BACK:
               return;
            case Status.STATUS_COMMITTING:
               throw new IllegalStateException("Already started committing. " + this);
            case Status.STATUS_COMMITTED:
               throw new IllegalStateException("Already committed. " + this);
            case Status.STATUS_ROLLEDBACK:
               throw new IllegalStateException("Already rolled back. " + this);
            case Status.STATUS_NO_TRANSACTION:
               throw new IllegalStateException("No transaction. " + this);
            case Status.STATUS_UNKNOWN:
               throw new IllegalStateException("Unknown state " + this);
            default:
               throw new IllegalStateException("Illegal status: " + getStringStatus(status) +
                                               " tx=" + this);
         }
      }
      finally
      {
         unlock();
      }
   }

   public int getStatus() throws SystemException
   {
      if (done)
      {
         return Status.STATUS_NO_TRANSACTION;
      }
      return status;
   }


   public boolean enlistResource(XAResource xaRes)
         throws RollbackException, IllegalStateException, SystemException
   {
      throw new NotYetImplementedException();
   }


   public boolean delistResource(XAResource xaRes, int flag)
         throws IllegalStateException, SystemException
   {
      throw new NotYetImplementedException();
   }


   public void registerSynchronization(Synchronization s)
         throws RollbackException, IllegalStateException, SystemException
   {
      if (s == null)
      {
         throw new IllegalArgumentException("Null synchronization " + this);
      }

      lock();
      try
      {
         checkStatus();

         if (syncCount == syncAllocSize)
         {
            // expand table
            syncAllocSize = 2 * syncAllocSize;

            Synchronization[] sy = new Synchronization[syncAllocSize];
            System.arraycopy(sync, 0, sy, 0, syncCount);
            sync = sy;
         }
         sync[syncCount++] = s;

//         // Register itself as a resource with the parent coordinator
//         if (parentCoordinator != null && recoveryCoordinator == null)
//         {
//            registerResourceWithParentCoordinator();
//         }
      }
      finally
      {
         unlock();
      }

   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   boolean isDone()
   {
      return done;
   }

   void associateCurrentThread()
   {
      Thread.interrupted();
      lock();
      try
      {
         threads.add(Thread.currentThread());
      }
      finally
      {
         unlock();
      }
   }

   void disassociateCurrentThread()
   {
      if (done)
      {
         threads.remove(Thread.currentThread());
      }
      else
      {
         // Removing the association for an active transaction
         lock();
         try
         {
            threads.remove(Thread.currentThread());
         }
         finally
         {
            unlock();
         }
      }
      Thread.interrupted();
   }

   /**
    * Returns the local id of this transaction. The local id is used as a transaction propagation
    * context within the JBoss server, and in the TransactionManagerImpl for mapping local
    * transaction ids to transactions.
    */
   LocalId getLocalId()
   {
      return xid.getLocalId();
   }

   /**
    * Returns the global id of this transaction. Ths global id is used in the TransactionManager,
    * which keeps a map from global ids to transactions.
    */
   GlobalId getGlobalId()
   {
      return xid.getTrulyGlobalId();
   }


   // Protected -----------------------------------------------------

   synchronized void lock()
   {
      if (done)
      {
         throw new IllegalStateException("Transaction has terminated " + this);
      }

      if (locked)
      {
         log.warn("Lock contention, transaction =" + toString());

         while (locked)
         {
            try
            {
               // Wakeup happens when:
               // - notify() is called from unlock()
               // - notifyAll is called from instanceDone()
               wait();
            }
            catch (InterruptedException ex)
            {
               // ignore
            }

            if (done)
            {
               throw new IllegalStateException("Transaction has now terminated " + this);
            }
         }
      }
      locked = true;
   }

   synchronized void unlock()
   {
      if (!locked)
      {
         log.warn("Unlocking, but not locked, tx=" + toString(), new Throwable("[Stack trace]"));
      }

      locked = false;
      notify();
   }


   // Private -------------------------------------------------------

   /**
    * Before prepare
    */
   private void beforePrepare()
         throws HeuristicMixedException, HeuristicRollbackException, RollbackException
   {
      switch (status)
      {
         case Status.STATUS_PREPARING:
            throw new IllegalStateException("Already started preparing. " + this);
         case Status.STATUS_PREPARED:
            throw new IllegalStateException("Already prepared. " + this);
         case Status.STATUS_ROLLING_BACK:
            throw new IllegalStateException("Already started rolling back. " +
                                            this);
         case Status.STATUS_ROLLEDBACK:
            instanceDone();
            checkHeuristics();
            throw new IllegalStateException("Already rolled back." + this);
         case Status.STATUS_COMMITTING:
            throw new IllegalStateException("Already started committing. " + this);
         case Status.STATUS_COMMITTED:
            instanceDone();
            checkHeuristics();
            throw new IllegalStateException("Already committed. " + this);
         case Status.STATUS_NO_TRANSACTION:
            throw new IllegalStateException("No transaction. " + this);
         case Status.STATUS_UNKNOWN:
            throw new IllegalStateException("Unknown state " + this);
         case Status.STATUS_MARKED_ROLLBACK:
            endResources();
            rollbackResources();
            completeTransaction();
            checkHeuristics();
            throw new RollbackException("Already marked for rollback " + this);
         case Status.STATUS_ACTIVE:
            break;
         default:
            throw new IllegalStateException("Illegal status: " + getStringStatus(status) +
                                            " tx=" + this);
      }

      doBeforeCompletion();

      if (log.isTraceEnabled()) { log.trace("Before completion done, tx=" + this + ", status=" + getStringStatus(status)); }

      endResources();
   }


   /**
    * Mark this transaction as non-existing.
    */
   private synchronized void instanceDone()
   {
      TransactionManagerImpl manager = TransactionManagerImpl.getInstance();

      if (status == Status.STATUS_COMMITTED)
      {
         manager.incCommitCount();
      }
      else
      {
         manager.incRollbackCount();
      }

      // Clear tables refering to external objects. Even if a client holds on to this instance
      // forever, the objects that we have referenced may be garbage collected.

//      parentCoordinator = null;
      sync = null;
      xaResources = null;
      remoteResources = null;
      transactionLocalMap.clear();
      threads.clear();

      manager.releaseTransactionImpl(this);

      status = Status.STATUS_NO_TRANSACTION;
      notifyAll();
      done = true;
   }

   /**
    * Call synchronization beforeCompletion(). This will release the lock while calling out.
    */
   private void doBeforeCompletion()
   {
      unlock();
      try
      {
         for (int i = 0; i < syncCount; i++)
         {
            try
            {
               if (log.isTraceEnabled()) { log.trace("calling sync " + i + ", " + sync[i] + " tx=" + this); }

               sync[i].beforeCompletion();
            }
            catch (Throwable t)
            {
               if (log.isTraceEnabled()) { log.trace("failed before completion " + sync[i], t); }

               status = Status.STATUS_MARKED_ROLLBACK;

               // save the cause off so the user can inspect it
               cause = t;
               break;
            }
         }
      }
      finally
      {
         lock();
      }
   }

   /**
    * Call synchronization afterCompletion(). This will release the lock while calling out.
    */
   private void doAfterCompletion()
   {
      // Assert: Status indicates: Too late to add new synchronizations.
      unlock();
      try
      {
         for (int i = 0; i < syncCount; i++)
         {
            try
            {
               sync[i].afterCompletion(status);
            }
            catch (Throwable t)
            {
               if (log.isTraceEnabled()) { log.trace("failed after completion " + sync[i], t); }
            }
         }
      }
      finally
      {
         lock();
      }
   }

   /**
    * We got another heuristic. Promote heuristicCode if needed and tell the resource to forget the
    * heuristic. This will release the lock while calling out.
    *
    * @param resource The resource of the XA resource that got a heuristic in our internal tables,
    *        or null if the heuristic came from here.
    * @param code The heuristic code, one of XAException.XA_HEURxxx.
    */
   private void gotHeuristic(EnlistedResource resource, int code)
   {
      switch (code)
      {
         case XAException.XA_HEURMIX:
            heuristicCode = XAException.XA_HEURMIX;
            break;
         case XAException.XA_HEURRB:
            if (heuristicCode == HEUR_NONE)
            {
               heuristicCode = XAException.XA_HEURRB;
            }
            else if (heuristicCode == XAException.XA_HEURCOM ||
                     heuristicCode == XAException.XA_HEURHAZ)
            {
               heuristicCode = XAException.XA_HEURMIX;
            }
            break;
         case XAException.XA_HEURCOM:
            if (heuristicCode == HEUR_NONE)
            {
               heuristicCode = XAException.XA_HEURCOM;
            }
            else if (heuristicCode == XAException.XA_HEURRB ||
                     heuristicCode == XAException.XA_HEURHAZ)
            {
               heuristicCode = XAException.XA_HEURMIX;
            }
            break;
         case XAException.XA_HEURHAZ:
            if (heuristicCode == HEUR_NONE)
            {
               heuristicCode = XAException.XA_HEURHAZ;
            }
            else if (heuristicCode == XAException.XA_HEURCOM ||
                     heuristicCode == XAException.XA_HEURRB)
            {
               heuristicCode = XAException.XA_HEURMIX;
            }
            break;
         default:
            throw new IllegalArgumentException();
      }

      if (resource != null)
      {
         resource.forget();
      }
   }


   /**
    * Check for heuristics, clear and throw exception if any found.
    */
   private void checkHeuristics() throws HeuristicMixedException, HeuristicRollbackException
   {
      boolean trace = log.isTraceEnabled();

      switch (heuristicCode)
      {
         case XAException.XA_HEURHAZ:
         case XAException.XA_HEURMIX:
            heuristicCode = HEUR_NONE;
            if (trace) { log.trace("Throwing HeuristicMixedException, tx=" + this + "status=" + getStringStatus(status)); }
            throw new HeuristicMixedException();
         case XAException.XA_HEURRB:
            heuristicCode = HEUR_NONE;
            if (trace) { log.trace("Throwing HeuristicRollbackException, tx=" + this + "status=" + getStringStatus(status)); }
            throw new HeuristicRollbackException();
         case XAException.XA_HEURCOM:
            heuristicCode = HEUR_NONE;
            // Why isn't HeuristicCommitException used in JTA ?
            // And why define something that is not used ?
            // For now we just have to ignore this failure, even if it happened
            // on rollback.
            if (trace) {log.trace("NOT Throwing HeuristicCommitException, tx=" + this + "status=" + getStringStatus(status)); }
            return;
      }
   }

   /**
    * Complete the transaction
    */
   private void completeTransaction()
   {
      cancelTimeout();
      doAfterCompletion();
      instanceDone();
   }

   /**
    * Checks if the transaction state allows calls to the methods enlistResource() and
    * registerSynchronization()
    */
   private void checkStatus() throws RollbackException
   {
      switch (status)
      {
         case Status.STATUS_ACTIVE:
         case Status.STATUS_PREPARING:
            break;
         case Status.STATUS_PREPARED:
            throw new IllegalStateException("Already prepared. " + this);
         case Status.STATUS_COMMITTING:
            throw new IllegalStateException("Already started committing. " + this);
         case Status.STATUS_COMMITTED:
            throw new IllegalStateException("Already committed. " + this);
         case Status.STATUS_MARKED_ROLLBACK:
            throw new RollbackException("Already marked for rollback " + this);
         case Status.STATUS_ROLLING_BACK:
            throw new RollbackException("Already started rolling back. " + this);
         case Status.STATUS_ROLLEDBACK:
            throw new RollbackException("Already rolled back. " + this);
         case Status.STATUS_NO_TRANSACTION:
            throw new IllegalStateException("No transaction. " + this);
         case Status.STATUS_UNKNOWN:
            throw new IllegalStateException("Unknown state " + this);
         default:
            throw new IllegalStateException("Illegal status: " + getStringStatus(status) +
                                            " tx=" + this);
      }
   }

   private String getStringStatus(int status)
   {
      switch (status)
      {
         case Status.STATUS_PREPARING:
            return "STATUS_PREPARING";
         case Status.STATUS_PREPARED:
            return "STATUS_PREPARED";
         case Status.STATUS_ROLLING_BACK:
            return "STATUS_ROLLING_BACK";
         case Status.STATUS_ROLLEDBACK:
            return "STATUS_ROLLEDBACK";
         case Status.STATUS_COMMITTING:
            return "STATUS_COMMITING";
         case Status.STATUS_COMMITTED:
            return "STATUS_COMMITED";
         case Status.STATUS_NO_TRANSACTION:
            return "STATUS_NO_TRANSACTION";
         case Status.STATUS_UNKNOWN:
            return "STATUS_UNKNOWN";
         case Status.STATUS_MARKED_ROLLBACK:
            return "STATUS_MARKED_ROLLBACK";
         case Status.STATUS_ACTIVE:
            return "STATUS_ACTIVE";
         default:
            return "STATUS_UNKNOWN(" + status + ")";
      }
   }

//   /**
//    * Registers this transaction as a resource with its parent coordinator.
//    * Called from <code>enlistResource</code>.
//    */
//   private void registerResourceWithParentCoordinator() throws RollbackException
//   {
//      throw new NotYetImplementedException();
//   }


   /**
    * Prepare all enlisted resources. If the first phase of the commit process results in a decision
    * to commit the status will be Status.STATUS_PREPARED on return. Otherwise the status will be
    * Status.STATUS_MARKED_ROLLBACK on return. This will release the lock while calling out.
    *
    * @return True iff all resources voted read-only.
    */
   private boolean prepareResources()
   {
      boolean readOnly = true;

      status = Status.STATUS_PREPARING;

      // Prepare XA resources.
      for (int i = 0; i < xaResources.size(); ++i)
      {
         // Abort prepare on state change.
         if (status != Status.STATUS_PREPARING)
         {
            return false;
         }

         EnlistedXAResource resource = (EnlistedXAResource) xaResources.get(i);

         if (resource.isResourceManager() == false)
         {
            continue; // This RM already prepared.
         }

         // Ignore the last resource it is done later
         if (resource == lastResource)
         {
            continue;
         }

         try
         {
            int vote = resource.prepare();

            if (vote == RS_VOTE_OK)
            {
               readOnly = false;
            }
            else if (vote != RS_VOTE_READONLY)
            {
               // Illegal vote: rollback.
               if (log.isTraceEnabled()) { log.trace("illegal vote in prepare resources tx=" + this + " resource=" + resource, new Exception()); }
               status = Status.STATUS_MARKED_ROLLBACK;
               return false;
            }
         }
         catch (XAException e)
         {
            readOnly = false;
            logXAException(e);
            switch (e.errorCode)
            {
            case XAException.XA_HEURCOM:
               // Heuristic commit is not that bad when preparing.
               // But it means trouble if we have to rollback.
               gotHeuristic(resource, e.errorCode);
               break;
            case XAException.XA_HEURRB:
            case XAException.XA_HEURMIX:
            case XAException.XA_HEURHAZ:
               // Other heuristic exceptions cause a rollback
               gotHeuristic(resource, e.errorCode);
               // fall through
            default:
               cause = e;
               status = Status.STATUS_MARKED_ROLLBACK;
               break;
            }
         }
         catch (Throwable t)
         {
            if (log.isTraceEnabled()) { log.trace("unhandled throwable in prepareResources " + this, t); }
            status = Status.STATUS_MARKED_ROLLBACK;
            cause = t;
            return false;
         }
      }

      // Prepare remote DTM/OTS resources.
      for (int i = 0; i < remoteResources.size(); ++i)
      {
         // Abort prepare on state change.
         if (status != Status.STATUS_PREPARING)
         {
            return false;
         }

         EnlistedRemoteResource resource =
            (EnlistedRemoteResource) remoteResources.get(i);

         try
         {
            int vote = resource.prepare();

            if (vote == RS_VOTE_OK)
            {
               readOnly = false;
            }
            else if (vote != RS_VOTE_READONLY)
            {
               // Illegal vote: rollback.
               if (log.isTraceEnabled()) { log.trace("illegal vote in prepare resources tx=" + this + " resource=" + resource, new Exception()); }
               status = Status.STATUS_MARKED_ROLLBACK;
               return false;
            }
         }
         catch (Exception e)
         {
            // Blanket catch for the following exception types:
            //   TransactionAlreadyPreparedException,
            //   HeuristicMixedException,
            //   HeuristicHazardException, and
            //   RemoteException, which includes TransactionRolledbackException.
            if (log.isTraceEnabled()) { log.trace("Exception in prepareResources " + this, e); }

            if (e instanceof HeuristicMixedException)
            {
               gotHeuristic(resource, XAException.XA_HEURMIX);
            }
            else if (e instanceof HeuristicHazardException)
            {
               gotHeuristic(resource, XAException.XA_HEURHAZ);
            }

            cause = e;
            status = Status.STATUS_MARKED_ROLLBACK;
         }
      }

      // Abort prepare on state change.
      if (status != Status.STATUS_PREPARING)
         return false;

      // Are we doing the last resource gambit?
      if (lastResource != null)
      {
         try
         {
            lastResource.prepareLastResource();
            lastResource.commit(false);
         }
         catch (XAException e)
         {
            if (log.isTraceEnabled()) { log.trace("prepareResources got XAException when committing last resource " + this, e); }
            logXAException(e);
            switch (e.errorCode)
            {
            case XAException.XA_HEURCOM:
               // Ignore this exception, as the heuristic outcome
               // is the one we wanted anyway.
               if (log.isTraceEnabled()) { log.trace("prepareResources ignored XAException.XA_HEURCOM when committing last resource " + this, e); }
               break;
            case XAException.XA_HEURRB:
            case XAException.XA_HEURMIX:
            case XAException.XA_HEURHAZ:
               //usually throws an exception, but not for a couple of cases.
               gotHeuristic(lastResource, e.errorCode);
               // fall through
            default:
               // take the XAException as a "no" vote from the last resource
               status = Status.STATUS_MARKED_ROLLBACK;
               cause = e;
               return false; // to make the caller look at at the "marked rollback" status
            }
         }
         catch (Throwable t)
         {
            if (log.isTraceEnabled()) { log.trace("unhandled throwable in prepareResources " + this, t); }
            status = Status.STATUS_MARKED_ROLLBACK;
            cause = t;
            return false; // to make the caller look at the "marked rollback" status
         }
      }

      if (status == Status.STATUS_PREPARING)
         status = Status.STATUS_PREPARED;

      return readOnly;
   }


   /**
    * Commit all enlisted resources. This will release the lock while calling out.
    */
   private void commitResources(boolean onePhase, RecoveryLogTerminator terminator)
   {
      status = Status.STATUS_COMMITTING;
      boolean commitFailure = false;
      int committedResources = 0;

      // Commit XA resources.
      for (int i = 0; i < xaResources.size(); ++i)
      {

         // Abort commit on state change.
         if (status != Status.STATUS_COMMITTING)
         {
            return;
         }

         EnlistedXAResource resource = (EnlistedXAResource) xaResources.get(i);

         // Ignore the last resource, it is already committed
         if (onePhase == false && lastResource == resource)
         {
            continue;
         }

         try
         {
            resource.commit(onePhase);
            committedResources++;
         }
         catch (XAException e)
         {
            logXAException(e);
            switch (e.errorCode)
            {
               case XAException.XA_HEURCOM:
                  // Ignore this exception, as the the heuristic outcome is the one we wanted anyway
                  committedResources++;
                  if (log.isTraceEnabled()) { log.trace("commitResources ignored XAException.XA_HEURCOM " + this, e); }
                  // Two phase commit is committed after prepare is logged.
                  break;
               case XAException.XA_HEURRB:
               case XAException.XA_HEURMIX:
               case XAException.XA_HEURHAZ:
                  //usually throws an exception, but not for a couple of cases.
                  gotHeuristic(resource, e.errorCode);
                  if (onePhase)
                  {
                     status = Status.STATUS_MARKED_ROLLBACK;
                  }
                  break;
               default:
                  commitFailure = true;
                  cause = e;
                  if (onePhase)
                  {
                     status = Status.STATUS_MARKED_ROLLBACK;
                     break;
                  }
                  //Not much we can do if there is an RMERR in the
                  //commit phase of 2pc. I guess we try the other rms.
            }
         }
         catch (Throwable t)
         {
            if (t instanceof RecoveryTestingException)
            {
               throw (RecoveryTestingException) t;
            }
            if (log.isTraceEnabled()) { log.trace("unhandled throwable in commitResources " + this, t); }
         }
      }

      // Commit remote DTM/OTS resources.
      for (int i = 0; i < remoteResources.size(); ++i)
      {
         // Abort commit on state change.
         if (status != Status.STATUS_COMMITTING)
         {
            return;
         }

         EnlistedRemoteResource resource = (EnlistedRemoteResource) remoteResources.get(i);

         try
         {
            resource.commit(onePhase);
            committedResources++;
         }
         catch (TransactionRolledbackException e)
         {
            commitFailure = true;
            cause = e;
            if (onePhase)
            {
               status = Status.STATUS_MARKED_ROLLBACK;
            }
            else
            {
               // The resource decided to rollback in the second phase of 2PC:
               // this is a heuristic outcome.
               if (committedResources == 0)
               {
                  gotHeuristic(resource, XAException.XA_HEURRB);
               }
               else
               {
                  gotHeuristic(resource, XAException.XA_HEURMIX);
               }
            }
         }
         catch (Exception e)
         {
            // Blanket catch for the following exception types:
            //   TransactionNotPreparedException,
            //   HeuristicRollbackException,
            //   HeuristicMixedException,
            //   HeuristicHazardException, and
            //   RemoteException.
            if (log.isTraceEnabled()) { log.trace("Exception in commitResources " + this, e); }

            if (e instanceof HeuristicRollbackException)
            {
               gotHeuristic(resource, XAException.XA_HEURRB);
            }
            else if (e instanceof HeuristicMixedException)
            {
               gotHeuristic(resource, XAException.XA_HEURMIX);
            }
            else if (e instanceof HeuristicHazardException)
            {
               gotHeuristic(resource, XAException.XA_HEURHAZ);
            }
            else
            {
               commitFailure = true;
               cause = e;
            }
            if (status == Status.STATUS_PREPARING)
            {
               status = Status.STATUS_MARKED_ROLLBACK;
            }
         }
      }

      // we not not clean up tx in logger if there was at least one commit failure.
      // we'll retry again at recovery.
      if (terminator != null && !commitFailure)
      {
         try
         {
            terminator.committed(xid);
         }
         catch (Exception ignored)
         {
         }
      }

      if (status == Status.STATUS_COMMITTING)
      {
         status = Status.STATUS_COMMITTED;
      }
   }

   /**
    * Rollback all enlisted resources.
    * This will release the lock while calling out.
    */
   private void rollbackResources()
   {
      status = Status.STATUS_ROLLING_BACK;

      // Rollback XA resources.
      for (int i = 0; i < xaResources.size(); ++i)
      {
         EnlistedXAResource resource = (EnlistedXAResource) xaResources.get(i);
         try
         {
            resource.rollback();
         }
         catch (XAException e)
         {
            logXAException(e);
            switch (e.errorCode)
            {
               case XAException.XA_HEURRB:
                  // Heuristic rollback is not that bad when rolling back.
                  gotHeuristic(resource, e.errorCode);
                  continue;
               case XAException.XA_HEURCOM:
               case XAException.XA_HEURMIX:
               case XAException.XA_HEURHAZ:
                  gotHeuristic(resource, e.errorCode);
                  continue;
               default:
                  cause = e;
                  break;
            }
         }
         catch (Throwable t)
         {
            if (t instanceof RecoveryTestingException)
            {
               throw (RecoveryTestingException) t;
            }
            if (log.isTraceEnabled()) { log.trace("unhandled throwable in rollbackResources " + this, t); }
         }
      }

      // Rollback remote DTM/OTS resources.
      for (int i = 0; i < remoteResources.size(); ++i)
      {
         EnlistedRemoteResource resource = (EnlistedRemoteResource) remoteResources.get(i);

         try
         {
            resource.rollback();
         }
         catch (HeuristicCommitException e)
         {
            gotHeuristic(resource, XAException.XA_HEURCOM);
            continue;
         }
         catch (HeuristicMixedException e)
         {
            gotHeuristic(resource, XAException.XA_HEURMIX);
            continue;
         }
         catch (HeuristicHazardException e)
         {
            gotHeuristic(resource, XAException.XA_HEURHAZ);
            continue;
         }
         catch (RemoteException e)
         {
            cause = e;
         }
      }

      status = Status.STATUS_ROLLEDBACK;
   }

   /**
    * Create an Xid representing a new branch of this transaction.
    */
   private Xid createXidBranch()
   {
      long branchId = ++lastBranchId;

      return xidFactory.newBranch(xid, branchId);
   }

   /**
    * Determine the commit strategy
    *
    * @return 0 for nothing to do, 1 for one phase and 2 for two phase
    */
   private int getCommitStrategy()
   {
      int xaResourceCount = xaResources.size();
      int remoteResourceCount = remoteResources.size();
      int resourceCount = xaResourceCount + remoteResourceCount;

      if (resourceCount == 0)
      {
         return 0;
      }
      if (resourceCount == 1)
      {
         return 1;
      }

      // At least two resources have participated in this transaction.

      if (remoteResourceCount > 0)
      {
         // They cannot be all XA resources on the same RM,
         // as at least one of them is a remote DTM/OTS resource.
         return 2;
      }
      else  // They are all XA resources, look if the're all on the same RM.
      {
         // First XAResource surely isResourceManager, it's the first!
         for (int i = 1; i < xaResourceCount; ++i)
         {
            EnlistedXAResource resource = (EnlistedXAResource) xaResources.get(i);
            if (resource.isResourceManager())
            {
               // this one is not the same rm as previous ones,
               // there must be at least 2
               return 2;
            }

         }
         // All RMs are the same one, one phase commit is ok.
         return 1;
      }
   }


   /**
    * Cancel the timeout.
    * This will release the lock while calling out.
    */
   private void cancelTimeout()
   {
      if (timeout != null)
      {
         unlock();
         try
         {
            timeout.cancel();
         }
         catch (Exception e)
         {
            if (log.isTraceEnabled()) { log.trace("failed to cancel timeout " + this, e); }
         }
         finally
         {
            lock();
         }
         timeout = null;
      }
   }



   /**
    * Return the resource for the given XAResource
    */
   private EnlistedXAResource findResource(XAResource xaRes)
   {
      // A linear search may seem slow, but please note that the number of XA resources registered
      // with a transaction are usually low.
      // Note: This searches backwards intentionally!  It ensures that if this resource was enlisted
      // multiple times, then the last one will be returned.  All others should be in the state
      // RS_ENDED. This allows ResourceManagers that always return false from isSameRM to be
      // enlisted and delisted multiple times.
      for (int idx = xaResources.size() - 1; idx >= 0; --idx)
      {
         EnlistedXAResource resource = (EnlistedXAResource) xaResources.get(idx);
         if (xaRes == resource.getXAResource())
         {
            return resource;
         }
      }
      return null;
   }

   private EnlistedXAResource findResourceManager(XAResource xaRes) throws XAException
   {
      for (int i = 0; i < xaResources.size(); ++i)
      {
         EnlistedXAResource resource = (EnlistedXAResource) xaResources.get(i);
         if (resource.isResourceManager(xaRes))
         {
            return resource;
         }
      }
      return null;
   }

   /**
    * Add a resource, expanding tables if needed.
    *
    * @param xaRes - The new XA resource to add. It is assumed that the resource is not already in
    *        the table of XA resources.
    * @param branchXid - The Xid for the transaction branch that is to be used for associating with
    *        this resource.
    * @param sameRMResource - The resource of the first XA resource having the same resource manager
    *        as xaRes, or null if xaRes is the first resource seen with this resource manager.
    * @return the new resource
    */
   private EnlistedXAResource addResource(XAResource xaRes, Xid branchXid,
                                          EnlistedXAResource sameRMResource)
   {
      EnlistedXAResource resource = new EnlistedXAResource(xaRes, branchXid, sameRMResource);
      xaResources.add(resource);

      // Remember the first resource that wants the last resource gambit
      if (lastResource == null && xaRes instanceof LastResource)
      {
         lastResource = resource;
      }
      return resource;
   }

   /**
    * End Tx association for all resources.
    */
   private void endResources()
   {
      for (int idx = 0; idx < xaResources.size(); ++idx)
      {
         EnlistedXAResource resource = (EnlistedXAResource) xaResources.get(idx);
         try
         {
            resource.endResource();
         }
         catch (XAException xae)
         {
            logXAException(xae);
            status = Status.STATUS_MARKED_ROLLBACK;
            cause = xae;
         }
      }
      resourcesEnded = true; // Too late to enlist new resources.
   }

   private void logXAException(XAException xae)
   {
      log.warn("XAException: tx=" + toString(), xae);
   }

   /**
    * Check we have no outstanding work
    *
    * @throws IllegalStateException when there is still work
    */
   private void checkWork()
   {
      if (work != null)
      {
         throw new IllegalStateException("Work still outstanding " + work + " tx=" + this);
      }
   }


   // Inner classes -------------------------------------------------

   /**
    * Represents a resource enlisted in the transaction. The resource can be either an
    * XA resource or a remote DTM/OTS resource.
    */
   private interface EnlistedResource
   {
      public void forget();
   }

   /**
    * Represents an XA resource enlisted in the transaction
    */
   private class EnlistedXAResource implements EnlistedResource
   {
      // The XAResource
      private XAResource xaResource;

      // The state of the resources
      private int resourceState;

      // The related XA resource from the same resource manager
      private EnlistedXAResource resourceSameRM;

      // The Xid of this resource
      private Xid resourceXid;

      /**
       * Create a new resource
       */
      public EnlistedXAResource(XAResource xaResource, Xid resourceXid,
                                EnlistedXAResource resourceSameRM)
      {
         this.xaResource = xaResource;
         this.resourceXid = resourceXid;
         this.resourceSameRM = resourceSameRM;
         resourceState = RS_NEW;
      }

      /**
       * Get the XAResource for this resource
       */
      public XAResource getXAResource()
      {
         return xaResource;
      }

      /**
       * Get the Xid for this resource
       */
      public Xid getXid()
      {
         return resourceXid;
      }

      /**
       * Is the resource enlisted?
       */
      public boolean isEnlisted()
      {
         return resourceState == RS_ENLISTED;
      }

      /**
       * Is this a resource manager
       */
      public boolean isResourceManager()
      {
         return resourceSameRM == null;
      }

      /**
       * Is this the resource manager for the passed XA resource
       */
      public boolean isResourceManager(XAResource xaRes) throws XAException
      {
         return resourceSameRM == null && xaRes.isSameRM(xaResource);
      }

      /**
       * Is the resource delisted and the XAResource always returns false
       * for isSameRM
       */
      public boolean isDelisted(XAResource xaRes) throws XAException
      {
         return resourceState == RS_ENDED && xaResource.isSameRM(xaRes) == false;
      }

      /**
       * Call <code>start()</code> on a XAResource and update
       * internal state information.
       * This will release the lock while calling out.
       *
       * @return true when started, false otherwise
       */
      public boolean startResource() throws XAException
      {
         int flags = XAResource.TMJOIN;

         if (resourceSameRM == null)
         {
            switch (resourceState)
            {
            case RS_NEW:
               flags = XAResource.TMNOFLAGS;
               break;
            case RS_SUSPENDED:
               flags = XAResource.TMRESUME;
               break;
            default:
               if (log.isTraceEnabled()) { log.trace("Unhandled resource state: " + resourceState + " (not RS_NEW or RS_SUSPENDED, using TMJOIN flags)"); }
            }
         }

         if (log.isTraceEnabled()) { log.trace("startResource(" + xidFactory.toString(resourceXid) + ") entered: " + xaResource.toString() + " flags=" + flags); }

         unlock();
         // OSH FIXME: resourceState could be incorrect during this callout.
         try
         {
            try
            {
               xaResource.start(resourceXid, flags);
            }
            catch (XAException e)
            {
               throw e;
            }
            catch (Throwable t)
            {
               if (log.isTraceEnabled()) { log.trace("unhandled throwable error in startResource", t); }
               status = Status.STATUS_MARKED_ROLLBACK;
               return false;
            }

            // Now the XA resource is associated with a transaction.
            resourceState = RS_ENLISTED;
         }
         finally
         {
            lock();
            if (log.isTraceEnabled()) { log.trace("startResource(" + xidFactory.toString(resourceXid) + ") leaving: " + xaResource.toString() + " flags=" + flags); }
         }
         return true;
      }

      /**
       * Delist the resource unless we already did it
       */
      public boolean delistResource(XAResource xaRes, int flag)
            throws XAException
      {
         if (isDelisted(xaRes))
         {
            // This RM always returns false on isSameRM.  Further,
            // the last resource has already been delisted.
            log.warn("Resource already delisted.  tx=" + this.toString());
            return false;
         }
         endResource(flag);
         return true;
      }

      /**
       * End the resource
       */
      public void endResource()
            throws XAException
      {
         if (resourceState == RS_ENLISTED || resourceState == RS_SUSPENDED)
         {
            if (log.isTraceEnabled()) { log.trace("endresources(" + xaResource + "): state=" + resourceState); }
            endResource(XAResource.TMSUCCESS);
         }
      }

      /**
       * Call <code>end()</code> on the XAResource and update internal state information.
       * This will release the lock while calling out.
       *
       * @param flag The flag argument for the end() call.
       */
      private void endResource(int flag) throws XAException
      {
         if (log.isTraceEnabled()) { log.trace("endResource(" + xidFactory.toString(resourceXid) + ") entered: " + xaResource.toString() + " flag=" + flag); }

         unlock();
         // OSH FIXME: resourceState could be incorrect during this callout.
         try
         {
            try
            {
               xaResource.end(resourceXid, flag);
            }
            catch (XAException e)
            {
               throw e;
            }
            catch (Throwable t)
            {
               if (log.isTraceEnabled()) { log.trace("unhandled throwable error in endResource", t); }
               status = Status.STATUS_MARKED_ROLLBACK;
               // Resource may or may not be ended after illegal exception. We just assume it ended.
               resourceState = RS_ENDED;
               return;
            }

            // Update our internal state information
            if (flag == XAResource.TMSUSPEND)
            {
               resourceState = RS_SUSPENDED;
            }
            else
            {
               if (flag == XAResource.TMFAIL)
               {
                  status = Status.STATUS_MARKED_ROLLBACK;
               }
               resourceState = RS_ENDED;
            }
         }
         finally
         {
            lock();
            if (log.isTraceEnabled()) { log.trace("endResource(" + xidFactory.toString(resourceXid) + ") leaving: " + xaResource.toString() + " flag=" + flag); }
         }
      }

      /**
       * Forget the resource
       */
      public void forget()
      {
         unlock();
         if (log.isTraceEnabled()) { log.trace("Forget: " + xaResource + " xid=" + xidFactory.toString(resourceXid)); }
         try
         {
            xaResource.forget(resourceXid);
         }
         catch (XAException xae)
         {
            logXAException(xae);
            cause = xae;
         }
         finally
         {
            lock();
         }
         resourceState = RS_FORGOT;
      }

      /**
       * Prepare the resource
       */
      public int prepare() throws XAException
      {
         int vote;
         unlock();
         if (log.isTraceEnabled()) { log.trace("Prepare: " + xaResource + " xid=" + xidFactory.toString(resourceXid)); }
         try
         {
            vote = xaResource.prepare(resourceXid);
         }
         finally
         {
            lock();
         }

         if (vote == XAResource.XA_OK)
         {
            resourceState = RS_VOTE_OK;
         }
         else if (vote == XAResource.XA_RDONLY)
         {
            resourceState = RS_VOTE_READONLY;
         }
         return resourceState;
      }

      /**
       * Prepare the last resource
       */
      public void prepareLastResource() throws XAException
      {
         resourceState = RS_VOTE_OK;
      }

      /**
       * Commit the resource
       */
      public void commit(boolean onePhase) throws XAException
      {
         if (!onePhase && resourceState != RS_VOTE_OK)
         {
            return; // Voted read-only at prepare phase.
         }

         if (resourceSameRM != null)
         {
            return; // This RM already committed.
         }

         unlock();
         if (log.isTraceEnabled()) { log.trace("Commit: " + xaResource + " xid=" + xidFactory.toString(resourceXid) + " onePhase=" + onePhase); }
         try
         {
            xaResource.commit(resourceXid, onePhase);
         }
         finally
         {
            lock();
         }
      }

      /**
       * Rollback the resource
       */
      public void rollback() throws XAException
      {
         if (resourceState == RS_VOTE_READONLY)
         {
            return;
         }
         // Already forgotten
         if (resourceState == RS_FORGOT)
         {
            return;
         }
         if (resourceSameRM != null)
         {
            return; // This RM already rolled back.
         }

         unlock();
         if (log.isTraceEnabled()) { log.trace("Rollback: " + xaResource + " xid=" + xidFactory.toString(resourceXid)); }
         try
         {
            xaResource.rollback(resourceXid);
         }
         finally
         {
            lock();
         }
      }
   }


   /**
    * Represents a remote resource enlisted in the transaction.
    */
   private class EnlistedRemoteResource implements EnlistedResource
   {
      /**
       * The remote resource.
       */
      private Resource remoteResource;

      /**
       * The state of the resource.
       */
      private int resourceState;

      /**
       * Creates a new <code>EnlistedRemoteResource</code>.
       */
      public EnlistedRemoteResource(Resource remoteResource)
      {
         this.remoteResource = remoteResource;
         resourceState = RS_NEW;
      }

      /**
       * Gets the remote resource represented by this
       * <code>EnlistedRemoteResource</code> instance.
       */
      public Resource getRemoteResource()
      {
         return remoteResource;
      }

      /**
       * Prepare the resource
       */
      public int prepare() throws RemoteException,
                                  TransactionAlreadyPreparedException,
                                  HeuristicMixedException,
                                  HeuristicHazardException
      {
         Vote vote = null;
         unlock();
         try
         {
            vote = remoteResource.prepare();
         }
         finally
         {
            lock();
         }

         if (vote == Vote.COMMIT)
         {
            resourceState = RS_VOTE_OK;
         }
         else if (vote == Vote.READONLY)
         {
            resourceState = RS_VOTE_READONLY;
         }

         return resourceState;
      }

      /**
       * Commit the resource
       * @throws RemoteException
       * @throws HeuristicHazardException
       * @throws HeuristicMixedException
       * @throws HeuristicRollbackException
       * @throws TransactionNotPreparedException
       */
      public void commit(boolean onePhase)
            throws RemoteException,
                   TransactionNotPreparedException,
                   HeuristicRollbackException,
                   HeuristicMixedException,
                   HeuristicHazardException
      {
         if (log.isTraceEnabled()) { log.trace("Committing resource " + remoteResource + " state=" + resourceState); }

         if (!onePhase && resourceState != RS_VOTE_OK)
         {
            return; // Voted read-only at prepare phase.
         }

         unlock();
         try
         {
            if (onePhase)
            {
               remoteResource.commitOnePhase();
            }
            else
            {
               remoteResource.commit();
            }
         }
         finally
         {
            lock();
         }
      }

      /**
       * Rollback the resource
       */
      public void rollback()
            throws RemoteException,
                   HeuristicCommitException,
                   HeuristicMixedException,
                   HeuristicHazardException
      {
         if (resourceState == RS_VOTE_READONLY)
         {
            return;
         }
         // Already forgotten
         if (resourceState == RS_FORGOT)
         {
            return;
         }

         unlock();
         try
         {
            remoteResource.rollback();
         }
         finally
         {
            lock();
         }
      }

      /**
       * Forget the resource
       */
      public void forget()
      {
         unlock();
         if (log.isTraceEnabled()) { log.trace("Forget: " + remoteResource); }
         try
         {
            remoteResource.forget();
         }
         catch (RemoteException e)
         {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
         finally
         {
            lock();
         }
         resourceState = RS_FORGOT;
      }
   }
}
