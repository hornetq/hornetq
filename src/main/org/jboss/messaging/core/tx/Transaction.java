/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.tx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.PersistenceManager;


/**
 * 
 * A JMS Server local transaction
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * partially based on org.jboss.mq.pm.Tx by
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * 
 * @version $Revision$
 */
public class Transaction
{
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected int state;
   
   protected long txID;
   
   protected List postCommitTasks;
   
   protected List postRollbackTasks;
   
   protected PersistenceManager pm;
   
   
   //FIXME These two attributes can be combined
   //protected boolean checkPersisted;
   
   //True if the transaction has resulted in a tx record being inserted in the db
   protected boolean insertedTXRecord;

   // Static --------------------------------------------------------
   
   public static final int STATE_ACTIVE = 0;
   
   public static final int STATE_PREPARED = 1;
   
   public static final int STATE_COMMITTED = 2;
   
   public static final int STATE_ROLLEDBACK = 3;
   
   // Constructors --------------------------------------------------
   
   Transaction(long id, PersistenceManager mgr)
   {
      state = STATE_ACTIVE;
      txID = id;
      pm = mgr;
      postCommitTasks = new ArrayList();
      postRollbackTasks = new ArrayList();
   }
   
   // Public --------------------------------------------------------
   
   public int getState()
   {
      return state;
   }
   
   public long getID()
   {
      return txID;
   }
   
   
   public void addPostCommitTasks(Runnable task)
   {
      postCommitTasks.add(task);
   }
   
   public void addPostRollbackTasks(Runnable task)
   {
      postRollbackTasks.add(task);
   }
   
   
   public void commit() throws Exception
   {
      //TODO - commit the tx in the database
      
      state = STATE_COMMITTED;
      
      if (insertedTXRecord)
      {
         if (pm == null)
         {
            throw new IllegalStateException("Reliable messages were handled in the transaction, but there is no persistence manager!");
         }
         pm.commitTx(this);
      }
      
      Iterator iter = postCommitTasks.iterator();
      while (iter.hasNext())
      {
         Runnable task = (Runnable)iter.next();
         task.run();
      }
   }
   
   public void prepare()
   {
      state = STATE_PREPARED;
   }
   
   public void rollback() throws Exception
   {
      //TODO - rollback the tx in the database
      
      state = STATE_ROLLEDBACK;
      
      if (insertedTXRecord)
      {
         pm.rollbackTx(this);
      }
      
      Iterator iter = postRollbackTasks.iterator();
      while (iter.hasNext())
      {
         Runnable task = (Runnable)iter.next();
         task.run();
      }
   }
   

   public boolean insertedTXRecord()
   { 
      boolean inserted = insertedTXRecord;
      insertedTXRecord = true;
      return inserted;
   }
   
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}


