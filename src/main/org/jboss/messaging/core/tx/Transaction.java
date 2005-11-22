/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.messaging.core.tx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.PersistenceManager;
import org.jboss.logging.Logger;


/**
 * 
 * A JMS Server local transaction
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.com">Ovidiu Feodorov</a>
 * 
 * partially based on org.jboss.mq.pm.Tx by
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * 
 * @version $Revision 1.1$
 */
public class Transaction
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Transaction.class);

   // Attributes ----------------------------------------------------
   
   protected long longTxID;
   
   protected String guidTxID;
   
   protected int state;
   
   protected Xid xid;
   
   protected List postCommitTasks;
   
   protected List postRollbackTasks;
   
   protected PersistenceManager pm;
   
   //True if the transaction has resulted in a tx record being inserted in the db
   protected boolean insertedTXRecord;

   // Static --------------------------------------------------------
   
   public static final int STATE_ACTIVE = 0;
   
   public static final int STATE_PREPARED = 1;
   
   public static final int STATE_COMMITTED = 2;
   
   public static final int STATE_ROLLEDBACK = 3;

   public static final int STATE_ROLLBACK_ONLY = 4;

   public String stateToString(int state)
   {
      if (state == STATE_ACTIVE)
      {
         return "ACTIVE";
      }
      else if (state == STATE_PREPARED)
      {
         return "PREPARED";
      }
      else if (state == STATE_COMMITTED)
      {
         return "COMMITTED";
      }
      else if (state == STATE_ROLLEDBACK)
      {
         return "ROLLEDBACK";
      }
      else if (state == STATE_ROLLBACK_ONLY)
      {
         return "ROLLBACK_ONLY";
      }
      else
      {
         return "UNKNOWN";
      }
   }

   // Constructors --------------------------------------------------
   
   Transaction(Xid xid, PersistenceManager mgr)
   {
      state = STATE_ACTIVE;
      this.xid = xid;
      pm = mgr;
      postCommitTasks = new ArrayList();
      postRollbackTasks = new ArrayList();
   }
   
   // Public --------------------------------------------------------
   
   public int getState()
   {
      return state;
   }      
   
   public Xid getXid()
   {
      return xid;
   }
      
   public void addPostCommitTask(Runnable task)
   {
      postCommitTasks.add(task);
   }
   
   public void addPostRollbackTask(Runnable task)
   {
      postRollbackTasks.add(task);
   }
      
   public void commit() throws Exception
   {
      if (state == STATE_ROLLBACK_ONLY)
      {
         throw new TransactionException("Transaction marked rollback only, cannot commit");
      }

      if (log.isTraceEnabled()) { log.trace("committing " + this); }

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

      if (log.isTraceEnabled()) { log.trace("rolling back " + this); }

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

   public void setRollbackOnly() throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("setting rollback_only on " + this); }

      state = STATE_ROLLBACK_ONLY;
   }


   public boolean insertedTXRecord()
   { 
      boolean inserted = insertedTXRecord;
      insertedTXRecord = true;
      return inserted;
   }
   
   public void setGuidTxID(String guid)
   {
      this.guidTxID = guid;
   }
   
   public String getGuidTxId()
   {
      return guidTxID;
   }
   
   public long getLongTxId()
   {
      return longTxID;
   }
   
   public void setLongTxId(long id)
   {
      this.longTxID = id;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("TX(");
      if (this.guidTxID != null)
      {
         sb.append(this.guidTxID);
      }
      else
      {
         sb.append(this.longTxID);
      }
      sb.append("):");
      sb.append(stateToString(state));
      return sb.toString();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}


