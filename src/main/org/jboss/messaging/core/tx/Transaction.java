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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.jboss.logging.Logger;

/**
 * 
 * A JMS Server local transaction
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.com">Ovidiu Feodorov</a>
 * 
 * @version $Revision 1.1$
 *
 * $Id$
 */
public class Transaction
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Transaction.class);

   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
     
   protected long id;
   
   protected int state;
   
   protected Xid xid;
   
   protected List callbacks;
   
   protected Map callbackMap;
      
   
   /**
    * If this is a XA transaction, when a commit is executed the transaction has to be removed from the transaction repository.
    * This reference will guarantee the reference back to the repository where the transaction was created
    *
   */
   protected TransactionRepository repository;
   
   //A special first callback that is ensured to be executed first
   protected TxCallback firstCallback;
   
   // Static --------------------------------------------------------
   
   public static final int STATE_ACTIVE = 0;
   
   public static final int STATE_PREPARED = 1;
   
   public static final int STATE_COMMITTED = 2;
   
   public static final int STATE_ROLLEDBACK = 3;

   public static final int STATE_ROLLBACK_ONLY = 4;

   public static String stateToString(int state)
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
   
   Transaction(long id)
   {
      this.id = id;
      state = STATE_ACTIVE;
      callbacks = new ArrayList();
      callbackMap = new HashMap();
   }
   
   Transaction(long id, Xid xid, TransactionRepository tr)
   {
      this(id);
      this.xid = xid;
      this.repository = tr;
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

   public void addCallback(TxCallback callback, Object key)
   {            
      callbacks.add(callback);
      
      callbackMap.put(key, callback);
   } 
   
   public void addFirstCallback(TxCallback callback, Object key)
   {            
      if (firstCallback != null)
      {
         throw new IllegalStateException("There is already a first callback");
      }
      
      this.firstCallback = callback;
      
      callbackMap.put(key, callback);
   }
   
   public TxCallback getCallback(Object key)
   {
      return (TxCallback)callbackMap.get(key);
   }
      
   public synchronized void commit() throws Exception
   {
      if (state == STATE_ROLLBACK_ONLY)
      {
         throw new TransactionException("Transaction marked rollback only, cannot commit");
      }
      if (state == STATE_COMMITTED)
      {
         throw new TransactionException("Transaction already committed, cannot commit");
      }
      if (state == STATE_ROLLEDBACK)
      {
         throw new TransactionException("Transaction already rolled back, cannot commit");
      }

      if (trace) { log.trace("executing before commit hooks " + this); }
       
      boolean onePhase = state != STATE_PREPARED;
      
      if (firstCallback != null)
      {
         firstCallback.beforeCommit(onePhase);
      }
      
      Iterator iter = callbacks.iterator();
      
      while (iter.hasNext())
      {
         TxCallback callback = (TxCallback)iter.next();
         
         callback.beforeCommit(onePhase);
      }
      
      state = STATE_COMMITTED;
      
      if (trace) { log.trace("committed " + this); }
      
      iter = callbacks.iterator();
      
      if (trace) { log.trace("executing after commit hooks " + this); }
      
      if (firstCallback != null)
      {         
         firstCallback.afterCommit(onePhase);
      }
      
      while (iter.hasNext())
      {
         TxCallback callback = (TxCallback)iter.next();
         
         callback.afterCommit(onePhase);
      }
          
      callbacks = null;
      
      callbackMap = null;      
      
      firstCallback = null;
      
      if (repository !=null)
      {
         repository.deleteTransaction(this);
      }
            
      if (trace) { log.trace("commit process complete " + this); }            
   }
   
   public synchronized void prepare() throws Exception
   {
      if (state != STATE_ACTIVE)
      {
         throw new TransactionException("Transaction not active, cannot prepare");
      }
      
      if (trace) { log.trace("executing before prepare hooks " + this); }
      
      if (firstCallback != null)
      {
         firstCallback.beforePrepare();
      }
      
      Iterator iter = callbacks.iterator();
      
      while (iter.hasNext())
      {
         TxCallback callback = (TxCallback)iter.next();
         
         callback.beforePrepare();
      }
      
      state = STATE_PREPARED;
      
      if (trace) { log.trace("prepared " + this); }
      
      if (firstCallback != null)
      {
         firstCallback.afterPrepare();
      }
      
      iter = callbacks.iterator();
      
      if (trace) { log.trace("executing after prepare hooks " + this); }
      
      while (iter.hasNext())
      {
         TxCallback callback = (TxCallback)iter.next();
         
         callback.afterPrepare();
      }            
      
      if (trace) { log.trace("prepare process complete " + this); }
   }
   
   public synchronized void rollback() throws Exception
   {
      if (state == STATE_COMMITTED)
      {
         throw new TransactionException("Transaction already committed, cannot rollback");
      }
      if (state == STATE_ROLLEDBACK)
      {
         throw new TransactionException("Transaction already rolled back, cannot rollback");
      }
      
      if (trace) { log.trace("executing before rollback hooks " + this); }
      
      boolean onePhase = state != STATE_PREPARED;
      
      if (firstCallback != null)
      {
         firstCallback.beforeRollback(onePhase);
      }

      for(Iterator i = callbacks.iterator(); i.hasNext(); )
      {
         TxCallback callback = (TxCallback)i.next();
         callback.beforeRollback(onePhase);
      }
      
      state = STATE_ROLLEDBACK;
      
      if (trace) { log.trace("rolled back " + this); }

      if (trace) { log.trace("executing after prepare hooks " + this); }

      if (firstCallback != null)
      {
         firstCallback.afterRollback(onePhase);
      }
      
      for(Iterator i = callbacks.iterator(); i.hasNext();)
      {
         TxCallback callback = (TxCallback)i.next();
         callback.afterRollback(onePhase);
      }            
      
      callbacks = null;
      callbackMap = null;
      
      if (repository != null)
      {
         repository.deleteTransaction(this);
      }
      
      if (trace) { log.trace("rollback process complete " + this); }
   }

   public synchronized void setRollbackOnly() throws Exception
   {
      if (trace) { log.trace("setting rollback_only on " + this); }

      state = STATE_ROLLBACK_ONLY;
   }
   
   public long getId()
   {
      return id;
   }
      
   public String toString()
   {
      StringBuffer sb = new StringBuffer("TX(");
      sb.append(id);
      sb.append("):");
      sb.append(stateToString(state));
      return sb.toString();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}


