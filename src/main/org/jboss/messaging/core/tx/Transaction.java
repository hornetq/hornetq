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
   
   protected List keyedCallbacks;
   
   protected Map keyedCallbackMap;
   
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
   
   Transaction(long id)
   {
      this.id = id;
      state = STATE_ACTIVE;
      callbacks = new ArrayList();
      keyedCallbacks = new ArrayList();
      keyedCallbackMap = new HashMap();
   }
   
   Transaction(long id, Xid xid)
   {
      this(id);
      this.xid = xid;
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
      
   public void addCallback(TxCallback callback)
   {
      callbacks.add(callback);
   } 
   
   public void addKeyedCallback(TxCallback callback, Object key)
   {
      keyedCallbacks.add(callback);
      
      keyedCallbackMap.put(key, callback);
   } 
   
   public TxCallback getKeyedCallback(Object key)
   {
      return (TxCallback)keyedCallbackMap.get(key);
   }
      
   public void commit() throws Exception
   {
      if (state == STATE_ROLLBACK_ONLY)
      {
         throw new TransactionException("Transaction marked rollback only, cannot commit");
      }

      if (trace) { log.trace("executing before commit hooks " + this); }
       
      boolean onePhase = state != STATE_PREPARED;
      
      List cb = new ArrayList(callbacks);
      cb.addAll(keyedCallbacks);
      
      Iterator iter = cb.iterator();
      
      while (iter.hasNext())
      {
         TxCallback callback = (TxCallback)iter.next();
         
         callback.beforeCommit(onePhase);
      }
      
      state = STATE_COMMITTED;
      
      if (trace) { log.trace("committed " + this); }
      
      iter = cb.iterator();
      
      if (trace) { log.trace("executing after commit hooks " + this); }
      
      while (iter.hasNext())
      {
         TxCallback callback = (TxCallback)iter.next();
         
         callback.afterCommit(onePhase);
      }
      
      callbacks = null;
      
      keyedCallbacks = null;
      
      keyedCallbackMap = null;      
      
      if (trace) { log.trace("commit process complete " + this); }
   }
   
   public void prepare() throws Exception
   {
      if (trace) { log.trace("executing before prepare hooks " + this); }
      
      List cb = new ArrayList(callbacks);
      cb.addAll(keyedCallbacks);
      
      Iterator iter = cb.iterator();
      
      while (iter.hasNext())
      {
         TxCallback callback = (TxCallback)iter.next();
         
         callback.beforePrepare();
      }
      
      state = STATE_PREPARED;
      
      if (trace) { log.trace("prepared " + this); }
      
      iter = cb.iterator();
      
      if (trace) { log.trace("executing after prepare hooks " + this); }
      
      while (iter.hasNext())
      {
         TxCallback callback = (TxCallback)iter.next();
         
         callback.afterPrepare();
      }            
      
      if (trace) { log.trace("prepare process complete " + this); }
   }
   
   public void rollback() throws Exception
   {
      if (trace) { log.trace("executing before rollback hooks " + this); }
      
      boolean onePhase = state != STATE_PREPARED;
      
      List cb = new ArrayList(callbacks);
      cb.addAll(keyedCallbacks);
      
      Iterator iter = cb.iterator();
      
      while (iter.hasNext())
      {
         TxCallback callback = (TxCallback)iter.next();
         
         callback.beforeRollback(onePhase);
      }
      
      state = STATE_ROLLEDBACK;
      
      if (trace) { log.trace("rolled back " + this); }
      
      iter = cb.iterator();
      
      if (trace) { log.trace("executing after prepare hooks " + this); }
      
      while (iter.hasNext())
      {
         TxCallback callback = (TxCallback)iter.next();
         
         callback.afterRollback(onePhase);
      }            
      
      callbacks = null;
      
      keyedCallbacks = null;
      
      keyedCallbackMap = null;
      
      if (trace) { log.trace("rollback process complete " + this); }
   }

   public void setRollbackOnly() throws Exception
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


