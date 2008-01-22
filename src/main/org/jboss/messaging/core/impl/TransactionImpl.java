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
package org.jboss.messaging.core.impl;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.Transaction;
import org.jboss.messaging.core.TransactionSynchronization;
import org.jboss.messaging.util.Logger;

/**
 * 
 * A TransactionImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TransactionImpl implements Transaction
{
   private static final Logger log = Logger.getLogger(TransactionImpl.class);
   
   private List<Message> messagesToAdd;
   
   private List<MessageReference> refsToRemove;
   
   private List<TransactionSynchronization> synchronizations = new ArrayList<TransactionSynchronization>();
   
   private Xid xid;
   
   private boolean containsPersistent;
   
   private boolean prepared;
   
   //FIXME - temp
   public TransactionImpl()
   {
      messagesToAdd = new ArrayList<Message>();
      
      refsToRemove = new ArrayList<MessageReference>();            
   }
   
   public TransactionImpl(Xid xid)
   {
      this();
      
      this.xid = xid;      
   }
   
   public void setContainsPersistent(boolean persistent)
   {
      this.containsPersistent = persistent;
   }
   
   public TransactionImpl(List<Message> messagesToAdd, List<MessageReference> refsToRemove,
                          boolean containsPersistent)
   {
      this.messagesToAdd = messagesToAdd;
      
      this.refsToRemove = refsToRemove;
      
      this.containsPersistent = containsPersistent;
   }
   
   public TransactionImpl(Xid xid, List<Message> messagesToAdd, List<MessageReference> refsToRemove,
                          boolean containsPersistent)
   {
      this(messagesToAdd, refsToRemove, containsPersistent);
      
      this.xid = xid;
   }
   
   // Transaction implementation -----------------------------------------------------------
   
   //FIXME temp
   
   
   public void addAllSends(List<Message> msgs)
   {
      messagesToAdd.addAll(msgs);
   }
   
   public void addAllAcks(List<MessageReference> refs)
   {
      refsToRemove.addAll(refs);
   }
   
   //End FIXME
   
   public void addSynchronization(TransactionSynchronization sync)
   {
      synchronizations.add(sync);
   }
   
   public void prepare(PersistenceManager persistenceManager) throws Exception
   {
      if (xid == null)
      {
         throw new IllegalStateException("Cannot call prepare() on a non XA transaction");
      }
      else if (containsPersistent)
      {
         persistenceManager.prepareTransaction(xid, messagesToAdd, refsToRemove);
         
         prepared = true;
      }
   }
   
   public void commit(PersistenceManager persistenceManager) throws Exception
   {
      callSynchronizations(SyncType.BEFORE_COMMIT);
            
      if (containsPersistent)
      {
         if (xid == null)
         {
            //1PC commit
            
            persistenceManager.commitTransaction(messagesToAdd, refsToRemove);
         }
         else
         {
            //2PC commit
            
            if (!prepared)
            {
               throw new IllegalStateException("Transaction is not prepared");
            }
            
            persistenceManager.commitPreparedTransaction(xid);                        
         } 
      }
            
      //Now add to queue(s)
      
      for (Message msg: messagesToAdd)
      {
         for (MessageReference ref: msg.getReferences())
         {
            ref.getQueue().addLast(ref);
         }
      }
      
      //And acknowledge
      
      //TODO find better transactional abstraction so don't have to manually do this
      
      for (MessageReference ref: refsToRemove)
      {
         ref.getQueue().referenceAcknowledged();
      }
      
      callSynchronizations(SyncType.AFTER_COMMIT);
   }
   
   public void rollback(PersistenceManager persistenceManager) throws Exception
   {
      callSynchronizations(SyncType.BEFORE_ROLLBACK);
      
      if (xid == null)
      {
         //1PC rollback - nothing to do
      }
      else
      {
         persistenceManager.unprepareTransaction(xid, messagesToAdd, refsToRemove);
         
         //Now we need to add the refs back on the tx
         
         for (MessageReference ref: refsToRemove)
         {
            if (ref.getMessage().isDurable() && ref.getQueue().isDurable())
            {
               ref.getMessage().addBackDurableReference(ref);
            }
         }
      }
      
      callSynchronizations(SyncType.AFTER_ROLLBACK);      
   }
   
   // Private -------------------------------------------------------------------
   
   private void callSynchronizations(SyncType type) throws Exception
   {
      for (TransactionSynchronization sync: synchronizations)
      {
         if (type == SyncType.BEFORE_COMMIT)
         {
            sync.beforeCommit();
         }
         else if (type == SyncType.AFTER_COMMIT)
         {
            sync.afterCommit();
         }
         else if (type == SyncType.BEFORE_ROLLBACK)
         {
            sync.beforeRollback();
         }
         else if (type == SyncType.AFTER_ROLLBACK)
         {
            sync.afterRollback();
         }            
      }
   }
   
   // Inner Enums -------------------------------------------------------------------------------
   
   private enum SyncType
   {
      BEFORE_COMMIT, AFTER_COMMIT, BEFORE_ROLLBACK, AFTER_ROLLBACK;
   }
         
}
