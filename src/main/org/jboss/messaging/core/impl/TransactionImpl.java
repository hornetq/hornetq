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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.Queue;
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
   
   private List<Message> messagesToAdd = new ArrayList<Message>();
   
   private List<MessageReference> acknowledgements = new ArrayList<MessageReference>();  
   
   private List<TransactionSynchronization> synchronizations = new ArrayList<TransactionSynchronization>();
   
   private Xid xid;
   
   private boolean containsPersistent;
   
   private volatile boolean prepared;
   
   private volatile boolean suspended;
   
   public TransactionImpl()
   {            
   }
   
   public TransactionImpl(Xid xid)
   {
      this.xid = xid;      
   }
   
   // Transaction implementation -----------------------------------------------------------
   
   public void addMessage(Message message)
   {
      messagesToAdd.add(message);
      
      if (message.getNumDurableReferences() != 0)
      {
         containsPersistent = true;
      }
   }
   
   public void addAcknowledgement(MessageReference acknowledgement)
   {
      acknowledgements.add(acknowledgement);
       
      if (acknowledgement.getMessage().isDurable() && acknowledgement.getQueue().isDurable())
      {
         containsPersistent = true;
      }
   }
      
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
         persistenceManager.prepareTransaction(xid, messagesToAdd, acknowledgements);
      }
            
      prepared = true;
   }
   
   public void commit(boolean onePhase, PersistenceManager persistenceManager) throws Exception
   {
      callSynchronizations(SyncType.BEFORE_COMMIT);
      
      if (containsPersistent)
      {
         if (xid == null || onePhase)
         {
            //1PC commit
            
            persistenceManager.commitTransaction(messagesToAdd, acknowledgements);
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
            
      for (Message msg: messagesToAdd)
      {
         msg.send();
      }
      
      for (MessageReference reference: acknowledgements)
      {
         reference.getQueue().decrementDeliveringCount();
      }
      
      callSynchronizations(SyncType.AFTER_COMMIT);
      
      clear();      
   }
   
   public void rollback(PersistenceManager persistenceManager) throws Exception
   {
      callSynchronizations(SyncType.BEFORE_ROLLBACK);
        
      if (prepared)
      {
         persistenceManager.unprepareTransaction(xid, messagesToAdd, acknowledgements);             
      }
      
      cancelDeliveries(persistenceManager);
                        
      callSynchronizations(SyncType.AFTER_ROLLBACK);  
      
      clear();      
   }      
   
   public int getAcknowledgementsCount()
   {
      return acknowledgements.size();
   }
   
   public void suspend()
   {
      suspended = true;
   }
   
   public void resume()
   {
      suspended = false;
   }
   
   public boolean isSuspended()
   {
      return suspended;
   }
   
   public Xid getXid()
   {
      return xid;
   }
   
   public boolean isEmpty()
   {
      return messagesToAdd.isEmpty() && acknowledgements.isEmpty();
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

   private void clear()
   {
      messagesToAdd.clear();
      
      acknowledgements.clear();
      
      synchronizations.clear();
      
      containsPersistent = false;
   }
   
   private void cancelDeliveries(PersistenceManager persistenceManager) throws Exception
   {
      Map<Queue, LinkedList<MessageReference>> queueMap = new HashMap<Queue, LinkedList<MessageReference>>();
      
      //Need to sort into lists - one for each queue involved.
      //Then cancelling back atomicly for each queue adding list on front to guarantee ordering is preserved      
      
      for (MessageReference ref: acknowledgements)
      {
         Queue queue = ref.getQueue();
         
         LinkedList<MessageReference> list = queueMap.get(queue);
         
         if (list == null)
         {
            list = new LinkedList<MessageReference>();
            
            queueMap.put(queue, list);
         }
                 
         if (ref.cancel(persistenceManager))
         {
            list.add(ref);
         }
      }
      
      for (Map.Entry<Queue, LinkedList<MessageReference>> entry: queueMap.entrySet())
      {                  
         LinkedList<MessageReference> refs = entry.getValue();
                
         entry.getKey().addListFirst(refs);
      }
   }
   
   // Inner Enums -------------------------------------------------------------------------------
   
   private enum SyncType
   {
      BEFORE_COMMIT, AFTER_COMMIT, BEFORE_ROLLBACK, AFTER_ROLLBACK;
   }
         
}
