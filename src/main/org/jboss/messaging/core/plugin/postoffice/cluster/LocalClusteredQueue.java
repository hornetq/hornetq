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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.util.Future;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A LocalClusteredQueue
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class LocalClusteredQueue extends PagingFilteredQueue implements ClusteredQueue
{
   private static final Logger log = Logger.getLogger(LocalClusteredQueue.class);
   
   private boolean trace = log.isTraceEnabled();
      
   private PostOfficeInternal office;
   
   private volatile int lastCount;
   
   private volatile RemoteQueueStub pullQueue;
   
   private int nodeId;
   
   private TransactionRepository tr;
   
   //TODO - we shouldn't have to specify office AND nodeId
   public LocalClusteredQueue(PostOffice office, int nodeId, String name, long id, MessageStore ms, PersistenceManager pm,             
                              boolean acceptReliableMessages, boolean recoverable, QueuedExecutor executor,
                              Filter filter, TransactionRepository tr,
                              int fullSize, int pageSize, int downCacheSize)
   {
      super(name, id, ms, pm, acceptReliableMessages, recoverable, executor, filter, fullSize, pageSize, downCacheSize);
     
      this.nodeId = nodeId;
      
      this.tr = tr;
      
      //TODO - This cast is potentially unsafe - handle better
      this.office = (PostOfficeInternal)office;
   }
   
   public LocalClusteredQueue(PostOffice office, int nodeId, String name, long id, MessageStore ms, PersistenceManager pm,             
                              boolean acceptReliableMessages, boolean recoverable, QueuedExecutor executor,
                              Filter filter, TransactionRepository tr)
   {
      super(name, id, ms, pm, acceptReliableMessages, recoverable, executor, filter);
      
      this.nodeId = nodeId;
      
      this.tr = tr;
      
      //TODO - This cast is potentially unsafe - handle better
      this.office = (PostOfficeInternal)office;
   }
   
   public void setPullQueue(RemoteQueueStub queue)
   {
      this.pullQueue = queue;
   }
   
   public RemoteQueueStub getPullQueue()
   {
      return pullQueue;
   }
      
   public QueueStats getStats()
   {      
      //Currently we only return the current message reference count for the channel
      //Note we are only interested in the number of refs in the main queue, not
      //in any deliveries
      //Also we are only interested in the value obtained after delivery is complete.
      //This is so we don't end up with transient values since delivery is half way through
      
      int cnt = getRefCount();
      
      if (cnt != lastCount)
      {
         lastCount = cnt;
         
         //We only return stats if it has changed since last time - this is so when we only
         //broadcast data when necessary
         return new QueueStats(name, cnt);
      }
      else
      {
         return null;
      } 
   }      
    
   public boolean isLocal()
   {
      return true;
   }
     
   public int getNodeId()
   {
      return nodeId;
   }
   
   /*
    * This is the same as the normal handle() method on the Channel except it doesn't
    * persist the message even if it is persistent - this is because persistent messages
    * are always persisted on the sending node before sending.
    */
   public Delivery handleFromCluster(MessageReference ref)
      throws Exception
   {
      if (trace) { log.trace("Handling ref from cluster: " + ref); }
      
      if (filter != null && !filter.accept(ref))
      {
         Delivery del = new SimpleDelivery(this, ref, true, false);
         
         if (trace) { log.trace("Reference " + ref + " rejected by filter"); }
         
         return del;
      }
      
      checkClosed();
      
      Future result = new Future();
      
      // Instead of executing directly, we add the handle request to the event queue.
      // Since remoting doesn't currently handle non blocking IO, we still have to wait for the
      // result, but when remoting does, we can use a full SEDA approach and get even better
      // throughput.
      this.executor.execute(new HandleRunnable(result, null, ref, false));

      return (Delivery)result.getResult();
   }
   
   public void acknowledgeFromCluster(Delivery d) throws Throwable
   {
      acknowledgeInternal(d, null, false, false);      
   }
   
   public void handlePullMessagesResult(RemoteQueueStub remoteQueue, List messages,
                                        long holdingTxId, boolean failBeforeCommit, boolean failAfterCommit) throws Exception
   { 
      //This needs to be run on a different thread to the one used by JGroups to deliver the message
      //to avoid deadlock
      Runnable runnable = new MessagePullResultRunnable(remoteQueue, messages, holdingTxId,
                                                        failBeforeCommit, failAfterCommit);
      
      executor.execute(runnable);      
   }
   
   //TODO it's not ideal that we need to pass in a PullMessagesRequest
   public void handleGetDeliveriesRequest(int returnNodeId, int number, TransactionId txId, PullMessagesRequest tx) throws Exception
   {
      //This needs to be run on a different thread to the one used by JGroups to deliver the message
      //to avoid deadlock
      Runnable runnable = new MessagePullRequestRunnable(returnNodeId, number, txId, tx);
      
      executor.execute(runnable);
   }
       
   public boolean isClustered()
   {
      return true;
   }
        
   public int getRefCount()
   {
      //We are only interested in getting the reference count when delivery is not in progress
      //since we don't want mid delivery transient spurious values, so we execute the request
      //on the same thread.
      
      Future result = new Future();
      
      try
      {
         this.executor.execute(new GetRefCountRunnable(result));
      }
      catch (InterruptedException e)
      {
         log.warn("Thread interrupted", e);
      }

      return ((Integer)result.getResult()).intValue();
   }
   
   protected void deliverInternal() throws Throwable
   {      
      super.deliverInternal();
        
      //If the receivers are still ready to accept more refs then we might pull messages
      //from a remote queue          
      if (receiversReady && pullQueue != null)
      {
         //We send a message to the remote queue to pull a message - the remote queue will then send back
         //another message asynchronously with the result.
         //We don't do this synchronously with a message dispatcher since that can lead to distributed
         //deadlock
          
         sendPullMessage();
      }
   }
   
   private void sendPullMessage() throws Exception
   {
      if (pullQueue == null)
      {
         //Nothing to do
         return;
      }
      
      //Avoid synchronization
      RemoteQueueStub theQueue = pullQueue;
            
      if (theQueue == null)
      {
         return;
      }
      
      executor.execute(new SendPullRequestRunnable(theQueue));          
   }
   
   /*
    * Get the ref count - executed on event queue
    */
   private class GetRefCountRunnable implements Runnable
   {
      Future result;
      
      public GetRefCountRunnable(Future result)
      {
         this.result = result;
      }
      
      public void run()
      {
         int refCount = messageRefs.size();
         
         result.setResult(new Integer(refCount));        
      }
   }  
   
   /*
    * Send a message pull request.
    * 
    * TODO - do we really need this class?
    * Why can't we just execute on the same thread?
    */
   private class SendPullRequestRunnable implements Runnable
   {
      private RemoteQueueStub theQueue;
      
      private SendPullRequestRunnable(RemoteQueueStub theQueue)
      {
         this.theQueue = theQueue;
      }

      public void run()
      {
         try
         {
            //TODO
            //We create a tx just so we get the id - we could just get the id directly from the id
            //manager
            Transaction tx = tr.createTransaction();
                             
            ClusterRequest req = new PullMessagesRequest(nodeId, tx.getId(), theQueue.getChannelID(),
                                                         name, 1);
            
            office.asyncSendRequest(req, theQueue.getNodeId()); 
         }
         catch (Exception e)
         {
            log.error("Failed to pull message", e);
         }
      }
      
   }
   
   /**
    * This is how we "pull" messages from one node to another
    * If any of the messages are reliable then this needs to be done reliable (i.e. without loss or redelivery)
    * Normally this would require 2PC which would make performance suck.
    * However since we know both queues share the same DB then we can do the persistence locally in the same
    * tx thus avoiding 2PC and maintaining reliability :)
    * We do the following:
    * 
    * 1. Send a PullMessagesRequest to the remote node, on receipt it will create deliveries for message(s), and 
    * possibly add a holding tx (if there are any persistent messages), the messages will then be returned in
    * a PullMessagesResultRequest which is sent asynchronously from the remote node back to here to avoid
    * distributed deadlock.
    * 2. When the result is returned it hits this method.
    * 3. The retrieved messages are added to the local queue in the tx
    * 4. Deliveries corresponding to the messages retrieved are acknowledged LOCALLY for the remote queue.
    * 5. The local tx is committed.
    * 6. Send "commit" message to remote node
    * 7. "Commit" message is received and deliveries in the holding transaction are acknowledged IN MEMORY only.
    * On failure, commit or rollback will be called on the holding transaction causing the deliveries to be acked or cancelled
    * depending on whether they exist in the database
    * 
    * Recovery is handled in the same way as CastMessagesCallback
    * 
    */   
   
   private class MessagePullRequestRunnable implements Runnable
   { 
      int returnNodeId;
      
      int number;
      
      TransactionId txId;
      
      PullMessagesRequest tx;
      
      public MessagePullRequestRunnable(int returnNodeId, int number, TransactionId txId, PullMessagesRequest tx)
      { 
         this.returnNodeId = returnNodeId;
         
         this.number = number;
         
         this.txId = txId;
         
         this.tx = tx;
      }
      
      public void run()
      {
         try
         {
            List dels = null;
            
            //We only get the refs if receiversReady = false so as not to steal messages that
            //might be consumed by local receivers            
            if (!receiversReady)
            {               
               int count = 0;
               
               MessageReference ref;
               
               dels = new ArrayList();
               
               synchronized (refLock)
               {
                  synchronized (deliveryLock)
                  {
                     while (count < number && (ref = removeFirstInMemory()) != null)
                     {
                        SimpleDelivery del = new SimpleDelivery(LocalClusteredQueue.this, ref);
                        
                        deliveries.add(del);
                        
                        dels.add(del);       
                        
                        count++;
                     }  
                  }
               }                    
            }
            else
            {
               dels = Collections.EMPTY_LIST;
            }
            
            if (trace) { log.trace("PullMessagesRunnable got " + dels.size() + " deliveries"); }
            
            PullMessagesResultRequest response = new PullMessagesResultRequest(LocalClusteredQueue.this.nodeId, txId.getTxId(), name, dels.size());
            
            List reliableDels = null;
            
            if (!dels.isEmpty())
            {
               Iterator iter = dels.iterator();
               
               Delivery del = (Delivery)iter.next();
               
               if (del.getReference().isReliable())
               {
                  //Add it to internal list
                  if (reliableDels == null)
                  {
                     reliableDels = new ArrayList();                                    
                  }
                  
                  reliableDels.add(del);
               }
               else
               {
                  //We can ack it now
                  del.acknowledge(null);
               }
               
               response.addMessage(del.getReference().getMessage());
            }
                 
            if (reliableDels != null)
            {
               //Add this to the holding area
               tx.setReliableDels(reliableDels);
               office.holdTransaction(txId, tx);
            }
             
            //We send the messages asynchronously to avoid a deadlock situation which can occur
            //if we were using MessageDispatcher to get the messages.
            
            office.asyncSendRequest(response, returnNodeId);   
         }
         catch (Throwable e)
         {
            log.error("Failed to get deliveries", e);
         }                     
      }
   } 
   
   private class MessagePullResultRunnable implements Runnable
   {
      private RemoteQueueStub remoteQueue;
      
      private List messages;
      
      private long holdingTxId;
      
      //for testing only
      private boolean failBeforeCommit;
      private boolean failAfterCommit;
            
      private MessagePullResultRunnable(RemoteQueueStub remoteQueue,
                                        List messages, long holdingTxId,
                                        boolean failBeforeCommit, boolean failAfterCommit)
      {
         this.remoteQueue = remoteQueue;
         
         this.messages = messages;
         
         this.holdingTxId = holdingTxId;
         
         this.failBeforeCommit = failBeforeCommit;
         this.failAfterCommit = failAfterCommit;                  
      }

      public void run()
      {
         try
         {
            // TODO we should optimise for the case when only one message is pulled which is basically all
            //we support now anyway
            //Also we should optimise for the case when only non persistent messages are pulled
            //in this case we don't need to create a tx.
            
            Transaction tx = tr.createTransaction();
            
            Iterator iter = messages.iterator();
            
            boolean containsReliable = false;
            
            while (iter.hasNext())
            {
               org.jboss.messaging.core.Message msg = (org.jboss.messaging.core.Message)iter.next();
               
               if (msg.isReliable())
               {
                  //It will already have been persisted on the other node
                  //so we need to set the persisted flag here
                  msg.setPersisted(true);
                  
                  containsReliable = true;
               }
                     
               MessageReference ref = null;
               
               try
               {
                  ref = ms.reference(msg);
                  
                  //Should be executed synchronously since we already in the event queue
                  Delivery delRet = handleInternal(null, ref, tx, true, true);

                  if (delRet == null || !delRet.isSelectorAccepted())
                  {
                     //This should never happen
                     throw new IllegalStateException("Queue did not accept reference!");
                  }            
               }
               finally
               {
                  if (ref != null)
                  {
                     ref.releaseMemoryReference();
                  }
               }
                       
               //Acknowledge on the remote queue stub
               Delivery del = new SimpleDelivery(remoteQueue, ref);
               
               del.acknowledge(tx);        
            }
            
            //For testing to simulate failures
            if (failBeforeCommit)
            {
               throw new Exception("Test failure before commit");
            }
               
            tx.commit();
            
            //For testing to simulate failures
            if (failAfterCommit)
            {
               throw new Exception("Test failure after commit");
            }
            
            //TODO what if commit throws an exception - this means the commit message doesn't hit the 
            //remote node so the holding transaction stays in the holding area 
            //Need to catch the exception and throw a check message
            //What we need to do is catch any exceptions at the top of the call, i.e. just after the interface
            //and send a checkrequest
            //This applies to a normal message and messages requests too
                  
            //We only need to send a commit message if there were reliable messages since otherwise
            //the transaction wouldn't have been added in the holding area
            if (containsReliable && isRecoverable())
            {         
               ClusterRequest req = new PullMessagesRequest(nodeId, holdingTxId);
               
               office.asyncSendRequest(req, remoteQueue.getNodeId());
            }  
         }      
         catch (Throwable e)
         {
            log.error("Failed to handle pulled message", e);
         }
      }
      
   }
       
   
}
