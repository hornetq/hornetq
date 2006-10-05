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
import org.jboss.messaging.util.StreamUtils;

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
   
   private volatile boolean changedSignificantly;
   
   private RemoteQueueStub pullQueue;
   
   private int nodeId;
   
   //TODO Make configurable
   private int pullSize;
   
   private TransactionRepository tr;
   
   private Object pullLock = new Object();
 
   //TODO - we shouldn't have to specify office AND nodeId
   public LocalClusteredQueue(PostOffice office, int nodeId, String name, long id, MessageStore ms, PersistenceManager pm,             
                              boolean acceptReliableMessages, boolean recoverable, QueuedExecutor executor,
                              Filter filter, TransactionRepository tr,
                              int fullSize, int pageSize, int downCacheSize)
   {
      super(name, id, ms, pm, acceptReliableMessages, recoverable, executor, filter, fullSize, pageSize, downCacheSize);
     
      this.nodeId = nodeId;
      
      this.tr = tr;
      
      //FIXME - this cast is a hack
      this.office = (PostOfficeInternal)office;
   }
   
   public LocalClusteredQueue(PostOffice office, int nodeId, String name, long id, MessageStore ms, PersistenceManager pm,             
                              boolean acceptReliableMessages, boolean recoverable, QueuedExecutor executor,
                              Filter filter, TransactionRepository tr)
   {
      super(name, id, ms, pm, acceptReliableMessages, recoverable, executor, filter);
      
      this.nodeId = nodeId;
      
      this.tr = tr;
      
      //FIXME - this cast is a hack
      this.office = (PostOfficeInternal)office;
   }
   
   public void setPullInfo(RemoteQueueStub queue, int pullSize)
   {
      synchronized (pullLock)
      {
         this.pullQueue = queue;
         
         this.pullSize = pullSize;
      }
   }
   
   public QueueStats getStats()
   {      
      int cnt = messageCount();
      
      if (cnt != lastCount)
      {
         changedSignificantly = true;
         
         lastCount = cnt;
      }
      else
      {
         changedSignificantly = false;
      }
      
      return new QueueStats(name, cnt);
   }      
   
   //Have the stats changed significantly since the last time we request them?
   public boolean changedSignificantly()
   {
      return changedSignificantly;
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
    * Used when pulling messages from a remote queue
    */
   public List getDeliveries(int number) throws Exception
   {
      List dels = new ArrayList();
      
      synchronized (refLock)
      {
         synchronized (deliveryLock)
         {
            //We only get the refs if receiversReady = false so as not to steal messages that
            //might be consumed by local receivers            
            if (!receiversReady)
            {               
               int count = 0;
               
               MessageReference ref;
               
               while (count < number && (ref = removeFirstInMemory()) != null)
               {
                  SimpleDelivery del = new SimpleDelivery(this, ref);
                  
                  deliveries.add(del);
                  
                  dels.add(del);       
                  
                  count++;
               }           
               return dels;
            }
            else
            {
               return Collections.EMPTY_LIST;
            }
         }
      }          
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
   
   protected void deliverInternal(boolean handle) throws Throwable
   {            
      int beforeSize = -1;
      
      if (!handle)
      {
         beforeSize  = messageRefs.size();
      }      
      
      super.deliverInternal(handle);

      if (!handle)
      {
         int afterSize = messageRefs.size();
         
         if (trace)
         {
            log.trace(this + " Deciding whether to pull messages. " +
                     "receiversready:" + receiversReady + " before size:" + beforeSize + " afterSize: " + afterSize);
         }
         
         if (receiversReady && beforeSize == 0 && afterSize == 0)
         {
            //Delivery has been prompted (not from handle call)
            //and has run, and there are consumers that are still interested in receiving more
            //refs but there are none available in the channel (either the channel is empty
            //or there are only refs that don't match any selectors)
            //then we should perhaps pull some messages from a remote queue
            pullMessages();
         }
      }
   }
   
   public boolean isClustered()
   {
      return true;
   }
   
   /**
    * Pull messages from a remote queue to this queue.
    * If any of the messages are reliable then this needs to be done reliable (i.e. without loss or redelivery)
    * Normally this would require 2PC which would make performance suck.
    * However since we know both queues share the same DB then we can do the persistence locally in the same
    * tx thus avoiding 2PC and maintaining reliability:)
    * We do the following:
    * 
    * 1. A tx is started locally
    * 2. Create deliveries for message(s) on the remote node - bring messages back to the local node
    * We send a message to the remote node to retrieve a set of deliveries from the queue - it gets a max of num
    * deliveries.
    * The unreliable ones can be acknowledged immediately, the reliable ones are not acknowledged and a holding transaction
    * is placed in the holding area on the remote node, which contains knowledge of the deliveries.
    * The messages corresponding to the deliveries are returned to the local node
    * 3. The retrieved messages are added to the local queue in the tx
    * 4. Deliveries corresponding to the messages retrieved are acknowledged LOCALLY for the remote queue.
    * 5. The local tx is committed.
    * 6. Send "commit" message to remote node
    * 7. "Commit" message is received and deliveries in the holding transaction are acknowledged IN MEMORY only.
    * On failure, commit or rollback will be called on the holding transaction causing the deliveries to be acked or cancelled
    * depending on whether they exist in the database
    * 
    * This method will always be executed on the channel's event queue (via the deliver method)
    * so no need to do any handles or acks inside another event message
    */
   private void pullMessages() throws Throwable
   {       
      RemoteQueueStub theQueue;
      int thePullSize;
      
      synchronized (pullLock)
      {
         if (pullQueue == null)
         {
            return;
         }
         theQueue = pullQueue;
         thePullSize = pullSize;
      }
                
      Transaction tx = tr.createTransaction();
         
      ClusterRequest req = new PullMessagesRequest(this.nodeId, tx.getId(), theQueue.getChannelID(),
                                                   name, thePullSize);
      
      if (trace)
      {
         log.trace(System.identityHashCode(this) + " Executing pull messages request for queue " + name +
                   " pulling from node " + theQueue.getNodeId() + " to node " + this.nodeId);
      }
      
      byte[] bytes = (byte[])office.syncSendRequest(req, theQueue.getNodeId(), true);
      
      if (bytes == null)
      {
         //Ok - node might have left the group
         return;
      }
      
      PullMessagesResponse response = new PullMessagesResponse();
      
      StreamUtils.fromBytes(response, bytes);

      List msgs = response.getMessages();
      
      if (trace) { log.trace(System.identityHashCode(this) + " I retrieved " + msgs.size() + " messages from pull"); }
      
      Iterator iter = msgs.iterator();
      
      boolean containsReliable = false;
      
      while (iter.hasNext())
      {
         org.jboss.messaging.core.Message msg = (org.jboss.messaging.core.Message)iter.next();
         
         if (msg.isReliable())
         {
            //It will already have been persisted on the other node
            msg.setPersisted(true);
            
            containsReliable = true;
         }
         
         MessageReference ref = null;
         
         try
         {
            ref = ms.reference(msg);
            
            Delivery delRet = handleInternal(null, ref, tx, true, true);
            
            if (delRet == null || !delRet.isSelectorAccepted())
            {
               //This should never happen
               throw new IllegalStateException("Aaarrgg queue did not accept reference");
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
         Delivery del = new SimpleDelivery(theQueue, ref);
         
         del.acknowledge(tx);        
      }
          
      tx.commit();
      
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
         req = new PullMessagesRequest(this.nodeId, tx.getId());
         
         office.asyncSendRequest(req, theQueue.getNodeId());
      }
      
   }
}
