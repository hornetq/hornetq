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
package org.jboss.messaging.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.refqueue.BasicPrioritizedDeque;
import org.jboss.messaging.core.refqueue.PrioritizedDeque;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TxCallback;

/**
 * Represents the state of a Channel.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.3</tt>
 *
 * ChannelState.java,v 1.3 2006/03/05 16:17:23 timfox Exp
 */
public class ChannelState implements State
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ChannelState.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   protected PrioritizedDeque messageRefs;
   
   protected Set deliveries;

   protected Channel channel;
   
   protected List downCache;
   
   protected boolean acceptReliableMessages;
   
   protected boolean recoverable;
    
   protected long messageOrdering;
   
   protected PersistenceManager pm;
   
   protected int fullSize;
   
   protected int pageSize;
   
   protected int downCacheSize;
   
   protected boolean paging;
   
   protected int refsInStorage;
   
   private Object refLock;
   
   private Object deliveryLock;
   
   //When we load refs from the channel we do so with values of ordering >= this value
   private long loadFromOrderingValue;
    
   // Constructors --------------------------------------------------
   
   public ChannelState(Channel channel, PersistenceManager pm,
                       boolean acceptReliableMessages, boolean recoverable,
                       int fullSize, int pageSize, int downCacheSize)
   {
      if (pm == null)
      {
          throw new IllegalArgumentException("ChannelState requires a " +
                                             "non-null persistence manager");
      }
      if (channel == null)
      {
          throw new IllegalArgumentException("ChannelState requires a " +
                                             "non-null Channel");
      }
      if (pageSize >= fullSize)
      {
         throw new IllegalArgumentException("pageSize must be less than full size");
      }
      if (downCacheSize > pageSize)
      {
         throw new IllegalArgumentException("pageSize cannot be smaller than downCacheSize");         
      }
      if (pageSize <= 0)
      {
         throw new IllegalArgumentException("pageSize must be greater than zero");
      }
      if (downCacheSize <= 0)
      {
         throw new IllegalArgumentException("downCacheSize must be greater than zero");
      }
                
      this.channel = channel;
      
      this.pm = pm;
      
      this.acceptReliableMessages = acceptReliableMessages;
      
      this.recoverable = recoverable;
      
      messageRefs = new BasicPrioritizedDeque(10);
      
      deliveries = new HashSet();
      
      downCache = new ArrayList();
      
      this.fullSize = fullSize;
      
      this.pageSize = pageSize;
      
      this.downCacheSize = downCacheSize;
      
      refLock = new Object();
      
      deliveryLock = new Object();
   }

   // State implementation -----------------------------------

   public boolean isRecoverable()
   {
      return recoverable;
   }

   public boolean acceptReliableMessages()
   {
      return acceptReliableMessages;
   }      
   
   public void addReference(MessageReference ref, Transaction tx) throws Throwable
   {
      if (trace) { log.trace(this + " adding " + ref + "transactionally in transaction: " + tx); }

      if (ref.isReliable() && !acceptReliableMessages)
      {
         // this transaction has no chance to succeed, since a reliable message cannot be
         // safely stored by a non-recoverable state, so doom the transaction
         if (trace) { log.trace(this + " cannot handle reliable messages, dooming the transaction"); }
         tx.setRollbackOnly();
      }
      else
      {
         //add to post commit callback
         synchronized (refLock)
         {
            ref.setOrdering(getNextReferenceOrdering());
            //AddReferenceCallback callback = new AddReferenceCallback(ref);
            //tx.addCallback(callback);
            this.getCallback(tx).addRef(ref);
            if (trace) { log.trace(this + " added transactionally " + ref + " in memory"); }
         }                  
      }                
        
      if (ref.isReliable() && recoverable)
      {         
         //Reliable message in a recoverable state - also add to db
         if (trace) { log.trace("adding " + ref + (tx == null ? " to database non-transactionally" : " in transaction: " + tx)); }
         
         pm.addReference(channel.getChannelID(), ref, tx);         
      }
             
   }
          
   public boolean addReference(MessageReference ref) throws Throwable
   {
      boolean first;
             
      synchronized (refLock)
      {         
         ref.setOrdering(getNextReferenceOrdering());
               
         if (ref.isReliable() && recoverable)
         {
            //Reliable message in a recoverable state - also add to db
            if (trace) { log.trace("adding " + ref + " to database non-transactionally"); }
            
            pm.addReference(channel.getChannelID(), ref, null);            
         }
         
         first = addReferenceInMemory(ref);
                               
      }
      return first; 
   }
   
   public void addDelivery(Delivery d) throws Throwable
   {
      synchronized (deliveryLock)
      {
         deliveries.add(d);
         
         if (trace) { log.trace(this + " added " + d + " to memory"); }
      }
   }
   
   /*
    * Cancel an outstanding delivery.
    * This removes the delivery and adds the message reference back into the state
    */
   public void cancelDelivery(Delivery del) throws Throwable
   {
      if (trace) { log.trace(this + " cancelling " + del + " in memory"); } 
        
      synchronized (deliveryLock)
      {         
         boolean removed = deliveries.remove(del);
         
         if (!removed)
         {
            //This is ok
            //This can happen if the message is cancelled before the result of ServerConsumerDelegate.handle
            //has returned, in which case we won't have a record of the delivery in the Set
            
            //In this case we don't want to add the message reference back into the state
            //since it was never removed in the first place
            
            if (trace) { log.trace(this + " can't find delivery " + del + " in state so not replacing messsage ref"); }
            
         }
         else
         {         
            synchronized (refLock)
            {
               messageRefs.addFirst(del.getReference(), del.getReference().getPriority());
                           
               if (trace) { log.trace(this + " added " + del.getReference() + " back into state"); }
               
               if (paging)
               {
                  //if paging we need to evict the end reference to storage to preserve the number of refs in the queue
                                 
                  MessageReference ref = (MessageReference)messageRefs.removeLast();
                  
                  addToDownCache(ref);                                                                           
                  
                  refsInStorage++;
               }
            }
         }
      }
   }
  
   public void acknowledge(Delivery d, Transaction tx) throws Throwable
   {
      //Transactional so remove after tx commit

//      RemoveDeliveryCallback callback = new RemoveDeliveryCallback(d);
//      
//      tx.addCallback(callback);
      
      this.getCallback(tx).addDelivery(d);
      
      if (trace) { log.trace(this + " added " + d + " to memory on transaction " + tx); }
      
      if (recoverable && d.getReference().isReliable())
      {         
         pm.removeReference(channel.getChannelID(), d.getReference(), tx);           
      }             
   }
   
   public void acknowledge(Delivery d) throws Throwable
   {            
      synchronized (deliveryLock)
      {      
         acknowledgeInMemory(d);
      }
      
      if (recoverable && d.getReference().isReliable())
      {
         //TODO - Optimisation - If the message is acknowledged before the call to handle() returns
         //And it is a new message then there won't be any reference in the database
         //So the call to remove from the db is wasted.
         //We should add a flag to check this         
         pm.removeReference(channel.getChannelID(), d.getReference(), null);         
      } 
      
      d.getReference().releaseMemoryReference();      
   }
         
   public MessageReference removeFirstInMemory() throws Throwable
   {      
      synchronized (refLock)
      {         
         MessageReference result = (MessageReference)messageRefs.removeFirst();
         
         checkLoad();      
              
         return (MessageReference)result;      
      }
   }
   
   public MessageReference peekFirst() throws Throwable
   {      
      synchronized (refLock)
      {
         MessageReference ref = (MessageReference)messageRefs.peekFirst();
         
         return ref;
      }
   }
    
   public List delivering(Filter filter)
   {
      List delivering = new ArrayList();
      
      synchronized (deliveryLock)
      {
         for(Iterator i = deliveries.iterator(); i.hasNext(); )
         {
            Delivery d = (Delivery)i.next();
            
            MessageReference r = d.getReference();

            // TODO: I need to dereference the message each time I apply the filter. Refactor so the message reference will also contain JMS properties
            if (filter == null || filter.accept(r.getMessage()))
            {
               delivering.add(r);
            }
         }
      }
      if (trace) {  log.trace(this + ": the non-recoverable state has " + delivering.size() + " messages being delivered"); }
      
      return delivering;
   }

   public List undelivered(Filter filter)
   {
      List undelivered = new ArrayList();
      
      synchronized(refLock)
      {
         Iterator iter = messageRefs.getAll().iterator();
         
         while (iter.hasNext())
         {
            MessageReference r = (MessageReference)iter.next();

            // TODO: I need to dereference the message each time I apply the filter. Refactor so the message reference will also contain JMS properties
            if (filter == null || filter.accept(r.getMessage()))
            {
               undelivered.add(r);
            }
            else
            {
               if (trace) { log.trace(this + ": " + r + " NOT accepted by filter so won't add to list"); }
            }
         }
      }
      if (trace) { log.trace(this + ": undelivered() returns a list of " + undelivered.size() + " undelivered memory messages"); }
      
      return undelivered;
   }

   public List browse(Filter filter)
   {
      //FIXME - This is currently broken since it doesn't take into account refs paged into persistent storage
      //Also is very inefficient since it makes a copy
      List result = delivering(filter);
      List undel = undelivered(filter);
      
      result.addAll(undel);
      return result;
   }
   
   public void clear()
   {
      //NOOP
   }
   
   public int messageCount()
   {
      synchronized (refLock)
      {
         synchronized (deliveryLock)
         {
            return messageRefs.size() + deliveries.size();
         }
      }      
   }

   public void load() throws Exception
   {
      if (trace) { log.trace(this + " loading channel state"); }
      synchronized (refLock)
      {         
         refsInStorage = pm.getNumberOfUnloadedReferences(channel.getChannelID()); 
         
         loadFromOrderingValue = pm.getMinOrdering(channel.getChannelID());

         if (refsInStorage > 0)
         {
            load(Math.min(refsInStorage, fullSize));
         }
      }
   }
   
   public void removeAll()
   {
      synchronized (refLock)
      {
         synchronized (deliveryLock)
         {
            //Remove all deliveries

            Iterator iter = deliveries.iterator();
            while (iter.hasNext())
            {
               Delivery d = (Delivery)iter.next();
               MessageReference r = d.getReference();
               removeCompletely(r);
            }
            deliveries.clear();
         
            //Remove all holding messages

            iter = messageRefs.getAll().iterator();
            while (iter.hasNext())
            {
               MessageReference r = (MessageReference)iter.next();
               removeCompletely(r);
            }
            messageRefs.clear();
            
         }
      }           
   }
   
   public int memoryRefCount()
   {
      synchronized (refLock)
      {
         return messageRefs.size();
      }      
   }
   
   public int memoryDeliveryCount()
   {
      synchronized (deliveryLock)
      {
         return deliveries.size();
      }
   }
   
   public int downCacheCount()
   {
      synchronized (refLock)
      {
         return downCache.size();
      }
   }
   
   public boolean isPaging()
   {
      synchronized (refLock)
      {
         return paging;
      }
   }
   
   // Public --------------------------------------------------------

   public String toString()
   {
      return "State[" + channel + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
            
   protected boolean addReferenceInMemory(MessageReference ref) throws Throwable
   {
      if (ref.isReliable() && !acceptReliableMessages)
      {
         throw new IllegalStateException("Reliable reference " + ref +
                                         " cannot be added to non-recoverable state");
      }
      
      boolean first;
      
      if (paging)
      {             
         addToDownCache(ref);        
         
         refsInStorage++;
                  
         first = false;                 
      }
      else
      {                  
         first = messageRefs.addLast(ref, ref.getPriority());

         if (trace) { log.trace(this + " added " + ref + " non-transactionally in memory"); }
         
         if (messageRefs.size() == fullSize)
         {            
            // We are full in memory - go into paging mode

            if (trace) { log.trace(this + " going into paging mode"); }
            
            paging = true;           
         }                         
      }   
      
      return first;
   }
   
   protected void addToDownCache(MessageReference ref) throws Exception
   {
      //If the down cache exists then refs are not spilled over immediately, but store in the cache
      //and spilled over in one go when the next load is requested, or when it is full
      
      //Both non reliable and reliable references can go in the down cache, however only non-reliable
      //references actually get added to storage, reliable references instead get their LOADED column
      //updated to "N".

      downCache.add(ref);

      if (trace) { log.trace(ref + " sent to downcache"); }
      
      if (downCache.size() == downCacheSize)
      {
         if (trace) { log.trace(this + "'s downcache is full (" + downCache.size() + " messages)"); }
         flushDownCache();
      }
   }
   
   protected void flushDownCache() throws Exception
   {
      if (trace) { log.trace(this + " flushing " + downCache.size() + " refs from downcache"); }
                  
      //Non persistent refs or persistent refs in a non recoverable state won't already be in the db
      //so they need to be inserted
      //Persistent refs in a recoverable state will already be there so need to be updated
      
      List toUpdate = new ArrayList();
      
      List toAdd = new ArrayList();
      
      Iterator iter = downCache.iterator();
      
      long minOrdering = Long.MAX_VALUE;
      
      while (iter.hasNext())
      {
         MessageReference ref = (MessageReference)iter.next();
         
         minOrdering = Math.min(minOrdering, ref.getOrdering());                 
         
         if (ref.isReliable() && recoverable)
         {
            toUpdate.add(ref);
         }
         else
         {
            toAdd.add(ref);
         }
      }
      
      if (!toAdd.isEmpty())
      {
         pm.addReferences(channel.getChannelID(), toAdd, false);
      }
      if (!toUpdate.isEmpty())
      {
         pm.updateReferencesNotLoaded(channel.getChannelID(), toUpdate);
      }
      
      //Release in memory refs for the refs we just spilled
      //Note! This must be done after the db inserts - to ensure message is still in memory
      iter = downCache.iterator();
      
      while (iter.hasNext())
      {
         MessageReference ref = (MessageReference)iter.next();
         
         ref.releaseMemoryReference();
      }
           
      downCache.clear();
      
      //when we select refs to load from the channel we load them with a value of ordering >= loadFromOrderingValue      
      if (this.loadFromOrderingValue == 0)
      {
         //First time paging
         this.loadFromOrderingValue = minOrdering;
      }
      else
      {
         //It's possible that one of the refs that we're spilling to disk
         //has a lower ordering value than the current loadFromOrderingValue value
         //normally this will not be the case but it can be the case if
         //messages are cancelled out of normal delivery order
         //or with committing transactions since the ordering for refs in a
         //transaction is determined when the refs are added to the tx, not
         //at commit time.
         //In these cases we need to adjust loadFromOrderingValue appropriately
         if (minOrdering < loadFromOrderingValue)
         {
            loadFromOrderingValue = minOrdering;
         }         
      }
                  
      if (trace) { log.trace(this + " cleared downcache"); }
   }
   
   protected void acknowledgeInMemory(Delivery d) throws Throwable
   {      
      if (d == null)
      {
         throw new IllegalArgumentException("Can't acknowledge a null delivery");
      }
             
      boolean removed = deliveries.remove(d);
      
      if (trace) { log.trace(this + " removed " + d + " from memory:" + removed); }       
   }
         
   protected void removeCompletely(MessageReference r)
   {
      if (recoverable && r.isReliable())
      {         
         try 
         {
            pm.removeReference(channel.getChannelID(), r, null);
         }
         catch (Exception e)
         {
            if (trace) { log.trace("removeAll() failed on removing " + r, e); }   
         }         
      }
   }
 
   protected void checkLoad() throws Throwable
   {   
      if (refsInStorage > 0)
      {         
         int numberLoadable = Math.min(refsInStorage, pageSize);
             
         if (messageRefs.size() <= fullSize - numberLoadable)
         {
            load(numberLoadable);  
         }         
      }
      else
      {         
         paging = false;         
      }
   }
   
   protected void load(int number) throws Exception
   {      
      if (trace) { log.trace(this + " Loading " + number + " refs from storage"); }
      
      //Must flush the down cache first
      flushDownCache();
      
      List refInfos = pm.getReferenceInfos(channel.getChannelID(), loadFromOrderingValue, number);
      
      //We may load less than desired due to "holes" - this is ok
      int numberLoaded = refInfos.size();
       
      if (numberLoaded == 0)
      {
         throw new IllegalStateException("Trying to page refs in from persitent storage - but can't find any!");
      }
                        
      MessageStore ms = channel.getMessageStore();
            
      Map refMap = new HashMap(refInfos.size());
      
      List msgIdsToLoad = new ArrayList(refInfos.size());
      
      Iterator iter = refInfos.iterator();
      
      //Put the refs that we already have messages for in a map
      while (iter.hasNext())
      {
         PersistenceManager.ReferenceInfo info = (PersistenceManager.ReferenceInfo)iter.next();
         
         long msgId = info.getMessageId();
          
         MessageReference ref = ms.reference(msgId);
         
         if (ref != null)
         {                                       
            refMap.put(new Long(msgId), ref);
         }
         else
         {
            //Add id to list of msg ids to load
            msgIdsToLoad.add(new Long(msgId));
         }
      }
      
      //Load the messages (if any)
      List messages = null;
      if (!msgIdsToLoad.isEmpty())
      {               
         messages = pm.getMessages(msgIdsToLoad);         
         
         if (messages.size() != msgIdsToLoad.size())
         {
            //Sanity check
            throw new IllegalStateException("Did not load correct number of messages, wanted:" + msgIdsToLoad.size() + " but got:" + messages.size());
         }
         
         //Create references for these messages and add them to the reference map
         iter = messages.iterator();
         
         while (iter.hasNext())
         {
            Message m = (Message)iter.next();
            
            //Message might actually be know to the store since we did the first check
            //since might have been added by different channel
            //in intervening period, but this is ok - the store knows to only return a reference
            //to the pre-existing message
            MessageReference ref = ms.reference(m);                        
                                    
            refMap.put(new Long(m.getMessageID()), ref);
         }
      }
      
      //Now we have all the messages loaded and refs created we need to put the refs in the right order
      //in the channel
  
      boolean loadedReliable = false;

      long firstOrdering = -1;
      long lastOrdering = -1;
      
      List toRemove = new ArrayList();
      
      iter = refInfos.iterator();
      while (iter.hasNext())
      {
         PersistenceManager.ReferenceInfo info = (PersistenceManager.ReferenceInfo)iter.next();
         
         if (firstOrdering == -1)
         {
            firstOrdering = info.getOrdering();
         }
         lastOrdering = info.getOrdering();
         
         long msgId = info.getMessageId();
         
         MessageReference ref = (MessageReference)refMap.get(new Long(msgId));
         
         ref.setDeliveryCount(info.getDeliveryCount());
         
         ref.setOrdering(info.getOrdering());   
         
         boolean first = messageRefs.addLast(ref, ref.getPriority());   
         
         if (recoverable && ref.isReliable())
         {
            loadedReliable = true;
         }
         else
         {
            //We put the non reliable refs (or reliable in a non-recoverable store)
            //in a list to be removed
            toRemove.add(ref);
         }
         
         if (first)
         {
            channel.deliver(null);
         }
      }
             
      if (!toRemove.isEmpty())
      {
         //Now we remove the references we loaded (only the non persistent or persistent in a non-recoverable
         //store)
         pm.removeReferences(channel.getChannelID(), toRemove);                  
      }
      
      if (loadedReliable)
      {
         //If we loaded any reliable refs then we must mark them as loaded in the store
         //otherwise they may get loaded again, the next time we do a load
         //We can't delete them since they're reliable and haven't been acked yet
         pm.updateReliableReferencesLoadedInRange(channel.getChannelID(), firstOrdering, lastOrdering);         
      }
      
      refsInStorage -= numberLoaded;
      
      loadFromOrderingValue = lastOrdering + 1;
       
      if (refsInStorage != 0 || messageRefs.size() == fullSize)
      {
         paging = true;
      }
      else
      {
         paging = false;
      }         
   }  
   
   protected InMemoryCallback getCallback(Transaction tx)
   {
      InMemoryCallback callback = (InMemoryCallback) tx.getKeyedCallback(this);

      if (callback == null)
      {
         callback = new InMemoryCallback();

         tx.addKeyedCallback(callback, this);
      }

      return callback;
   }
         
   // Private -------------------------------------------------------
   
   private long getNextReferenceOrdering()
   {
      return ++messageOrdering;
   }    
         
   // Inner classes -------------------------------------------------  
   
   private class InMemoryCallback implements TxCallback
   {
      private List refsToAdd;
      
      private List deliveriesToRemove;
      
      private long minOrder;
      
      private InMemoryCallback()
      {
         refsToAdd = new ArrayList();
         
         deliveriesToRemove = new ArrayList();
      }
      
      private void addRef(MessageReference ref)
      {
         refsToAdd.add(ref);
         
         minOrder = Math.min(minOrder, ref.getOrdering());
      }
      
      private void addDelivery(Delivery del)
      {
         deliveriesToRemove.add(del);
      }
      
      public void beforePrepare()
      {         
         //NOOP
      }
      
      public void beforeCommit(boolean onePhase)
      {         
      }
      
      public void beforeRollback(boolean onePhase)
      {         
         //NOOP
      }
      
      public void afterPrepare()
      {         
         //NOOP
      }
      
      public void afterCommit(boolean onePhase)
      {
         //We add the references to the state
         
         Iterator iter = refsToAdd.iterator();
         
         synchronized (refLock)
         {         
            while (iter.hasNext())
            {
               MessageReference ref = (MessageReference)iter.next();
               
               if (trace) { log.trace(this + ": adding " + ref + " to non-recoverable state"); }
                   
               boolean first = false;
               try
               {                        
                  first = addReferenceInMemory(ref);               
               }
               catch (Throwable t)
               {
                  // FIXME  - Sort out this exception handling
                  log.error("Failed to add reference", t);
               }
                           
               if (first)
               {
                  //No need to call prompt delivery if there are already messages in the queue
                  channel.deliver(null);
               }
            }              
         }
         
         //Remove deliveries
         
         iter = this.deliveriesToRemove.iterator();
         
         synchronized (deliveryLock)
         {         
            while (iter.hasNext())
            {            
               Delivery del = (Delivery)iter.next();
            
               if (trace) { log.trace(this + " removing " + del + " after commit"); }
               
               del.getReference().releaseMemoryReference();
               
               try
               {            
                  acknowledgeInMemory(del);                  
               }
               catch (Throwable t)
               {
                  //FIXME Sort out this exception handling!!!
                  log.error("Failed to ack message", t);
               }   
            }   
         }         
      } 
      
      public void afterRollback(boolean onePhase)
      {
         Iterator iter = refsToAdd.iterator();
         
         while (iter.hasNext())
         {
            MessageReference ref = (MessageReference)iter.next();
            
            ref.releaseMemoryReference();
         }
      }          
   }

//   private class AddReferenceCallback implements TxCallback
//   {
//      private MessageReference ref;
//      
//      private AddReferenceCallback(MessageReference ref)
//      {
//         this.ref = ref;
//      }
//      
//      public void beforePrepare()
//      {         
//         //NOOP
//      }
//      
//      public void beforeCommit(boolean onePhase)
//      {         
//      }
//      
//      public void beforeRollback(boolean onePhase)
//      {         
//         //NOOP
//      }
//      
//      public void afterPrepare()
//      {         
//         //NOOP
//      }
//      
//      public void afterCommit(boolean onePhase)
//      {
//         //We add the reference to the state
//         
//         if (trace) { log.trace(this + ": adding " + ref + " to non-recoverable state"); }
//             
//         boolean first = false;
//         try
//         {         
//            synchronized (refLock)
//            {
//               first = addReferenceInMemory(ref);
//            }
//         }
//         catch (Throwable t)
//         {
//            // FIXME  - Sort out this exception handling
//            log.error("Failed to add reference", t);
//         }
//                     
//         if (first)
//         {
//            //No need to call prompt delivery if there are already messages in the queue
//            channel.deliver(null);
//         }
//      } 
//      
//      public void afterRollback(boolean onePhase)
//      {
//         ref.releaseMemoryReference();
//      }           
//   }
   
//   private class RemoveDeliveryCallback implements TxCallback
//   {
//      private Delivery del;
//      
//      private RemoveDeliveryCallback(Delivery del)
//      {
//         this.del = del;
//      }
//      
//      public void beforePrepare()
//      {         
//         //NOOP
//      }
//      
//      public void beforeCommit(boolean onePhase)
//      {                                       
//      }
//      
//      public void beforeRollback(boolean onePhase)
//      {         
//         //NOOP
//      }
//      
//      public void afterPrepare()
//      {         
//         //NOOP
//      }
//      
//      public void afterCommit(boolean onePhase)
//      {
//         if (trace) { log.trace(this + " removing " + del + " after commit"); }
//                     
//         del.getReference().releaseMemoryReference();
//         
//         try
//         {            
//            synchronized (deliveryLock)
//            {
//               acknowledgeInMemory(del);
//            }
//         }
//         catch (Throwable t)
//         {
//            //FIXME Sort out this exception handling!!!
//            log.error("Failed to ack message", t);
//         }                  
//      } 
//      
//      public void afterRollback(boolean onePhase)
//      {
//         //NOOP
//      }           
//   }  
}
