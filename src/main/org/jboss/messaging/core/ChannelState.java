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
      if (downCacheSize < 0)
      {
         throw new IllegalArgumentException("downCacheSize cannot be less than zero");
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
            AddReferenceCallback callback = new AddReferenceCallback(ref);
            tx.addCallback(callback);
            if (trace) { log.trace(this + " added transactionally " + ref + " in memory"); }
         }                  
      }  
      
      if (recoverable && ref.isReliable())
      {         
         //Reliable message in a recoverable state - also add to db
         if (trace) { log.trace("adding " + ref + (tx == null ? " to database non-transactionally" : " in transaction: " + tx)); }
         pm.addReference(channel.getChannelID(), ref, tx);         
      }                
   }
    
   public boolean addReference(MessageReference ref) throws Throwable
   {
      boolean first;
      
      ref.incrementChannelCount();
      
      synchronized (refLock)
      {         
         ref.setOrdering(getNextReferenceOrdering());
         
         if (paging)
         {         
            // We are in paging mode so the ref must go into persistent storage
            
            if (!ref.isReliable() && downCacheSize > 0)
            {
               addToDownCache(ref);
            }
            else
            {
               // Reliable (or no downcache) so need to persist now
               pm.addReference(channel.getChannelID(), ref, null); 
            }
            
            refsInStorage++;
                     
            first = false;        
            
            //We now decrement the channel count - since we don't want the message to stay in the store
            ref.decrementChannelCount();
         }
         else
         {                  
            first = addReferenceInMemory(ref);             
            
            if (recoverable && ref.isReliable())
            {            
               //Reliable message in a recoverable state - also add to db
               if (trace) { log.trace("adding " + ref + " to database non-transactionally"); }
               pm.addReference(channel.getChannelID(), ref, null);            
            }      
         }            
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
                  
                  ref.decrementChannelCount();
                  
                  if (!ref.isReliable())
                  {
                     if (downCacheSize > 0)
                     {
                        addToDownCache(ref);
                     }
                     else
                     {                     
                        pm.addReference(channel.getChannelID(), ref, null); 
                     }   
                  }              
               }
            }
         }
      }
   }
  
   public void acknowledge(Delivery d, Transaction tx) throws Throwable
   {
      //Transactional so add a post commit callback to remove after tx commit
      RemoveDeliveryCallback callback = new RemoveDeliveryCallback(d);
      
      tx.addCallback(callback);
      
      if (trace) { log.trace(this + " added " + d + " to memory on transaction " + tx); }
      
      if (recoverable && d.getReference().isReliable())
      {         
         pm.removeReference(channel.getChannelID(), d.getReference(), tx);           
      }           
   }
   
   public void acknowledge(Delivery d) throws Throwable
   {
      d.getReference().decrementChannelCount();
      
      acknowledgeInMemory(d);   
      
      if (recoverable && d.getReference().isReliable())
      {
         //TODO - Optimisation - If the message is acknowledged before the call to handle() returns
         //And it is a new message then there won't be any reference in the database
         //So the call to remove from the db is wasted.
         //We should add a flag to check this         
         pm.removeReference(channel.getChannelID(), d.getReference(), null);         
      }           
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
         return (MessageReference)messageRefs.peekFirst();
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
      synchronized (refLock)
      {
         refsInStorage = pm.getNumberOfReferences(channel.getChannelID());
         
         if (refsInStorage > 0)
         {
            load(refsInStorage, true);
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
   
   // Public --------------------------------------------------------

   public String toString()
   {
      return "State[" + channel + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void addToDownCache(MessageReference ref) throws Exception
   {
      // Non reliable refs can be cached in a down cache and then flushed to storage in blocks

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
      if (trace) { log.trace(this + " flushes downcache"); }

      pm.addReferences(channel.getChannelID(), downCache);
      
      downCache.clear();

      if (trace) { log.trace(this + " cleared downcache"); }
   }
   
   protected boolean addReferenceInMemory(MessageReference ref) throws Throwable
   {
      if (ref.isReliable() && !acceptReliableMessages)
      {
         throw new IllegalStateException("Reliable reference " + ref +
                                         " cannot be added to non-recoverable state");
      }

      boolean first = messageRefs.addLast(ref, ref.getPriority());

      if (trace) { log.trace(this + " added " + ref + " non-transactionally in memory"); }

      if (messageRefs.size() == fullSize)
      {            
         // We are full in memory - go into paging mode

         if (trace) { log.trace(this + " going into paging mode"); }
         paging = true;
      }
            
      return first;    
   }
   
   protected void acknowledgeInMemory(Delivery d) throws Throwable
   {      
      if (d == null)
      {
         throw new IllegalArgumentException("Can't acknowledge a null delivery");
      }
      
      synchronized (deliveryLock)
      {      
         boolean removed = deliveries.remove(d);
         
         if (trace) { log.trace(this + " removed " + d + " from memory:" + removed); } 
      }
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
               
         if (messageRefs.size() == fullSize - numberLoadable)
         {
            load(numberLoadable, false);                            
         }         
      }
   }
   
   protected void load(int number, boolean initial) throws Exception
   {
      //Must flush the down cache first
      flushDownCache();
      
      List refInfos = pm.getReferenceInfos(channel.getChannelID(), number);
      
      if (refInfos.isEmpty())
      {
         throw new IllegalStateException("Trying to page refs in from persitent storage - but can't find any!");
      }
            
      if (refInfos.size() != number)
      {
         throw new IllegalStateException("Only loaded " + refInfos.size() + " references, wanted " + number);
      }
      
      //Calculate which messages we need to load - we don't want to load messages if they are already
      //in the store
      
      List msgIds = new ArrayList();
      
      Iterator iter = refInfos.iterator();            
      
      MessageStore ms = channel.getMessageStore();
      
      while (iter.hasNext())
      {         
         PersistenceManager.ReferenceInfo info = (PersistenceManager.ReferenceInfo)iter.next();
         
         long msgId = info.getMessageId();
          
         if (!ms.containsMessage(msgId))
         {
            msgIds.add(new Long(msgId));
         }
      }
         
      //Create the messages
      List messages = null;
      
      if (!msgIds.isEmpty())
      {               
         messages = pm.getMessages(msgIds);         
      }
      
      Map msgMap = null;
      
      if (messages != null)
      {      
         iter = messages.iterator();
         
         msgMap = new HashMap();
         
         while (iter.hasNext())
         {
            Message m = (Message)iter.next();
            
            msgMap.put(new Long(m.getMessageID()), m);        
         }
      }
      
      //Create the references
      iter = refInfos.iterator();
      
      long firstOrdering = -1;
      long lastOrdering = -1;
      
      while (iter.hasNext())
      {    
         PersistenceManager.ReferenceInfo info = (PersistenceManager.ReferenceInfo)iter.next();
         
         if (firstOrdering == -1)
         {
            firstOrdering = info.getOrdering();
         }
         
         long msgId = info.getMessageId();
         
         Message m = null;
         
         if (messages != null)
         {
            m = (Message)msgMap.get(new Long(msgId));
         }
         
         MessageReference ref;
         
         if (m == null)
         {
            //Store already has the message
            
            ref = ms.reference(msgId);
         }
         else
         {
            ref = ms.reference(m);
            
            m.setInStorage(true);
         }
             
         ref.setDeliveryCount(info.getDeliveryCount());
         
         lastOrdering = info.getOrdering();
         
         ref.setOrdering(lastOrdering);                  
         
         messageRefs.addLast(ref, ref.getPriority());              
         
         ref.incrementChannelCount();
      }     
      
      if (!initial)
      {
         //Now we remove the non persistent references from storage
         pm.removeNonPersistentMessageReferences(channel.getChannelID(), firstOrdering, lastOrdering);
         
         //And we also remove any messages corresponding to the non-persistent messages we just removed         
         if (messages != null)
         {
            List msgsToRemove = new ArrayList();
            
            iter = messages.iterator();
            
            while (iter.hasNext())
            {
               Message m = (Message)iter.next();
               
               if (!m.isReliable())
               {
                  msgsToRemove.add(new Long(m.getMessageID()));
               }
            }
            
            pm.removeMessages(msgsToRemove);
         }
      }
      
      refsInStorage -= number;
      
      if (refsInStorage == 0)
      {
         if (trace) { log.trace(this + " exiting paging mode"); }
         paging = false;
      }         
   }  
         
   // Private -------------------------------------------------------
   
   private long getNextReferenceOrdering()
   {
      return ++messageOrdering;
   }    
         
   // Inner classes -------------------------------------------------  
   
   private class AddReferenceCallback implements TxCallback
   {
      private MessageReference ref;
      
      private AddReferenceCallback(MessageReference ref)
      {
         this.ref = ref;
      }
      
      public void beforePrepare()
      {         
         //NOOP
      }
      
      public void beforeCommit(boolean onePhase)
      {         
         //NOOP
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
         //We add the reference to the state
         
         if (trace) { log.trace(this + ": adding " + ref + " to non-recoverable state"); }
         
         ref.incrementChannelCount();         
         
         boolean first = false;
         try
         {         
            synchronized (refLock)
            {
               first = addReferenceInMemory(ref);
            }
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
      
      public void afterRollback(boolean onePhase)
      {
         //NOOP
      }           
   }
   
   private class RemoveDeliveryCallback implements TxCallback
   {
      private Delivery del;
      
      private RemoveDeliveryCallback(Delivery del)
      {
         this.del = del;
      }
      
      public void beforePrepare()
      {         
         //NOOP
      }
      
      public void beforeCommit(boolean onePhase)
      {         
         //NOOP
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
         if (trace) { log.trace(this + " removing " + del + " after commit"); }
         
         del.getReference().decrementChannelCount();
         
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
      
      public void afterRollback(boolean onePhase)
      {
         //NOOP
      }           
   }   
}
