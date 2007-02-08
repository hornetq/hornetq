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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.jboss.jms.server.MyTimeoutFactory;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionException;
import org.jboss.messaging.core.tx.TxCallback;
import org.jboss.messaging.util.Future;
import org.jboss.messaging.util.prioritylinkedlist.BasicPriorityLinkedList;
import org.jboss.messaging.util.prioritylinkedlist.PriorityLinkedList;
import org.jboss.util.timeout.Timeout;
import org.jboss.util.timeout.TimeoutTarget;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedInt;

/**
 * 
 * This class provides much of the functionality needed to implement a channel.
 * 
 * This partial implementation supports atomicity, isolation and recoverability of reliable messages.
 * 
 * It uses a "SEDA-type" approach, where requests to handle messages,
 * and deliver to receivers are not executed concurrently but placed on an event
 * queue and executed serially by a single thread.
 * 
 * Currently remoting does not support a non blocking API so a full SEDA approach is not possible at this stage.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt> $Id: ChannelSupport.java,v 1.65
 *          2006/06/27 19:44:39 timfox Exp $
 */
public abstract class ChannelSupport implements Channel
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ChannelSupport.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   protected long channelID;

   protected Router router;

   protected MessageStore ms;

   protected QueuedExecutor executor;

   protected volatile boolean receiversReady;

   protected PriorityLinkedList messageRefs;

   protected boolean acceptReliableMessages;

   protected boolean recoverable;

   protected PersistenceManager pm;

   protected Object refLock;

   protected boolean active = true;
   
   //TODO - I would like to get rid of this - the only reason we still keep a count of
   //refs being delivered is because many tests require this
   //Having to keep this count requires synchronization between delivery thread and acknowledgement
   //thread which will hamper concurrency
   //Suggest that we have a flag that disables this for production systems
   protected SynchronizedInt deliveringCount;
    
   protected Set scheduledDeliveries;
   
   //The maximum number of refs this queue can hold, or -1 if no limit
   //If any more refs are added after this point they are dropped
   protected int maxSize;
  
   protected SynchronizedInt messagesAdded;
    
   // Constructors ---------------------------------------------------------------------------------

   protected ChannelSupport(long channelID, MessageStore ms,
                            PersistenceManager pm,
                            boolean acceptReliableMessages, boolean recoverable,
                            QueuedExecutor executor, int maxSize)
   {
      if (trace) { log.trace("creating " + (pm != null ? "recoverable " : "non-recoverable ") + "channel[" + channelID + "]"); }

      if (ms == null)
      {
         throw new IllegalArgumentException("ChannelSupport requires a non-null message store");
      }
      if (pm == null)
      {
         throw new IllegalArgumentException("ChannelSupport requires a " +
                                            "non-null persistence manager");
      }

      this.ms = ms;

      this.pm = pm;

      this.channelID = channelID;

      this.executor = executor;

      this.acceptReliableMessages = acceptReliableMessages;

      this.recoverable = recoverable;

      messageRefs = new BasicPriorityLinkedList(10);

      refLock = new Object();
      
      deliveringCount = new SynchronizedInt(0);
      
      scheduledDeliveries = new HashSet();
      
      this.maxSize = maxSize;
      
      this.messagesAdded = new SynchronizedInt(0);
   }

   // Receiver implementation ----------------------------------------------------------------------

   public Delivery handle(DeliveryObserver sender, MessageReference ref, Transaction tx)
   {
      if (!active)
      {
         return null;
      }

      checkClosed();

      Future result = new Future();

      if (tx == null)
      {
         try
         {
            // Instead of executing directly, we add the handle request to the event queue.
            // Since remoting doesn't currently handle non blocking IO, we still have to wait for the
            // result, but when remoting does, we can use a full SEDA approach and get even better
            // throughput.
            this.executor.execute(new HandleRunnable(result, sender, ref, true));
         }
         catch (InterruptedException e)
         {
            log.warn("Thread interrupted", e);
         }

         return (Delivery)result.getResult();
      }
      else
      {
         return handleInternal(sender, ref, tx, true, false);
      }
   }

   // DeliveryObserver implementation --------------------------------------------------------------

   public void acknowledge(Delivery d, Transaction tx) throws Throwable
   {
      if (trace) { log.trace("acknowledging " + d + (tx == null ? " non-transactionally" : " transactionally in " + tx)); }

      acknowledgeInternal(d, tx, true, false);
   }

   public void cancel(Delivery del) throws Throwable
   {
      //We may need to update the delivery count in the database
      
      MessageReference ref = del.getReference();
      
      if (ref.getMessage().isReliable())
      {
         pm.updateDeliveryCount(this.channelID, ref);
      }
      
      deliveringCount.decrement();
      
      if (!checkAndSchedule(ref))
      {
         // We put the cancellation on the event queue if it's not a scheduled delivery
         this.executor.execute(new CancelRunnable(ref));     
      }
   }      
        
   // Distributor implementation -------------------------------------------------------------------

   public boolean add(Receiver r)
   {
      if (trace) { log.trace(this + " attempting to add receiver " + r); }

      boolean added = router.add(r);

      if (trace) { log.trace("receiver " + r + (added ? "" : " NOT") + " added"); }

      receiversReady = true;

      return added;
   }

   public boolean remove(Receiver r)
   {
      boolean removed = router.remove(r);

      if (removed && !router.iterator().hasNext())
      {
         receiversReady = false;
      }

      if (trace) { log.trace(this + (removed ? " removed " : " did NOT remove ") + r); }

      return removed;
   }

   public void clear()
   {
      router.clear();
   }

   public boolean contains(Receiver r)
   {
      return router.contains(r);
   }

   public Iterator iterator()
   {
      return router.iterator();
   }

   public int getNumberOfReceivers()
   {
      return router.getNumberOfReceivers();
   }

   // Channel implementation -----------------------------------------------------------------------

   public long getChannelID()
   {
      return channelID;
   }

   public boolean isRecoverable()
   {
      return recoverable;
   }

   public boolean acceptReliableMessages()
   {
      return acceptReliableMessages;
   }

   public List browse()
   {
      return browse(null);
   }

   public List browse(Filter filter)
   {
      if (trace) { log.trace(this + " browse" + (filter == null ? "" : ", filter = " + filter)); }

      synchronized (refLock)
      {
         //FIXME - This is currently broken since it doesn't take into account
         // refs paged into persistent storage
         // Also is very inefficient since it makes a copy
         // The way to implement this properly is to use the Prioritized deque iterator
         // combined with an iterator over the refs in storage

         //TODO use the ref queue iterator
         List references = undelivered(filter);

         // dereference pass
         ArrayList messages = new ArrayList(references.size());
         for (Iterator i = references.iterator(); i.hasNext();)
         {
            MessageReference ref = (MessageReference) i.next();
            messages.add(ref.getMessage());
         }
         return messages;
      }      
   }

   public void deliver(boolean synchronous)
   {
      checkClosed();

      // We put a delivery request on the event queue.
      try
      {
         Future future = null;

         if (synchronous)
         {
            future = new Future();
         }

         //TODO we should keep track of how many deliveries are currently in the queue so we don't
         //     execute another delivery when one is in the queue, since this is pointless.

         executor.execute(new DeliveryRunnable(future));

         if (synchronous)
         {
            // Wait to complete
            future.getResult();
         }
      }
      catch (InterruptedException e)
      {
         log.warn("Thread interrupted", e);
      }
   }      

   public void close()
   {
      if (router != null)
      {
         router.clear();
         
         router = null;
      }
      
      clearAllScheduledDeliveries();
   }

   /*
    * This method clears the channel.
    * Basically it consumes the rest of the messages in the channel.
    * We can't just delete the corresponding references directly from the database since
    * a) We might be paging
    * b) The message might remain in the message store causing a leak
    *
    */
   public void removeAllReferences() throws Throwable
   {
      log.debug(this + " remnoving all references");
      
      synchronized (refLock)
      {            
         //Now we consume the rest of the messages
         //This may take a while if we have a lot of messages including perhaps millions
         //paged in the database - but there's no obvious other way to do it.
         //We cannot just delete them directly from the database - because we may end up with messages leaking
         //in the message store,
         //also we might get race conditions when other channels are updating the same message in the db

         //Note - we don't do this in a tx - because the tx could be too big if we have millions of refs
         //paged in storage

         MessageReference ref;
         while ((ref = removeFirstInMemory()) != null)
         {
            SimpleDelivery del = new SimpleDelivery(this, ref);

            del.acknowledge(null);
         }         
         
         deliveringCount.set(0);
      }
      
      clearAllScheduledDeliveries();
   }

   public List undelivered(Filter filter)
   {
      List undelivered = new ArrayList();

      synchronized (refLock)
      {
         Iterator iter = messageRefs.getAll().iterator();

         while (iter.hasNext())
         {
            MessageReference r = (MessageReference) iter.next();

            // TODO: I need to dereference the message each time I apply the
            // filter. Refactor so the message reference will also contain JMS
            // properties
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

   /**
    * Returns the count of messages stored AND being delivered AND scheduled
    */
   public int getMessageCount()
   {
      synchronized (refLock)
      {
         return messageRefs.size() + getDeliveringCount() + getScheduledCount();
      }
   }
   
   public int getDeliveringCount()
   {
      return deliveringCount.get();
   }
   
   public int getScheduledCount()
   {
      synchronized (scheduledDeliveries)
      {
         return scheduledDeliveries.size();
      }
   }

   public void activate()
   {
      synchronized (refLock)
      {
         active = true;         
      }
   }

   public void deactivate()
   {
      synchronized (refLock)
      {
         active = false;         
      }
   }

   public boolean isActive()
   {
      synchronized (refLock)
      {
         return active;         
      }
   }
   
   public List recoverDeliveries(List messageIds)
   {
      //debug
      Iterator iter = messageIds.iterator();
                  
      List dels = new ArrayList();
      
      synchronized (refLock)
      {
         ListIterator liter = messageRefs.iterator();
                           
         while (iter.hasNext())
         {
            Long id = (Long)iter.next();
            
            //Scan the queue
            while (true)
            {               
               if (!liter.hasNext())
               {
                  // TODO we need to look in paging state too - currently not supported

                  // http://jira.jboss.org/jira/browse/JBMESSAGING-808
                  log.warn(this + " cannot find reference " + id + " (Might be paged!)");
                  break;
               }
               
               MessageReference ref = (MessageReference)liter.next();
               
               if (ref.getMessage().getMessageID() == id.longValue())
               {
                  liter.remove();
                  
                  Delivery del = new SimpleDelivery(this, ref);
                  
                  dels.add(del);
                                 
                  break;
               }
            }
         }           
      }
            
      return dels;
   }
   
   public int getMaxSize()
   {
      synchronized (refLock)
      {
         return maxSize;
      }
   }
   
   public void setMaxSize(int newSize)
   {
      synchronized (refLock)
      {
         int count = getMessageCount();
         
         if (newSize != -1 && count > newSize)
         {
            log.warn("Cannot set maxSize to " + newSize + " since there are already " + count + " refs");
         }
         else
         {
            maxSize = newSize;
         }
      }
   }
   
   public int getMessagesAdded()
   {
      return messagesAdded.get();
   }

   // Public ---------------------------------------------------------------------------------------

   //Only used for testing
   public int memoryRefCount()
   {
      synchronized (refLock)
      {
         return messageRefs.size();
      }
   }

   //Only used for testing

   public String toString()
   {
      return "ChannelSupport[" + channelID + "]";
   }

   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
   
   protected void clearAllScheduledDeliveries()
   {
      synchronized (scheduledDeliveries)
      {
         Set clone = new HashSet(scheduledDeliveries);
         
         Iterator iter = clone.iterator();
         
         while (iter.hasNext())
         {
            Timeout timeout = (Timeout)iter.next();
            
            timeout.cancel();
         }
         
         scheduledDeliveries.clear();
      }
   }

   protected void cancelInternal(MessageReference ref) throws Exception
   {
      if (trace) { log.trace(this + " cancelling " + ref + " in memory"); }

      synchronized (refLock)
      {
         messageRefs.addFirst(ref, ref.getMessage().getPriority());
      }
                  
      if (trace) { log.trace(this + " added " + ref + " back into state"); }
   }
   
   /**
    * This methods delivers as many messages as possible to the router until no more deliveries are
    * returned. This method should never be called at the same time as handle.
    *
    * @see org.jboss.messaging.core.Channel#deliver()
    */
   protected void deliverInternal()
   {
      if (trace) { log.trace(this + " was prompted delivery"); }

      try
      {
         // The iterator is used to iterate through the refs in the channel in the case that they
         // don't match the selectors of any receivers.
         ListIterator iter = null;
         
         MessageReference ref = null;

         while (true)
         {           
            synchronized (refLock)
            {
               ref = nextReference(iter);               
            }
            
            if (ref != null)
            {
               // Attempt to push the ref to a receiver
               
               if (trace) { log.trace(this + " pushing " + ref); }                                  

               Delivery del = router.handle(this, ref, null);

               receiversReady = del != null;
               
               if (del == null)
               {
                  // No receiver, broken receiver or full receiver so we stop delivering
                  if (trace) { log.trace(this + " got no delivery for " + ref + " so no receiver got the message. Stopping delivery."); }
                  break;
               }
               else if (!del.isSelectorAccepted())
               {
                  // No receiver accepted the message because no selectors matched, so we create
                  // an iterator (if we haven't already created it) to iterate through the refs
                  // in the channel. No delivery was really performed
                  
                  if (iter == null)
                  {
                     iter = messageRefs.iterator();
                  }                     
               }
               else
               {
                  if (trace) { log.trace(this + ": " + del + " returned for message " + ref); }
                  
                  // Receiver accepted the reference
                  
                  removeReference(iter);
                  
                  deliveringCount.increment();                     
               }               
            }
            else
            {
               // No more refs in channel or only ones that don't match any selectors
               if (trace) { log.trace(this + " no more refs to deliver "); }
               
               break;
            }
         }
      }
      catch (Throwable t)
      {
         log.error(this + " Failed to deliver", t);
      }
   }
   
   protected boolean deliverScheduled(MessageReference ref)
   {
      try
      {      
         // We synchonize on the ref lock to prevent scheduled deivery kicking in before
         // load has finished
         synchronized (refLock)
         {
            // Attempt to push the ref to a receiver
            
            if (trace) { log.trace(this + " pushing " + ref); }                                  
   
            Delivery del = router.handle(this, ref, null);
   
            receiversReady = del != null;
            
            if (del == null)
            {
               // No receiver, broken receiver or full receiver so we stop delivering
               if (trace) { log.trace(this + ": no delivery returned for message" + ref + " so no receiver got the message. Delivery is now complete"); }
   
               return false;
            }
            else if (del.isSelectorAccepted())
            {
               if (trace) { log.trace(this + ": " + del + " returned for message:" + ref); }
               
               // Receiver accepted the reference
               
               deliveringCount.increment();                   
               
               return true;
            }                
         }
      }
      catch (Throwable t)
      {
         log.error(this + " Failed to deliver", t);
      }
      
      return false;
   }
      
   protected Delivery handleInternal(DeliveryObserver sender, MessageReference ref,
                                     Transaction tx, boolean persist, boolean synchronous)
   {
      if (ref == null)
      {
         return null;
      }

      if (trace) { log.trace(this + " handles " + ref + (tx == null ? " non-transactionally" : " in transaction: " + tx)); }
      
      if (maxSize != -1 && getMessageCount() >= maxSize)
      {
         //Have reached maximum size - will drop message
         
         log.warn(this + " has reached maximum size, " + ref + " will be dropped");
         
         return null;
      }
   
      // Each channel has its own copy of the reference
      ref = ref.copy();

      try
      {  
         if (tx == null)
         {
            // Don't even attempt synchronous delivery for a reliable message when we have an
            // non-recoverable state that doesn't accept reliable messages. If we do, we may get
            // into the situation where we need to reliably store an active delivery of a reliable
            // message, which in these conditions cannot be done.

            if (ref.getMessage().isReliable() && !acceptReliableMessages)
            {
               log.error("Cannot handle reliable message " + ref +
                  " because the channel has a non-recoverable state!");
               return null;
            }
        
            if (persist && ref.getMessage().isReliable() && recoverable)
            {
               // Reliable message in a recoverable state - also add to db
               if (trace) { log.trace(this + " adding " + ref + " to database non-transactionally"); }

               // TODO - this db access could safely be done outside the event loop
               pm.addReference(channelID, ref, null);        
            }
            
            // If the ref has a scheduled delivery time then we don't add to the in memory queue
            // instead we create a timeout, so when that time comes delivery will attempted directly
            
            if (!checkAndSchedule(ref))
            {               
               synchronized (refLock)
               {
                  addReferenceInMemory(ref);
               }
               
               // We only do delivery if there are receivers that haven't said they don't want any
               // more references.
               if (receiversReady)
               {
                  // Prompt delivery
                  deliverInternal();
               }
            }
         }
         else
         {
            if (trace) { log.trace(this + " adding " + ref + " to state " + (tx == null ? "non-transactionally" : "in transaction: " + tx)); }

            if (ref.getMessage().isReliable() && !acceptReliableMessages)
            {
               // this transaction has no chance to succeed, since a reliable
               // message cannot be
               // safely stored by a non-recoverable state, so doom the
               // transaction               
               log.warn(this + " cannot handle reliable messages, dooming the transaction");
               
               tx.setRollbackOnly();
            } 
            else
            {
               // add to post commit callback
               getCallback(tx, synchronous).addRef(ref);
               
               if (trace) { log.trace(this + " added transactionally " + ref + " in memory"); }
            }

            if (persist && ref.getMessage().isReliable() && recoverable)
            {
               // Reliable message in a recoverable state - also add to db
               if (trace) { log.trace(this + " adding " + ref + (tx == null ? " to database non-transactionally" : " in transaction: " + tx)); }

               pm.addReference(channelID, ref, tx);
            }
         }
         
         messagesAdded.increment();
      }
      catch (Throwable t)
      {
         log.error("Failed to handle message", t);

         ref.releaseMemoryReference();

         return null;
      }

      // I might as well return null, the sender shouldn't care
      
      return new SimpleDelivery(this, ref, true);
   }

   
   protected boolean checkAndSchedule(MessageReference ref)
   {
      if (ref.getScheduledDeliveryTime() > System.currentTimeMillis())
      {      
         if (trace) { log.trace("Scheduling delivery for " + ref + " to occur at " + ref.getScheduledDeliveryTime()); }
         
         //Schedule the cancel to actually occur at the specified time
         
         //Need to synchronize to prevent timeout being removed before it is added
         synchronized (scheduledDeliveries)
         {            
            Timeout timeout =
               MyTimeoutFactory.instance.getFactory().schedule(ref.getScheduledDeliveryTime(),
                        new DeliverRefTimeoutTarget(ref));
            
            scheduledDeliveries.add(timeout);
         }      
         
         return true;
      }
      else
      {
         return false;
      }
   }
   
   protected void acknowledgeInternal(Delivery d, Transaction tx, boolean persist,
                                      boolean synchronous) throws Exception
   {   
      if (tx == null)
      {                  
         if (persist && recoverable && d.getReference().getMessage().isReliable())
         {
            pm.removeReference(channelID, d.getReference(), null);
         }
              
         d.getReference().releaseMemoryReference(); 
         
         deliveringCount.decrement();
      }
      else
      {
         this.getCallback(tx, synchronous).addDelivery(d);
   
         if (trace) { log.trace(this + " added " + d + " to memory on transaction " + tx); }
   
         if (recoverable && d.getReference().getMessage().isReliable())
         {
            pm.removeReference(channelID, d.getReference(), tx);
         }
      }
   }

   protected InMemoryCallback getCallback(Transaction tx, boolean synchronous)
   {
      InMemoryCallback callback = (InMemoryCallback) tx.getCallback(this);            

      if (callback == null)
      {
         callback = new InMemoryCallback(synchronous);

         tx.addCallback(callback, this);
      }
      else
      {
         //Sanity
         if (callback.isSynchronous() != synchronous)
         {
            throw new IllegalStateException("Callback synchronousness status doesn't match");
         }
      }

      return callback;
   }
 
   protected MessageReference removeFirstInMemory() throws Exception
   {
      MessageReference result = (MessageReference) messageRefs.removeFirst();

      return (MessageReference) result;
   }
   
   protected void addReferenceInMemory(MessageReference ref) throws Exception
   {
      if (ref.getMessage().isReliable() && !acceptReliableMessages)
      {
         throw new IllegalStateException("Reliable reference " + ref +
                                         " cannot be added to non-recoverable state");
      }

      messageRefs.addLast(ref, ref.getMessage().getPriority());

      if (trace){ log.trace(this + " added " + ref + " non-transactionally in memory"); }      
   }    
   
   // Private --------------------------------------------------------------------------------------
   
   private void removeReference(ListIterator iter) throws Exception
   {
      synchronized (refLock)
      {
         if (iter == null)
         {
            if (trace) { log.trace(this + " removing first ref in memory"); } 
            
            removeFirstInMemory();
         }
         else
         {
            if (trace) { log.trace(this + " removed current message from iterator"); }                           
                        
            iter.remove();                                
         }
      }
   }
   
   private MessageReference nextReference(ListIterator iter) throws Throwable
   {
      MessageReference ref;
      
      if (iter == null)
      {
         //We just get the next ref from the head of the queue
         ref = (MessageReference) messageRefs.peekFirst();
      }
      else
      {
         // TODO This will not work with paged refs - see http://jira.jboss.com/jira/browse/JBMESSAGING-275
         // We need to extend it to work with refs from the db
         
         //We have an iterator - this means we are iterating through the queue to find a ref that matches
         if (iter.hasNext())
         {                        
            ref = (MessageReference)iter.next();
         } 
         else
         {
            ref = null;
         }
      }
      
      return ref;
   } 

   // Inner classes --------------------------------------------------------------------------------

   private class InMemoryCallback implements TxCallback, Runnable
   {
      private List refsToAdd;

      private List deliveriesToRemove;
      
      private boolean synchronous;
          
      private boolean committing;

      private Future result;

      private InMemoryCallback(boolean synchronous)
      {
         refsToAdd = new ArrayList();

         deliveriesToRemove = new ArrayList();
         
         this.synchronous = synchronous;
      }
      
      private boolean isSynchronous()
      {
         return synchronous;
      }

      private void addRef(MessageReference ref)
      {
         refsToAdd.add(ref);
      }

      private void addDelivery(Delivery del)
      {
         deliveriesToRemove.add(del);
      }

      public void beforePrepare()
      {
         // NOOP
      }

      public void beforeCommit(boolean onePhase)
      {
         // NOOP
      }

      public void beforeRollback(boolean onePhase)
      {
         // NOOP
      }

      public void afterPrepare()
      {
         // NOOP
      }
      
      public void run()
      {
         try
         {
            if (committing)
            {
               doAfterCommit();
            }
            else
            {
               doAfterRollback();
            }
            
            result.setResult(null);
         }
         catch (Throwable t)
         {
            log.debug(this + "'s execution generated exception", t);
            result.setException(t);
         }
      }

      public void afterCommit(boolean onePhase) throws Exception
      {
         if (synchronous)
         {
            try
            {
               doAfterCommit();
            }
            catch (Throwable t)
            {
               //TODO Sort out exception handling!!
               throw new TransactionException("Failed to commit", t);
            }
         }
         else
         {            
            // We don't execute the commit directly, we add it to the event queue of the channel
            // so it is executed in turn
            committing = true;
            executeAndWaitForResult();
         }
      }

      public void afterRollback(boolean onePhase) throws Exception
      {
         if (synchronous)            
         {
            doAfterRollback();
         }
         else
         {                     
            // We don't execute the commit directly, we add it to the event queue
            // of the channel
            // so it is executed in turn
            committing = false;
   
            executeAndWaitForResult();
         }
      }

      public String toString()
      {
         return ChannelSupport.this + ".InMemoryCallback[" +
                Integer.toHexString(InMemoryCallback.this.hashCode()) + "]";
      }

      private void executeAndWaitForResult() throws TransactionException
      {
         result = new Future();

         try
         {
            if (trace) { log.trace("adding " + this + " to " + ChannelSupport.this + "'s executor"); }
            executor.execute(this);
         }
         catch (InterruptedException e)
         {
            log.error("Thread interrupted", e);
         }

         // Wait for it to complete

         if (trace) { log.trace("waiting for " + this + " to complete"); }
         Throwable t = (Throwable)result.getResult();
         if (trace) { log.trace(InMemoryCallback.this + " completed"); }

         if (t != null)
         {
            if (t instanceof RuntimeException)
            {
               throw (RuntimeException) t;
            }
            if (t instanceof Error)
            {
               throw (Error) t;
            }
            if (t instanceof TransactionException)
            {
               throw (TransactionException) t;
            }
            throw new IllegalStateException("Unknown Throwable " + t);
         }
      }

      private void doAfterCommit() throws Throwable
      {
         // We add the references to the state
         
         for(Iterator i = refsToAdd.iterator(); i.hasNext(); )
         {
            MessageReference ref = (MessageReference)i.next();

            if (trace) { log.trace(this + ": adding " + ref + " to non-recoverable state"); }

            try
            {
               synchronized (refLock)
               {
                  addReferenceInMemory(ref);
               }
            }
            catch (Throwable t)
            {
               throw new TransactionException("Failed to add reference", t);
            }
         }

         // Remove deliveries
         
         for(Iterator i = deliveriesToRemove.iterator(); i.hasNext(); )
         {
            Delivery del = (Delivery)i.next();

            if (trace) { log.trace(this + " removing " + del + " after commit"); }

            del.getReference().releaseMemoryReference();
            
            deliveringCount.decrement();
         }
         
         // prompt delivery
         if (receiversReady)
         {
            deliverInternal();
         }
      }

      private void doAfterRollback()
      {
         for(Iterator i = refsToAdd.iterator(); i.hasNext(); )
         {
            MessageReference ref = (MessageReference)i.next();

            if (trace) { log.trace(this + " releasing memory " + ref + " after rollback"); }
            ref.releaseMemoryReference();
         }
      }
   }

   /**
    * Give subclass a chance to process the message before storing it internally.
    * TODO - Do we really need this?
    */
   protected void processMessageBeforeStorage(MessageReference reference)
   {
      // by default a noop
   }

   protected void checkClosed()
   {
      if (router == null)
      {
         throw new IllegalStateException(this + " closed");
      }
   }

   // Private --------------------------------------------------------------------------------------
 

  
   // Inner classes --------------------------------------------------------------------------------

   private class DeliveryRunnable implements Runnable
   {
      Future result;
      
      public DeliveryRunnable(Future result)
      {
         this.result = result;
      }
      
      public void run()
      {
         try
         {
            if (router.getNumberOfReceivers() > 0)
            {               
               deliverInternal();                     
            }
            if (result != null)
            {
               result.setResult(null);
            }
         }
         catch (Throwable t)
         {
            log.error("Failed to deliver", t);
            if (result != null)
            {
               result.setException(t);
            }
         }
      }
   } 
   
   private class DeliverScheduledRunnable implements Runnable
   {
      private MessageReference ref;
      
      private Future result;
      
      public DeliverScheduledRunnable(Future result, MessageReference ref)
      {
         this.ref = ref;
         
         this.result = result;
      }
      
      public void run()
      {
         try         
         {
            if (router.getNumberOfReceivers() > 0)
            {               
               boolean delivered = deliverScheduled(ref);     
               
               result.setResult(Boolean.valueOf(delivered));
            }
            else
            {
               result.setResult(Boolean.valueOf(false));
            }
         }
         catch (Throwable t)
         {
            log.error("Failed to deliver scheduled", t);            
         }
      }
   } 
   
   private class CancelRunnable implements Runnable
   {
      MessageReference ref;
      
      CancelRunnable(MessageReference ref)
      {
         this.ref = ref;
      }
      
      public void run()
      {
         try
         {
            cancelInternal(ref);
         }
         catch (Exception e)
         {
            log.error("Failed to cancel delivery", e);
         }
      }
   }
   
   protected class HandleRunnable implements Runnable
   {
      Future result;

      DeliveryObserver sender;

      MessageReference ref;
      
      boolean persist;

      public HandleRunnable(Future result, DeliveryObserver sender, MessageReference ref, boolean persist)
      {
         this.result = result;
         this.sender = sender;
         this.ref = ref;
         this.persist = persist;
      }

      public void run()
      {
         Delivery d = handleInternal(sender, ref, null, persist, false);
         result.setResult(d);
      }
   }   
   
   private class DeliverRefTimeoutTarget implements TimeoutTarget
   {
      private MessageReference ref;

      public DeliverRefTimeoutTarget(MessageReference ref)
      {
         this.ref = ref;
      }

      public void timedOut(Timeout timeout)
      {
         if (trace) { log.trace("Scheduled delivery timeout " + ref); }
         
         synchronized (scheduledDeliveries)
         {
            boolean removed = scheduledDeliveries.remove(timeout);
            
            if (!removed)
            {
               throw new IllegalStateException("Failed to remove timeout " + timeout);
            }
         }
              
         ref.setScheduledDeliveryTime(0);
           
         Future result = new Future();
         
         DeliverScheduledRunnable runnable = new DeliverScheduledRunnable(result, ref);
         
         try
         {
            executor.execute(runnable);
         }
         catch (InterruptedException e)
         {
            log.error("Thread interrupted", e);
         }
         
         Boolean b = (Boolean)result.getResult();
         boolean delivered = b.booleanValue();
         
         if (!delivered)
         {
            //add to front of queue
            try
            {
               executor.execute(new CancelRunnable(ref));
            }
            catch (InterruptedException e)
            {
               log.error("Thread interrupted", e);
            }
         }
         else
         {
            if (trace) { log.trace("Delivered scheduled delivery at " + System.currentTimeMillis() + " for " + ref); }
         }
      }
   }
}
