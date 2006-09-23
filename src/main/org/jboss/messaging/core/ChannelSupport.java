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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.refqueue.BasicPrioritizedDeque;
import org.jboss.messaging.core.refqueue.PrioritizedDeque;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionException;
import org.jboss.messaging.core.tx.TxCallback;
import org.jboss.messaging.util.Future;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

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
 * This prevents lock contention since requests are
 * executed serially, resulting in better scalability and higher throughput at the expense of some
 * latency.
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
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ChannelSupport.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   protected long channelID;

   protected Router router;

   protected MessageStore ms;

   protected QueuedExecutor executor;

   protected volatile boolean receiversReady;

   protected PrioritizedDeque messageRefs;

   protected Set deliveries;

   protected boolean acceptReliableMessages;

   protected boolean recoverable;

   protected PersistenceManager pm;

   protected Object refLock;

   protected Object deliveryLock;
   
   protected boolean active = true;
       
   // Constructors --------------------------------------------------

   protected ChannelSupport(long channelID, MessageStore ms,
                            PersistenceManager pm,
                            boolean acceptReliableMessages, boolean recoverable,
                            QueuedExecutor executor)
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

      messageRefs = new BasicPrioritizedDeque(10);

      deliveries = new LinkedHashSet();

      refLock = new Object();

      deliveryLock = new Object();
   }
   
   // Receiver implementation ---------------------------------------

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
      
   // DeliveryObserver implementation --------------------------

   public void acknowledge(Delivery d, Transaction tx) throws Throwable
   {
      if (trace) { log.trace("acknowledging " + d + (tx == null ? " non-transactionally" : " transactionally in " + tx)); }

      this.acknowledgeInternal(d, tx, true, false);
   }
   
  
   public void cancel(Delivery d) throws Throwable
   {
      // TODO We should also consider executing cancels on the event queue
      cancelInternal(d);   
   }

   // Distributor implementation ------------------------------------

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
   
   public int numberOfReceivers()
   {
      return router.numberOfReceivers();
   }

   // Channel implementation ----------------------------------------

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
      
      synchronized (deliveryLock)
      {
         synchronized (refLock)
         {
            //FIXME - This is currently broken since it doesn't take into account
            // refs paged into persistent storage
            // Also is very inefficient since it makes a copy
            // The way to implement this properly is to use the Prioritized deque iterator
            // combined with an iterator over the refs in storage
            
            //TODO use the ref queue iterator
            List references = delivering(filter);
                        
            List undel = undelivered(filter);            

            references.addAll(undel);
            
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
         //TODO we should keep track of how many deliveries are currently in the queue
         //so we don't execute another delivery when one is in the queue, since
         //this is pointless
                  
         this.executor.execute(new DeliveryRunnable(future));
         
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
   }  
   
   /*
    * This method clears the channel.
    * Basically it acknowledges any outstanding deliveries and consumes the rest of the messages in the channel.
    * We can't just delete the corresponding references directly from the database since
    * a) We might be paging
    * b) The message might remain in the message store causing a leak
    * 
    */
   public void removeAllReferences() throws Throwable
   {        
      synchronized (refLock)
      {
         synchronized (deliveryLock)
         {
            //Ack the deliveries
            
            //Clone to avoid ConcurrentModificationException
            Set dels = new HashSet(deliveries);

            Iterator iter = dels.iterator();
            while (iter.hasNext())
            {
               SimpleDelivery d = (SimpleDelivery) iter.next();
               
               d.acknowledge(null);
            }
            
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
               SimpleDelivery del = new SimpleDelivery(this, ref, false);
               
               del.acknowledge(null);           
            }
         }
      }
   }

   public List delivering(Filter filter)
   {
      List delivering = new ArrayList();

      synchronized (deliveryLock)
      {
         for (Iterator i = deliveries.iterator(); i.hasNext();)
         {
            Delivery d = (Delivery) i.next();

            MessageReference r = d.getReference();

            // TODO: I need to dereference the message each time I apply the
            // filter. Refactor so the message reference will also contain JMS
            // properties
            if (filter == null || filter.accept(r.getMessage()))
            {
               delivering.add(r);
            }
         }
      }
      if (trace) { log.trace(this + ": the non-recoverable state has " + delivering.size() + " messages being delivered"); }

      return delivering;
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
    
   public void activate()
   {
      synchronized (refLock)
      {
         synchronized (deliveryLock)
         {
            active = true;
         }
      }
   }
   
   public void deactivate()
   {
      synchronized (refLock)
      {
         synchronized (deliveryLock)
         {
            active = false;
         }
      }
   }
   
   public boolean isActive()
   {
      synchronized (refLock)
      {
         synchronized (deliveryLock)
         {
            return active;
         }
      }
   }
  
   // Public --------------------------------------------------------   

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

   public String toString()
   {
      return "ChannelSupport[" + channelID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /*
    * This methods delivers as many messages as possible to the router until no
    * more deliveries are returned. This method should never be called at the
    * same time as handle. 
    * 
    * @see org.jboss.messaging.core.Channel#deliver()
    */
   protected void deliverInternal(boolean handle) throws Throwable
   {
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
               if (iter == null)
               {
                  ref = (MessageReference) messageRefs.peekFirst();
               }
               else
               {
                  if (iter.hasNext())
                  {                        
                     ref = (MessageReference)iter.next();
                  } 
                  else
                  {
                     ref = null;
                  }
               }
            }

            if (ref != null)
            {
               // Check if message is expired (we also do this on the client
               // side)
               // If so ack it from the channel
               if (ref.isExpired())
               {
                  if (trace) { log.trace("Message reference: " + ref + " has expired"); }

                  // remove and acknowledge it
                  if (iter == null)
                  {
                     removeFirstInMemory();
                  }
                  else
                  {
                     iter.remove();
                  }

                  Delivery delivery = new SimpleDelivery(this, ref, true);

                  acknowledgeInternal(delivery, null, true, false);
               }
               else
               {
                  // Reference is not expired

                  // Attempt to push the ref to a receiver
                  Delivery del = router.handle(this, ref, null);

                  if (del == null)
                  {
                     // no receiver, broken receiver
                     // or full receiver    
                     // so we stop delivering
                     if (trace) { log.trace(this + ": no delivery returned for message" 
                                  + ref + " so no receiver got the message");
                                  log.trace("Delivery is now complete"); }

                     receiversReady = false;

                     return;
                  }
                  else if (!del.isSelectorAccepted())
                  {
                     // No receiver accepted the message because no selectors matched
                     // So we create an iterator (if we haven't already created it) to
                     // iterate through the refs in the channel
                     // TODO Note that this is only a partial solution since if there are messages paged to storage
                     // it won't try those - i.e. it will only iterate through those refs in memory.
                     // Dealing with refs in storage is somewhat tricky since we can't just load them and iterate
                     // through them since we might run out of memory
                     // So we will need to load individual refs from storage given the selector expressions
                     // Secondly we should also introduce some in memory indexes here to prevent having to
                     // iterate through all the refs every time
                     // Having said all that, having consumers on a queue that don't match many messages
                     // is an antipattern and should be avoided by the user
                     if (iter == null)
                     {
                        iter = messageRefs.iterator();
                     }                     
                  }
                  else
                  {
                     if (trace) { log.trace(this + ": " + del + " returned for message:" + ref); }
                     
                     //Receiver accepted the reference

                     // We must synchronize here to cope with another race
                     // condition where message is
                     // cancelled/acked in flight while the following few
                     // actions are being performed.
                     // e.g. delivery could be cancelled acked after being
                     // removed from state but before
                     // delivery being added (observed).
                     synchronized (del)
                     {
                        if (trace) { log.trace(this + " incrementing delivery count for " + del); }

                        // FIXME - It's actually possible the delivery could be
                        // cancelled before it reaches
                        // here, in which case we wouldn't get a delivery but we
                        // still need to increment the
                        // delivery count
                        // All the problems related to these race conditions and
                        // fiddly edge cases will disappear
                        // once we do
                        // http://jira.jboss.com/jira/browse/JBMESSAGING-355
                        // This will make life a lot easier

                        del.getReference().incrementDeliveryCount();                    

                        if (!del.isCancelled())
                        {
                           if (iter == null)
                           {
                              removeFirstInMemory();
                           }
                           else
                           {
                              iter.remove();                                
                           }

                           // delivered
                           if (!del.isDone())
                           {
                              // Add the delivery to state
                              synchronized (deliveryLock)
                              {
                                 deliveries.add(del);
                              }
                           }
                        }
                     }
                  }
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

   protected Delivery handleInternal(DeliveryObserver sender, MessageReference ref,
                                     Transaction tx, boolean persist, boolean synchronous)
   {
      if (ref == null)
      {
         return null;
      }

      if (trace) { log.trace(this + " handles " + ref + (tx == null ? " non-transactionally" : " in transaction: " + tx)); }
   
      //Each channel has its own copy of the reference
      ref = ref.copy();

      try
      {
         if (ref.isReliable() && !recoverable)
         {
            //Reliable reference in a non recoverable channel-
            //We handle it as a non reliable reference
            //It's important that we set it to non reliable otherwise if the channel
            //pages and is non recoverable a reliable ref will be paged in the database as reliable
            //which makes them hard to remove on server restart.
            //If we always page them as unreliable then it is easy to remove them.
            ref.setReliable(false);               
         }
         
         if (tx == null)
         {
            // Don't even attempt synchronous delivery for a reliable message
            // when we have an
            // non-recoverable state that doesn't accept reliable messages. If
            // we do, we may get
            // into the situation where we need to reliably store an active
            // delivery of a reliable
            // message, which in these conditions cannot be done.

            if (ref.isReliable() && !acceptReliableMessages)
            {
               log.error("Cannot handle reliable message " + ref
                        + " because the channel has a non-recoverable state!");
               return null;
            }
        
            if (persist && ref.isReliable() && recoverable)
            {
               // Reliable message in a recoverable state - also add to db
               if (trace) { log.trace(this + "adding " + ref + " to database non-transactionally"); }

               pm.addReference(channelID, ref, null);        
            }
            
            addReferenceInMemory(ref);
            
            // We only do delivery if there are receivers that haven't said they don't want
            // any more references.
            if (receiversReady)
            {
               // Prompt delivery
               deliverInternal(true);
            }
         }
         else
         {
            if (trace) { log.trace(this + "adding " + ref + " to state " + (tx == null ? "non-transactionally" : "in transaction: " + tx)); }

            if (ref.isReliable() && !acceptReliableMessages)
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

            if (persist && ref.isReliable() && recoverable)
            {
               // Reliable message in a recoverable state - also add to db
               if (trace) { log.trace(this + "adding " + ref + (tx == null ? " to database non-transactionally" : " in transaction: " + tx)); }

               pm.addReference(channelID, ref, tx);
            }
         }
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

   protected void acknowledgeInternal(Delivery d, Transaction tx, boolean persist, boolean synchronous) throws Exception
   {   
      if (tx == null)
      {
         synchronized (deliveryLock)
         {
            acknowledgeInMemory(d);
         }
            
         if (persist && recoverable && d.getReference().isReliable())
         {
            pm.removeReference(channelID, d.getReference(), null);
         }
              
         d.getReference().releaseMemoryReference();             
      }
      else
      {
         this.getCallback(tx, synchronous).addDelivery(d);
   
         if (trace) { log.trace(this + " added " + d + " to memory on transaction " + tx); }
   
         if (recoverable && d.getReference().isReliable())
         {
            pm.removeReference(channelID, d.getReference(), tx);
         }
      }
   }

   protected boolean acknowledgeInMemory(Delivery d)
   {
      if (d == null)
      {
         throw new IllegalArgumentException("Can't acknowledge a null delivery");
      }

      boolean removed = deliveries.remove(d);

      // It's ok if the delivery couldn't be found - this might happen
      // if the delivery is acked before the call to handle() has returned

      if (trace) { log.trace(this + " removed " + d + " from memory:" + removed); }
      
      return removed;
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
   
   protected abstract boolean cancelInternal(Delivery del) throws Exception;
   
   protected abstract MessageReference removeFirstInMemory() throws Exception;
   
   protected abstract void addReferenceInMemory(MessageReference ref) throws Exception;     
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

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
            // We don't execute the commit directly, we add it to the event queue
            // of the channel
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
            executor.execute(this);
         }
         catch (InterruptedException e)
         {
            log.error("Thread interrupted", e);
         }

         // Wait for it to complete

         Throwable t = (Throwable) result.getResult();

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

         Iterator iter = refsToAdd.iterator();

         while (iter.hasNext())
         {
            MessageReference ref = (MessageReference) iter.next();

            if (trace) { log.trace(this + ": adding " + ref + " to non-recoverable state"); }

            try
            {
               addReferenceInMemory(ref);
            }
            catch (Throwable t)
            {
               throw new TransactionException("Failed to add reference", t);
            }
         }

         // Remove deliveries

         iter = this.deliveriesToRemove.iterator();

         while (iter.hasNext())
         {
            Delivery del = (Delivery) iter.next();

            if (trace) { log.trace(this + " removing " + del + " after commit"); }

            del.getReference().releaseMemoryReference();

            try
            {
               synchronized (deliveryLock)
               {
                  acknowledgeInMemory(del);
               }
            }
            catch (Throwable t)
            {
               throw new TransactionException("Failed to ack message", t);
            }
         }
         
         //prompt delivery
         if (receiversReady)
         {
            deliverInternal(true);
         }
      }

      private void doAfterRollback()
      {
         Iterator iter = refsToAdd.iterator();

         while (iter.hasNext())
         {
            MessageReference ref = (MessageReference) iter.next();

            ref.releaseMemoryReference();
         }
      }

   }

   /**
    * Give subclass a chance to process the message before storing it
    * internally.
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

   // Private -------------------------------------------------------
 

  
   // Inner classes -------------------------------------------------

   protected class DeliveryRunnable implements Runnable
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
            if (router.numberOfReceivers() > 0)
            {               
               receiversReady = true;
               
               deliverInternal(false);                  
               
               if (result != null)
               {
                  result.setResult(null);
               }
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
}
