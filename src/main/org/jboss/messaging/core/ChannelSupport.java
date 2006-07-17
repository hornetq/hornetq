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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.memory.MemoryManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.refqueue.BasicPrioritizedDeque;
import org.jboss.messaging.core.refqueue.PrioritizedDeque;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionException;
import org.jboss.messaging.core.tx.TxCallback;
import org.jboss.messaging.util.Future;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedLong;

/**
 * A basic channel implementation. It supports atomicity, isolation and, if a
 * non-null PersistenceManager is available, it supports recoverability of
 * reliable messages. The channel implementation here uses a "SEDA-type"
 * approach, where requests to handle messages, deliver to receivers or
 * acknowledge messages are not executed concurrently but placed on an event
 * queue and executed serially by a single thread. This prevents lock contention
 * since requests are executed serially, resulting in better scalability and
 * higher throughput at the expense of some latency
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

   protected boolean receiversReady;

   protected PrioritizedDeque messageRefs;

   protected Set deliveries;

   protected List downCache;

   protected boolean acceptReliableMessages;

   protected boolean recoverable;

   protected SynchronizedLong messageOrdering;

   protected PersistenceManager pm;

   protected MemoryManager mm;

   protected int fullSize;

   protected int pageSize;

   protected int downCacheSize;

   protected boolean paging;

   protected int refsInStorage;

   private Object refLock;

   private Object deliveryLock;

   // When we load refs from the channel we do so with values of ordering >=
   // this value
   private long loadFromOrderingValue;

   // Constructors --------------------------------------------------

   /**
    * @param acceptReliableMessages -
    *           it only makes sense if tl is null. Otherwise ignored (a
    *           recoverable channel always accepts reliable messages)
    */

   protected ChannelSupport(long channelID, MessageStore ms,
            PersistenceManager pm, MemoryManager mm,
            boolean acceptReliableMessages, boolean recoverable, int fullSize,
            int pageSize, int downCacheSize, QueuedExecutor executor)
   {
      if (trace)
      {
         log.trace("creating "
                  + (pm != null ? "recoverable " : "non-recoverable ")
                  + "channel[" + channelID + "]");
      }

      if (ms == null)
      {
         throw new IllegalArgumentException(
                  "ChannelSupport requires a non-null message store");
      }
      if (pm == null)
      {
         throw new IllegalArgumentException("ChannelSupport requires a "
                  + "non-null persistence manager");
      }
      if (pageSize >= fullSize)
      {
         throw new IllegalArgumentException(
                  "pageSize must be less than full size");
      }
      if (downCacheSize > pageSize)
      {
         throw new IllegalArgumentException(
                  "pageSize cannot be smaller than downCacheSize");
      }
      if (pageSize <= 0)
      {
         throw new IllegalArgumentException(
                  "pageSize must be greater than zero");
      }
      if (downCacheSize <= 0)
      {
         throw new IllegalArgumentException(
                  "downCacheSize must be greater than zero");
      }

      this.ms = ms;

      this.pm = pm;

      this.mm = mm;

      this.channelID = channelID;

      this.executor = executor;

      this.acceptReliableMessages = acceptReliableMessages;

      this.recoverable = recoverable;

      messageRefs = new BasicPrioritizedDeque(10);

      deliveries = new LinkedHashSet();

      downCache = new ArrayList();

      this.fullSize = fullSize;

      this.pageSize = pageSize;

      this.downCacheSize = downCacheSize;

      refLock = new Object();

      deliveryLock = new Object();

      messageOrdering = new SynchronizedLong(0);
   }

   // Receiver implementation ---------------------------------------

   public Delivery handle(DeliveryObserver sender, Routable r, Transaction tx)
   {
      checkClosed();
      
      Future result = new Future();

      try
      {
         // Instead of executing directly, we add the handle request to the
         // event queue
         // Since remoting doesn't currently handle non blocking IO, we still
         // have to wait for the result
         // But when remoting does, we can use a full SEDA approach and get even
         // better throughput
         this.executor.execute(new HandleRunnable(result, sender, r, tx));
      }
      catch (InterruptedException e)
      {
         log.warn("Thread interrupted", e);
      }

      return (Delivery) result.getResult();
   }

   // DeliveryObserver implementation --------------------------

   public void acknowledge(Delivery d, Transaction tx)
   {
      if (trace)
      {
         log.trace("acknowledging "
                  + d
                  + (tx == null ? " non-transactionally"
                           : " transactionally in " + tx));
      }

      try
      {
         if (tx == null)
         {
            // acknowledge non transactionally

            // we put the acknowledgement on the event queue

            // try
            // {
            // Future result = new Future();
            //
            // this.executor.execute(new AcknowledgeRunnable(d, result));
            //               
            // //For now we wait for result, but this may not be necessary
            // result.getResult();
            // }
            // catch (InterruptedException e)
            // {
            // log.warn("Thread interrupted", e);
            // }

            // TODO We should consider also executing acks on the event queue
            acknowledgeInternal(d);

         }
         else
         {
            this.getCallback(tx).addDelivery(d);

            if (trace)
            {
               log.trace(this + " added " + d + " to memory on transaction "
                        + tx);
            }

            if (recoverable && d.getReference().isReliable())
            {
               pm.removeReference(channelID, d.getReference(), tx);
            }
         }
      }
      catch (Throwable t)
      {
         log.error("Failed to remove delivery " + d + " from state", t);
      }
   }

   public void cancel(Delivery d)
   {
      // We put the cancellation on the event queue
      // try
      // {
      // Future result = new Future();
      //         
      // this.executor.execute(new CancelRunnable(d, result));
      //         
      // //For now we wait for result, but this may not be necessary
      // result.getResult();
      // }
      // catch (InterruptedException e)
      // {
      // log.warn("Thread interrupted", e);
      // }

      // TODO We should also consider executing cancels on the event queue
      try
      {
         cancelInternal(d);
      }
      catch (Throwable t)
      {
         log.error("Failed to cancel delivery", t);
      }

   }

   // Distributor implementation ------------------------------------

   public boolean add(Receiver r)
   {
      if (trace)
      {
         log.trace(this + " attempting to add receiver " + r);
      }

      boolean added = router.add(r);

      if (trace)
      {
         log.trace("receiver " + r + (added ? "" : " NOT") + " added");
      }
      
      receiversReady = true;

      return added;
   }

   public boolean remove(Receiver r)
   {
      boolean removed = router.remove(r);

      if (trace)
      {
         log.trace(this + (removed ? " removed " : " did NOT remove ") + r);
      }

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
      if (trace)
      {
         log.trace(this + " browse"
                  + (filter == null ? "" : ", filter = " + filter));
      }
      
      synchronized (deliveryLock)
      {
         synchronized (refLock)
         {
            //FIXME - This is currently broken since it doesn't take into account
            // refs paged into persistent storage
            // Also is very inefficient since it makes a copy
            List references = delivering(filter);
            
            Iterator iter = references.iterator();
            while (iter.hasNext())
            {
               MessageReference ref = (MessageReference)iter.next();
            }
            
            List undel = undelivered(filter);
            
            iter = undel.iterator();
            while (iter.hasNext())
            {
               MessageReference ref = (MessageReference)iter.next();
            }

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
                  
         this.executor.execute(new DeliveryRunnable(future));
         
         if (synchronous)
         {
            //Wait to complete
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

   public void removeAllMessages()
   {
      synchronized (refLock)
      {
         synchronized (deliveryLock)
         {
            // Remove all deliveries

            Iterator iter = deliveries.iterator();
            while (iter.hasNext())
            {
               Delivery d = (Delivery) iter.next();
               MessageReference r = d.getReference();
               removeCompletely(r);
            }
            deliveries.clear();

            // Remove all holding messages

            iter = messageRefs.getAll().iterator();
            while (iter.hasNext())
            {
               MessageReference r = (MessageReference) iter.next();
               removeCompletely(r);
            }
            messageRefs.clear();

         }
      }
   }

   public void load() throws Exception
   {
      if (trace)
      {
         log.trace(this + " loading channel state");
      }
      synchronized (refLock)
      {
         // First we need to reset the loaded flag in the db to "N"
         pm.resetLoadedStatus(channelID);

         refsInStorage = pm.getNumberOfUnloadedReferences(channelID);

         loadFromOrderingValue = pm.getMinOrdering(channelID);

         if (refsInStorage > 0)
         {
            load(Math.min(refsInStorage, fullSize));
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
      if (trace)
      {
         log.trace(this + ": the non-recoverable state has "
                  + delivering.size() + " messages being delivered");
      }

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
               if (trace)
               {
                  log.trace(this + ": " + r
                           + " NOT accepted by filter so won't add to list");
               }
            }
         }
      }
      if (trace)
      {
         log.trace(this + ": undelivered() returns a list of "
                  + undelivered.size() + " undelivered memory messages");
      }

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
   protected void deliverInternal()
   {
      try
      {
         while (true)
         {
            MessageReference ref;

            synchronized (refLock)
            {
               ref = (MessageReference) messageRefs.peekFirst();               
            }

            if (ref != null)
            {
               // Check if message is expired (we also do this on the client
               // side)
               // If so ack it from the channel
               if (ref.isExpired())
               {
                  if (trace)
                  {
                     log.trace("Message reference: " + ref + " has expired");
                  }

                  // remove and acknowledge it

                  removeFirstInMemory();

                  Delivery delivery = new SimpleDelivery(this, ref, true);

                  // TODO - is this stage really necessary?
                  synchronized (deliveryLock)
                  {
                     deliveries.add(delivery);
                  }

                  acknowledgeInternal(delivery);
               }
               else
               {
                  // Reference is not expired

                  // Push the ref to a receiver
                  Delivery del = push(ref);

                  if (del == null)
                  {
                     // no receiver, receiver that doesn't accept the message or
                     // broken receiver

                     if (trace)
                     {
                        log.trace(this + ": no delivery returned for message"
                                 + ref + " so no receiver got the message");
                     }

                     // Now we stop delivering

                     if (trace)
                     {
                        log.trace("Delivery is now complete");
                     }

                     receiversReady = false;

                     return;
                  }
                  else
                  {
                     if (trace)
                     {
                        log.trace(this + ": delivery returned for message:"
                                 + ref);
                     }

                     // We must synchronize here to cope with another race
                     // condition where message is
                     // cancelled/acked in flight while the following few
                     // actions are being performed.
                     // e.g. delivery could be cancelled acked after being
                     // removed from state but before
                     // delivery being added (observed).
                     synchronized (del)
                     {
                        if (trace)
                        {
                           log.trace(this + " incrementing delivery count for "
                                    + del);
                        }

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

                        // Note we don't increment the delivery count if the
                        // message didn't match the selector
                        // FIXME - this is a temporary hack that will disappear
                        // once
                        // http://jira.jboss.org/jira/browse/JBMESSAGING-275
                        // is solved
                        boolean incrementCount = true;
                        if (del instanceof SimpleDelivery)
                        {
                           SimpleDelivery sd = (SimpleDelivery) del;
                           incrementCount = sd.isSelectorAccepted();
                        }
                        if (incrementCount)
                        {
                           del.getReference().incrementDeliveryCount();                    
                        }

                        if (!del.isCancelled())
                        {
                           removeFirstInMemory();

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
               // No more refs in channel
               if (trace)
               {
                  log.trace(this + " no more refs to deliver ");
               }
               break;
            }
         }
      }
      catch (Throwable t)
      {
         log.error(this + " Failed to deliver", t);
      }
   }

   protected Delivery handleInternal(DeliveryObserver sender, Routable r,
            Transaction tx)
   {
      if (r == null)
      {
         return null;
      }

      if (trace)
      {
         log.trace(this
                  + " handles "
                  + r
                  + (tx == null ? " non-transactionally" : " in transaction: "
                           + tx));
      }

      MessageReference ref = obtainReference(r);

      try
      {

         if (tx == null)
         {
            // Don't even attempt synchronous delivery for a reliable message
            // when we have an
            // non-recoverable state that doesn't accept reliable messages. If
            // we do, we may get
            // into the situation where we need to reliably store an active
            // delivery of a reliable
            // message, which in these conditions cannot be done.

            if (r.isReliable() && !acceptReliableMessages)
            {
               log.error("Cannot handle reliable message " + r
                        + " because the channel has a non-recoverable state!");
               return null;
            }

            checkMemory();

            ref.setOrdering(messageOrdering.increment());

            if (ref.isReliable() && recoverable)
            {
               // Reliable message in a recoverable state - also add to db
               if (trace)
               {
                  log.trace("adding " + ref
                           + " to database non-transactionally");
               }

               pm.addReference(channelID, ref, null);
            }

            addReferenceInMemory(ref);

            // We only do delivery if there are receivers that haven't said they
            // don't want
            // any more references
            if (receiversReady)
            {
               // Prompt delivery
               deliverInternal();
            }
         }
        else
         {
            if (trace)
            {
               log.trace("adding "
                        + ref
                        + " to state "
                        + (tx == null ? "non-transactionally"
                                 : "in transaction: " + tx));
            }

            checkMemory();

            if (ref.isReliable() && !acceptReliableMessages)
            {
               // this transaction has no chance to succeed, since a reliable
               // message cannot be
               // safely stored by a non-recoverable state, so doom the
               // transaction
               if (trace)
               {
                  log
                           .trace(this
                                    + " cannot handle reliable messages, dooming the transaction");
               }
               tx.setRollbackOnly();
            } 
            else
            {
               // add to post commit callback
               ref.setOrdering(messageOrdering.increment());
               this.getCallback(tx).addRef(ref);
               if (trace)
               {
                  log.trace(this + " added transactionally " + ref
                           + " in memory");
               }
            }

            if (ref.isReliable() && recoverable)
            {
               // Reliable message in a recoverable state - also add to db
               if (trace)
               {
                  log.trace("adding "
                           + ref
                           + (tx == null ? " to database non-transactionally"
                                    : " in transaction: " + tx));
               }

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
      return new SimpleDelivery(sender, ref, true);
   }

   protected void acknowledgeInternal(Delivery d) throws Throwable
   {
      synchronized (deliveryLock)
      {
         acknowledgeInMemory(d);
      }

      if (recoverable && d.getReference().isReliable())
      {
         // TODO - Optimisation - If the message is acknowledged before the call
         // to handle() returns
         // And it is a new message then there won't be any reference in the
         // database
         // So the call to remove from the db is wasted.
         // We should add a flag to check this
         pm.removeReference(channelID, d.getReference(), null);
      }

      d.getReference().releaseMemoryReference();
   }

   protected void cancelInternal(Delivery del) throws Throwable
   {
      if (trace)
      {
         log.trace(this + " cancelling " + del + " in memory");
      }

      boolean removed;

      synchronized (deliveryLock)
      {
         removed = deliveries.remove(del);
      }

      if (!removed)
      {
         // This is ok
         // This can happen if the message is cancelled before the result of
         // ServerConsumerDelegate.handle
         // has returned, in which case we won't have a record of the delivery
         // in the Set

         // In this case we don't want to add the message reference back into
         // the state
         // since it was never removed in the first place

         if (trace)
         {
            log.trace(this + " can't find delivery " + del
                     + " in state so not replacing messsage ref");
         }

      }
      else
      {
         synchronized (refLock)
         {
            messageRefs.addFirst(del.getReference(), del.getReference()
                     .getPriority());

            if (paging)
            {
               // if paging we need to evict the end reference to storage to
               // preserve the number of refs in the queue

               MessageReference ref = (MessageReference) messageRefs
                        .removeLast();

               addToDownCache(ref);

               refsInStorage++;
            }
         }

         if (trace)
         {
            log.trace(this + " added " + del.getReference()
                     + " back into state");
         }
      }
   }

   protected MessageReference removeFirstInMemory() throws Throwable
   {
      synchronized (refLock)
      {
         MessageReference result = (MessageReference) messageRefs.removeFirst();

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

         return (MessageReference) result;
      }
   }

   protected MessageReference obtainReference(Routable r)
   {
      MessageReference ref = null;

      // Convert to reference
      try
      {
         if (!r.isReference())
         {
            // We should only handle references in core.
            // TODO enforce this in the signature of handle method
            // See http://jira.jboss.com/jira/browse/JBMESSAGING-255
            log.warn("Should only handle references");
            // Remove this when this is enforced
            ref = ms.reference((Message) r);
         }
         else
         {
            // Each channel has it's own copy of the reference
            ref = ((MessageReference) r).copy();
         }

         return ref;
      }
      catch (Exception e)
      {
         log.error("Failed to reference routable", e);
         // FIXME - Swallowing exceptions
         return null;
      }
   }

   protected void checkMemory()
   {

      // Disabled for now

      // if (mm != null)
      // {
      // boolean isLow = mm.isMemoryLow();
      //         
      // if (isLow)
      // {
      //            
      // synchronized (refLock)
      // {
      // if (!paging)
      // {
      // log.info("Memory is low:" + this);
      //                  
      // fullSize = messageRefs.size() + 1;
      //                  
      // //TODO Make this configurable
      // pageSize = downCacheSize = Math.max(1, fullSize / 50);
      //                  
      // log.info("Turned paging on, fullSize=" + fullSize + " dc:" +
      // downCacheSize + " ps: " + pageSize);
      // }
      // else
      // {
      // //log.info("already paging");
      // }
      //               
      // }
      // }
      // }
   }

   protected void addReferenceInMemory(MessageReference ref) throws Throwable
   {
      if (ref.isReliable() && !acceptReliableMessages)
      {
         throw new IllegalStateException("Reliable reference " + ref
                  + " cannot be added to non-recoverable state");
      }

      synchronized (refLock)
      {
         if (paging)
         {
            addToDownCache(ref);

            refsInStorage++;
         }
         else
         {
            messageRefs.addLast(ref, ref.getPriority());

            if (trace)
            {
               log.trace(this + " added " + ref
                        + " non-transactionally in memory");
            }

            if (messageRefs.size() == fullSize)
            {
               // We are full in memory - go into paging mode

               if (trace)
               {
                  log.trace(this + " going into paging mode");
               }

               paging = true;
            }
         }
      }
   }

   protected void addToDownCache(MessageReference ref) throws Exception
   {
      // If the down cache exists then refs are not spilled over immediately,
      // but store in the cache
      // and spilled over in one go when the next load is requested, or when it
      // is full

      // Both non reliable and reliable references can go in the down cache,
      // however only non-reliable
      // references actually get added to storage, reliable references instead
      // get their LOADED column
      // updated to "N".

      downCache.add(ref);

      if (trace)
      {
         log.trace(ref + " sent to downcache");
      }

      if (downCache.size() == downCacheSize)
      {
         if (trace)
         {
            log.trace(this + "'s downcache is full (" + downCache.size()
                     + " messages)");
         }
         flushDownCache();
      }
   }

   protected void flushDownCache() throws Exception
   {
      if (trace)
      {
         log.trace(this + " flushing " + downCache.size()
                  + " refs from downcache");
      }

      // Non persistent refs or persistent refs in a non recoverable state won't
      // already be in the db
      // so they need to be inserted
      // Persistent refs in a recoverable state will already be there so need to
      // be updated

      List toUpdate = new ArrayList();

      List toAdd = new ArrayList();

      Iterator iter = downCache.iterator();

      long minOrdering = Long.MAX_VALUE;

      while (iter.hasNext())
      {
         MessageReference ref = (MessageReference) iter.next();

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
         pm.addReferences(channelID, toAdd, false);
      }
      if (!toUpdate.isEmpty())
      {
         pm.updateReferencesNotLoaded(channelID, toUpdate);
      }

      // Release in memory refs for the refs we just spilled
      // Note! This must be done after the db inserts - to ensure message is
      // still in memory
      iter = downCache.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = (MessageReference) iter.next();

         ref.releaseMemoryReference();
      }

      downCache.clear();

      // when we select refs to load from the channel we load them with a value
      // of ordering >= loadFromOrderingValue
      if (this.loadFromOrderingValue == 0)
      {
         // First time paging
         this.loadFromOrderingValue = minOrdering;
      }
      else
      {
         // It's possible that one of the refs that we're spilling to disk
         // has a lower ordering value than the current loadFromOrderingValue
         // value
         // normally this will not be the case but it can be the case if
         // messages are cancelled out of normal delivery order
         // or with committing transactions since the ordering for refs in a
         // transaction is determined when the refs are added to the tx, not
         // at commit time.
         // In these cases we need to adjust loadFromOrderingValue appropriately
         if (minOrdering < loadFromOrderingValue)
         {
            loadFromOrderingValue = minOrdering;
         }
      }

      if (trace)
      {
         log.trace(this + " cleared downcache");
      }
   }

   protected void acknowledgeInMemory(Delivery d) throws Throwable
   {
      if (d == null)
      {
         throw new IllegalArgumentException("Can't acknowledge a null delivery");
      }

      boolean removed = deliveries.remove(d);

      // It's ok if the delivery couldn't be found - this might happen
      // if the delivery is acked before the call to handle() has returned

      if (trace)
      {
         log.trace(this + " removed " + d + " from memory:" + removed);
      }
   }

   protected void load(int number) throws Exception
   {
      if (trace)
      {
         log.trace(this + " Loading " + number + " refs from storage");
      }

      // Must flush the down cache first
      flushDownCache();

      List refInfos = pm.getReferenceInfos(channelID, loadFromOrderingValue,
               number);

      // We may load less than desired due to "holes" - this is ok
      int numberLoaded = refInfos.size();
      
      if (numberLoaded == 0)
      {
         throw new IllegalStateException(
                  "Trying to page refs in from persitent storage - but can't find any!");
      }

      Map refMap = new HashMap(refInfos.size());

      List msgIdsToLoad = new ArrayList(refInfos.size());

      Iterator iter = refInfos.iterator();

      // Put the refs that we already have messages for in a map
      while (iter.hasNext())
      {
         PersistenceManager.ReferenceInfo info = (PersistenceManager.ReferenceInfo) iter
                  .next();

         long msgId = info.getMessageId();

         MessageReference ref = ms.reference(msgId);

         if (ref != null)
         {
            refMap.put(new Long(msgId), ref);
         }
         else
         {
            // Add id to list of msg ids to load
            msgIdsToLoad.add(new Long(msgId));
         }
      }

      // Load the messages (if any)
      List messages = null;
      if (!msgIdsToLoad.isEmpty())
      {
         messages = pm.getMessages(msgIdsToLoad);

         if (messages.size() != msgIdsToLoad.size())
         {
            // Sanity check
            throw new IllegalStateException(
                     "Did not load correct number of messages, wanted:"
                              + msgIdsToLoad.size() + " but got:"
                              + messages.size());
         }

         // Create references for these messages and add them to the reference
         // map
         iter = messages.iterator();

         while (iter.hasNext())
         {
            Message m = (Message) iter.next();

            // Message might actually be know to the store since we did the
            // first check
            // since might have been added by different channel
            // in intervening period, but this is ok - the store knows to only
            // return a reference
            // to the pre-existing message
            MessageReference ref = ms.reference(m);

            refMap.put(new Long(m.getMessageID()), ref);
         }
      }

      // Now we have all the messages loaded and refs created we need to put the
      // refs in the right order
      // in the channel

      boolean loadedReliable = false;

      long firstOrdering = -1;
      long lastOrdering = -1;

      List toRemove = new ArrayList();

      iter = refInfos.iterator();
      while (iter.hasNext())
      {
         PersistenceManager.ReferenceInfo info = (PersistenceManager.ReferenceInfo) iter
                  .next();

         if (firstOrdering == -1)
         {
            firstOrdering = info.getOrdering();
         }
         lastOrdering = info.getOrdering();

         long msgId = info.getMessageId();

         MessageReference ref = (MessageReference) refMap.get(new Long(msgId));

         ref.setDeliveryCount(info.getDeliveryCount());

         ref.setOrdering(info.getOrdering());

         messageRefs.addLast(ref, ref.getPriority());

         if (recoverable && ref.isReliable())
         {
            loadedReliable = true;
         }
         else
         {
            // We put the non reliable refs (or reliable in a non-recoverable
            // store)
            // in a list to be removed
            toRemove.add(ref);
         }
      }

      if (!toRemove.isEmpty())
      {
         // Now we remove the references we loaded (only the non persistent or
         // persistent in a non-recoverable
         // store)
         pm.removeReferences(channelID, toRemove);
      }

      if (loadedReliable)
      {
         // If we loaded any reliable refs then we must mark them as loaded in
         // the store
         // otherwise they may get loaded again, the next time we do a load
         // We can't delete them since they're reliable and haven't been acked
         // yet
         pm.updateReliableReferencesLoadedInRange(channelID, firstOrdering,
                  lastOrdering);
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

   protected void removeCompletely(MessageReference r)
   {
      if (recoverable && r.isReliable())
      {
         try
         {
            pm.removeReference(channelID, r, null);
         }
         catch (Exception e)
         {
            if (trace)
            {
               log.trace("removeAll() failed on removing " + r, e);
            }
         }
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class InMemoryCallback implements TxCallback, Runnable
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

      private boolean committing;

      private Future result;

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

            // prompt delivery
            if (receiversReady)
            {
               deliverInternal();
            }

            result.setResult(null);
         }
         catch (Throwable t)
         {
            result.setException(t);
         }
      }

      public void afterCommit(boolean onePhase) throws TransactionException
      {
         // We don't execute the commit directly, we add it to the event queue
         // of the channel
         // so it is executed in turn
         committing = true;

         executeAndWaitForResult();
      }

      public void afterRollback(boolean onePhase) throws TransactionException
      {
         // We don't execute the commit directly, we add it to the event queue
         // of the channel
         // so it is executed in turn
         committing = false;

         executeAndWaitForResult();
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
            log.warn("Thread interrupted", e);
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

      private void doAfterCommit() throws TransactionException
      {
         // We add the references to the state

         Iterator iter = refsToAdd.iterator();

         while (iter.hasNext())
         {
            MessageReference ref = (MessageReference) iter.next();

            if (trace)
            {
               log.trace(this + ": adding " + ref
                                 + " to non-recoverable state");
            }

            try
            {
               addReferenceInMemory(ref);
            }
            catch (Throwable t)
            {
               // FIXME - Sort out this exception handling
               log.error("Failed to add reference", t);
            }
         }

         // Remove deliveries

         iter = this.deliveriesToRemove.iterator();

         while (iter.hasNext())
         {
            Delivery del = (Delivery) iter.next();

            if (trace)
            {
               log.trace(this + " removing " + del + " after commit");
            }

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
    * internally. Useful to get rid of the REMOTE_ROUTABLE header in a
    * distributed case, for example.
    */
   protected void processMessageBeforeStorage(MessageReference reference)
   {
      // by default a noop
   }

   // Private -------------------------------------------------------

   /**
    * Pushes the reference to <i>a</i> receiver, as dictated by the routing
    * policy
    */
   private Delivery push(MessageReference ref)
   {
      Delivery d = null;

      Set deliveries = router.handle(this, ref, null);

      if (deliveries.isEmpty())
      {
         return null;
      }

      // TODO
      // Sanity check - we shouldn't get more then one delivery - the Channel
      // can only cope with
      // one delivery per message reference at any one time. Eventually this
      // will be enforced in
      // the design of the core classes but for now we just throw an Exception
      if (deliveries.size() > 1)
      {
         throw new IllegalStateException(
                  "More than one delivery returned from router!");
      }

      d = (Delivery) deliveries.iterator().next();

      return d;
   }
   
   private void checkClosed()
   {
      if (router == null)
      {
         throw new IllegalStateException(this + " closed");
      }
   }

   // Inner classes -------------------------------------------------

   private class DeliveryRunnable implements Runnable
   {
      Future result;
      
      DeliveryRunnable(Future result)
      {
         this.result = result;
      }
      
      public void run()
      {
         receiversReady = true;
         deliverInternal();
         if (result != null)
         {
            result.setResult(null);
         }
      }
   }

   private class HandleRunnable implements Runnable
   {
      Future result;

      DeliveryObserver sender;

      Routable routable;

      Transaction tx;

      HandleRunnable(Future result, DeliveryObserver sender, Routable routable,
               Transaction tx)
      {
         this.result = result;
         this.sender = sender;
         this.routable = routable;
         this.tx = tx;
      }

      public void run()
      {
         Delivery d = handleInternal(sender, routable, tx);

         result.setResult(d);
      }
   }

}
