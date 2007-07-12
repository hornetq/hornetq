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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.jboss.jms.server.MessagingTimeoutFactory;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Channel;
import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.DeliveryObserver;
import org.jboss.messaging.core.contract.Distributor;
import org.jboss.messaging.core.contract.Filter;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.messaging.core.impl.tx.TransactionException;
import org.jboss.messaging.core.impl.tx.TxCallback;
import org.jboss.messaging.util.prioritylinkedlist.BasicPriorityLinkedList;
import org.jboss.messaging.util.prioritylinkedlist.PriorityLinkedList;
import org.jboss.util.timeout.Timeout;
import org.jboss.util.timeout.TimeoutTarget;

import EDU.oswego.cs.dl.util.concurrent.SynchronizedInt;

/**
 * 
 * This class provides much of the functionality needed to implement a channel.
 * 
 * This partial implementation supports atomicity, isolation and recoverability for reliable messages.
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
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

   protected Distributor distributor;

   protected MessageStore ms;

   protected boolean receiversReady;

   protected PriorityLinkedList messageRefs;

   protected boolean recoverable;

   protected PersistenceManager pm;

   protected Object lock;

   protected volatile boolean active;
   
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

   protected ChannelSupport(long channelID, MessageStore ms, PersistenceManager pm,
                            boolean recoverable, int maxSize)
   {
      if (trace) { log.trace("creating " + (pm != null ? "recoverable " : "non-recoverable ") + "channel[" + channelID + "]"); }

      this.ms = ms;

      this.pm = pm;

      this.channelID = channelID;

      this.recoverable = recoverable;

      messageRefs = new BasicPriorityLinkedList(10);

      lock = new Object();
      
      deliveringCount = new SynchronizedInt(0);
      
      scheduledDeliveries = new HashSet();
      
      this.maxSize = maxSize;
      
      messagesAdded = new SynchronizedInt(0);
   }
   
   // Receiver implementation ----------------------------------------------------------------------

   public Delivery handle(DeliveryObserver sender, MessageReference ref, Transaction tx)
   {
      if (!isActive())
      {	
      	if (trace) { log.trace(this + " is not active, returning null delivery for " + ref); }
      	
         return null;
      }

      checkClosed();
      
      return handleInternal(sender, ref, tx, true);      
   }

   // DeliveryObserver implementation --------------------------------------------------------------

   public void acknowledge(Delivery d, Transaction tx) throws Throwable
   {
      if (trace) { log.trace("acknowledging " + d + (tx == null ? " non-transactionally" : " transactionally in " + tx)); }

      acknowledgeInternal(d, tx, true);
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
         cancelInternal(ref);
      }
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

   public List browse(Filter filter)
   {
      if (trace) { log.trace(this + " browse" + (filter == null ? "" : ", filter = " + filter)); }

      synchronized (lock)
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
 
   public void deliver()
   {
      checkClosed();
      
      synchronized (lock)
      {      
	      if (distributor != null && distributor.getNumberOfReceivers() > 0)
	      {         
	         setReceiversReady(true);
	            
	         deliverInternal();                  
	      }
      }     
   }      

   public void close()
   {
   	synchronized (lock)
   	{
	      if (distributor != null)
	      {
	         distributor.clear();
	         
	         distributor = null;
	      }
	      
	      clearAllScheduledDeliveries();
   	}
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
      log.debug(this + " removing all references");
      
      synchronized (lock)
      {
         if (deliveringCount.get() > 0)
         {
            throw new IllegalStateException("Cannot remove references while deliveries are in progress, there are " +
            		                           deliveringCount.get());
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
            SimpleDelivery del = new SimpleDelivery(this, ref);

            del.acknowledge(null);

            // Delivery#acknowledge decrements the deliveringCount without incrementing it first (because
            // deliver has actually never been called), so increment it here to be accurate.  
            deliveringCount.increment();
         }
      }
      
      clearAllScheduledDeliveries();
   }

   public List undelivered(Filter filter)
   {
      List undelivered = new ArrayList();

      synchronized (lock)
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
      synchronized (lock)
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
      active = true;               
   }

   public void deactivate()
   {
      active = false;               
   }

   public boolean isActive()
   {
      return active;               
   }   
   
   public int getMaxSize()
   {
      synchronized (lock)
      {
         return maxSize;
      }
   }
   
   public void setMaxSize(int newSize)
   {
      synchronized (lock)
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
      synchronized (lock)
      {
         return messageRefs.size();
      }
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

      synchronized (lock)
      {
         messageRefs.addFirst(ref, ref.getMessage().getPriority());
      }
                  
      if (trace) { log.trace(this + " added " + ref + " back into state"); }
   }
   
   /**
    * This methods delivers as many messages as possible to the distributor until no more deliveries are
    * returned. This method should never be called at the same time as handle.
    *
    * @see org.jboss.messaging.core.contract.Channel#deliver()
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
         
         if (!getReceiversReady())
         {
         	if (trace) { log.trace(this + " receivers not ready so not delivering"); }
         	
            return;
         }
         
         while (true)
         {           
            ref = nextReference(iter);               
                     
            if (ref != null)
            {
               // Attempt to push the ref to a receiver
               
               if (trace) { log.trace(this + " pushing " + ref); }   
               
               Delivery del = distributor.handle(this, ref, null);
               
               setReceiversReady(del != null);
               
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
                     
                     //We just tried the first one, so we don't want to try it again
                     iter.next();
                  }                     
               }
               else
               {
                  if (trace) { log.trace(this + ": " + del + " returned for message " + ref); }
                  
                  // Receiver accepted the reference

                  synchronized (lock)
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
         synchronized (lock)
         {
            // Attempt to push the ref to a receiver
            
            if (trace) { log.trace(this + " pushing " + ref); }                                  
   
            Delivery del = distributor.handle(this, ref, null);
   
            setReceiversReady(del != null);
            
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
                                     Transaction tx, boolean persist)
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
               synchronized (lock)
               {
                  addReferenceInMemory(ref);
                  
                  deliverInternal();
               }            
            }
         }
         else
         {
            if (trace) { log.trace(this + " adding " + ref + " to state " + (tx == null ? "non-transactionally" : "in transaction: " + tx)); }

            // add to post commit callback
            getCallback(tx).addRef(ref);
            
            if (trace) { log.trace(this + " added transactionally " + ref + " in memory"); }
            
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

      return new SimpleDelivery(this, ref, true);
   }

   
   protected boolean checkAndSchedule(MessageReference ref)
   {
      if (ref.getScheduledDeliveryTime() > System.currentTimeMillis())
      {      
         if (trace) { log.trace("Scheduling delivery for " + ref + " to occur at " + ref.getScheduledDeliveryTime()); }
         
         // Schedule the cancel to actually occur at the specified time. Need to synchronize to
         // prevent timeout being removed before it is added.
         synchronized (scheduledDeliveries)
         {            
            Timeout timeout =
               MessagingTimeoutFactory.instance.getFactory().
                  schedule(ref.getScheduledDeliveryTime(), new DeliverRefTimeoutTarget(ref));
            
            scheduledDeliveries.add(timeout);
         }      
         
         return true;
      }
      else
      {
         return false;
      }
   }
   
   protected void acknowledgeInternal(Delivery d, Transaction tx, boolean persist) throws Exception
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
         this.getCallback(tx).addDelivery(d);
   
         if (trace) { log.trace(this + " added " + d + " to memory on transaction " + tx); }
   
         if (recoverable && d.getReference().getMessage().isReliable())
         {
            pm.removeReference(channelID, d.getReference(), tx);
         }
      }
   }

   protected InMemoryCallback getCallback(Transaction tx)
   {
      InMemoryCallback callback = (InMemoryCallback) tx.getCallback(this);            

      if (callback == null)
      {
         callback = new InMemoryCallback();

         tx.addCallback(callback, this);
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
      messageRefs.addLast(ref, ref.getMessage().getPriority());

      if (trace){ log.trace(this + " added " + ref + " non-transactionally in memory"); }      
   }    
   
   protected boolean getReceiversReady()
   {
   	return receiversReady;
   }
   
   protected void setReceiversReady(boolean receiversReady)
   {
   	this.receiversReady = receiversReady;
   }
   
   // Private --------------------------------------------------------------------------------------
      
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

   private class InMemoryCallback implements TxCallback
   {
      private List refsToAdd;

      private List deliveriesToRemove;
      
      private InMemoryCallback()
      {
         refsToAdd = new ArrayList();

         deliveriesToRemove = new ArrayList();
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
      
      public void afterCommit(boolean onePhase) throws Exception
      {         
         try
         {
            // We add the references to the state (or schedule them if appropriate)
         	
         	boolean promptDelivery = false;
            
            for(Iterator i = refsToAdd.iterator(); i.hasNext(); )
            {
               MessageReference ref = (MessageReference)i.next();
               
               if (checkAndSchedule(ref))
               {
               	if (trace) { log.trace(this + ": scheduled " + ref); }
               }
               else
               {	
	               if (trace) { log.trace(this + ": adding " + ref + " to memory"); }
	               
	               try
	               {
	                  synchronized (lock)
	                  {
	                     addReferenceInMemory(ref);
	                  }
	               }
	               catch (Throwable t)
	               {
	                  throw new TransactionException("Failed to add reference", t);
	               }
               
	               //Only need to prompt delivery if refs were added
	               promptDelivery = true;
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
            if (promptDelivery)
            {
            	synchronized (lock)
            	{
            		deliverInternal();
            	}
            }
         }
         catch (Throwable t)
         {
            log.error("failed to commit", t);
            throw new Exception("Failed to commit", t);
         }
         
      }

      public void afterRollback(boolean onePhase) throws Exception
      {
         for(Iterator i = refsToAdd.iterator(); i.hasNext(); )
         {
            MessageReference ref = (MessageReference)i.next();

            if (trace) { log.trace(this + " releasing memory " + ref + " after rollback"); }
            ref.releaseMemoryReference();
         }
      }

      public String toString()
      {
         return ChannelSupport.this + ".InMemoryCallback[" +
                Integer.toHexString(InMemoryCallback.this.hashCode()) + "]";
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
      if (distributor == null)
      {
         throw new IllegalStateException(this + " closed");
      }
   }

   // Private --------------------------------------------------------------------------------------
   
   // Inner classes --------------------------------------------------------------------------------
      
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
         
         boolean delivered = false;
         
         if (distributor.getNumberOfReceivers() > 0)
         {               
            delivered = deliverScheduled(ref);     
         }

         if (!delivered)
         {
            try
            {
               cancelInternal(ref);
            }
            catch (Exception e)
            {
               log.error("Failed to cancel", e);
            }
         }
         else
         {
            if (trace) { log.trace("Delivered scheduled delivery at " + System.currentTimeMillis() + " for " + ref); }
         }
      }
   }
}
