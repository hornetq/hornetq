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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.refqueue.BasicPrioritizedDeque;
import org.jboss.messaging.core.refqueue.PrioritizedDeque;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TxCallback;
import org.jboss.messaging.core.util.ConcurrentHashSet;


/**
 * In-memory (unrecoverable in case of failure) channel state implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class NonRecoverableState implements State
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(NonRecoverableState.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   protected PrioritizedDeque messageRefs;
   
   protected Set deliveries;

   protected Channel channel;
   
   protected boolean acceptReliableMessages;
   
   protected long messageOrdering;
   
   // Constructors --------------------------------------------------

   public NonRecoverableState(Channel channel, boolean acceptReliableMessages)
   {
      this.channel = channel;
      this.acceptReliableMessages = acceptReliableMessages;
      messageRefs = new BasicPrioritizedDeque(10);
      deliveries = new ConcurrentHashSet();
   }

   // State implementation -----------------------------------

   public boolean isRecoverable()
   {
      return false;
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
         NonRecoverableAddReferenceCallback callback = new NonRecoverableAddReferenceCallback(ref);
         tx.addCallback(callback);
         if (trace) { log.trace(this + " added transactionally " + ref + " in memory"); }
      }  
   }

   public boolean addReference(MessageReference ref) throws Throwable
   {
      if (trace) { log.trace(this + " adding " + ref + " non-transactionally"); }

      if (ref.isReliable() && !acceptReliableMessages)
      {
         throw new IllegalStateException("Reliable reference " + ref +
                                         " cannot be added to non-recoverable state");
      }

      if (trace) { log.trace(this + " added " + ref + " in memory"); } 
      
      boolean first = messageRefs.addLast(ref, ref.getPriority());    
      
      ref.setOrdering(getNextMessageOrdering());
      
      return first;             
   }
  

   public void addDelivery(Delivery d) throws Throwable
   {
      deliveries.add(d);

      if (trace) { log.trace(this + " added " + d + " to memory"); }
   }
   
   /*
    * Cancel an outstanding delivery.
    * This removes the delivery and adds the message reference back into the state
    */
   public void cancelDelivery(Delivery del) throws Throwable
   {
      if (trace) { log.trace(this + " cancelling " + del + " in memory"); }
      
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
         messageRefs.addFirst(del.getReference(), del.getReference().getPriority());
         if (trace) { log.trace(this + " added " + del.getReference() + " back into state"); }
      }
   }
   
   public void acknowledge(Delivery d, Transaction tx) throws Throwable
   {
      // Transactional so add a post commit callback to remove after tx commit
      NonRecoverableRemoveDeliveryCallback callback = new NonRecoverableRemoveDeliveryCallback(d);
      
      tx.addCallback(callback);
      
      if (trace) { log.trace(this + " added " + d + " to memory on transaction " + tx); }
   }
   
   public void acknowledge(Delivery d) throws Throwable
   {
      boolean removed = deliveries.remove(d);
      
      if (removed && trace) { log.trace(this + " removed " + d + " from memory"); }   
   }
   
   public void removeAll()
   {
      // Remove all deliveries
      synchronized(deliveries)
      {
         Iterator iter = deliveries.iterator();
         while (iter.hasNext())
         {
            Delivery d = (Delivery)iter.next();
            MessageReference r = d.getReference();
            removeCompletely(r);
         }
         deliveries.clear();
      }
      // Remove all holding messages
      synchronized(messageRefs)
      {
         Iterator iter = messageRefs.getAll().iterator();
         while (iter.hasNext())
         {
            MessageReference r = (MessageReference)iter.next();
            removeCompletely(r);
         }
         messageRefs.clear();
      }
   }
   
   public MessageReference removeFirst()
   {
      MessageReference result = (MessageReference)messageRefs.removeFirst();

      if (trace) { log.trace(this + " removing the oldest message in memory returns " + result); }
      
      return result;
   }
   
   public MessageReference peekFirst()
   {
      MessageReference result = (MessageReference)messageRefs.peekFirst();

      if (trace) { log.trace(this + " peeking the oldest message in memory returns " + result); }
      return result;
   }
 
   public List delivering(Filter filter)
   {
      List delivering = new ArrayList();
      synchronized (deliveries)
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
      synchronized(messageRefs)
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
      List result = delivering(filter);
      List undel = undelivered(filter);
      
      result.addAll(undel);
      return result;
   }
   
   public void clear()
   {
      messageRefs.clear();
      messageRefs = null;
   }
   
   public void load() throws Exception
   {
      //do nothing
   }
   
   public int messageCount()
   {
      return messageRefs.size() + deliveries.size();
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "State[" + channel + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected void removeCompletely(MessageReference r)
   {
      // Not necessary?
      // r.release();
   }      
   
   // Private -------------------------------------------------------
   
   /*
    * TODO we could reduce contention on this lock by maintaining the ordering count per 
    * ServerProducerEndpoint and with each ServerProducerEndpoint being allocated blocks
    * of ids every so often from the server peer.
    * Let us see whether this is a major synchronization bottleneck first - Tim
    */
   private synchronized long getNextMessageOrdering()
   {
      return ++messageOrdering;
   }
   
   // Inner classes -------------------------------------------------  
   
   private class NonRecoverableAddReferenceCallback implements TxCallback
   {
      private MessageReference ref;
      
      private NonRecoverableAddReferenceCallback(MessageReference ref)
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
         
         boolean first = messageRefs.addLast(ref, ref.getPriority());      
         
         ref.incChannelCount();
         
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
   
   private class NonRecoverableRemoveDeliveryCallback implements TxCallback
   {
      private Delivery del;
      
      private NonRecoverableRemoveDeliveryCallback(Delivery del)
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
         
         deliveries.remove(del);         
         
         del.getReference().decChannelCount();
      } 
      
      public void afterRollback(boolean onePhase)
      {
         //NOOP
      }           
   }   
}
