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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.plugin.contract.TransactionLog;
import org.jboss.jms.server.plugin.contract.MessageStore;

/**
 * A basic channel implementation. It supports atomicity, isolation and, if a non-null
 * TransactionLog is available, it supports recoverability of reliable messages.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> 
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class ChannelSupport implements Channel
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ChannelSupport.class);

   // Static --------------------------------------------------------

    // Attributes ----------------------------------------------------

   protected Serializable channelID;
   protected Router router;
   protected State state;
   protected TransactionLog tl;
   protected MessageStore ms;

   // Constructors --------------------------------------------------

   /**
    * @param acceptReliableMessages - it only makes sense if tl is null. Otherwise ignored (a
    *        recoverable channel always accepts reliable messages)
    */
   protected ChannelSupport(Serializable channelID,
                            MessageStore ms,
                            TransactionLog tl,
                            boolean acceptReliableMessages)
   {
      if (log.isTraceEnabled()) { log.trace("creating " + (tl != null ? "recoverable " : "non-recoverable ") + "channel[" + channelID + "]"); }

      this.channelID = channelID;
      this.ms = ms;
      this.tl = tl;
      if (tl == null)
      {
         state = new NonRecoverableState(this, acceptReliableMessages);
      }
      else
      {
         state = new RecoverableState(this, tl);
         // acceptReliableMessage ignored, the channel alwyas accepts reliable messages
      }
   }


   // Receiver implementation ---------------------------------------

   public Delivery handle(DeliveryObserver sender, Routable r, Transaction tx)
   {            
      checkClosed();
      
      if (r == null)
      {
         return null;
      }

      if (log.isTraceEnabled()){ log.trace(this + " handles " + r + (tx == null ? " non-transactionally" : " in transaction: " + tx) ); }

      // don't even attempt synchronous delivery for a reliable message when we have an
      // non-recoverable state that doesn't accept reliable messages. If we do, we may get into the
      // situation where we need to reliably store an active delivery of a reliable message, which
      // in these conditions cannot be done.

      MessageReference ref = ref(r);
      
      try
      {
         
         if (tx == null)
         {      
            if (r.isReliable() && !state.acceptReliableMessages())
            {
               log.error("Cannot handle reliable message " + r +
                         " because the channel has a non-recoverable state!");
               return null;
            }
            
            //This returns true if the ref was added to an empty ref queue
            boolean first = state.addReference(ref);
                        
            //Previously we would call push() at this point to push the reference to the consumer
            //One of the problems this had was it would end up leap-frogging messages that were
            //already in the queue.
            //So now we add the message to the back of the queue and call deliver()            
            //In fact we only need to call deliver() if the queue was empty before we added
            //the ref.
            //If the queue wasn't empty there would be no active waiting receiver, so there
            //would be no need to call deliver()
            //Once I have improve the locking on the ref queue, this should result in very little (if any)
            //lock contention between the thread depositing the message on the queue and the thread
            //activating the consumer and prompting delivery.
            
            if (first)
            {
               deliver(this, null);         
            }
         }
         else
         {   
            if (log.isTraceEnabled()){ log.trace("adding " + ref + " to state " + (tx == null ? "non-transactionally" : "in transaction: " + tx) ); }
            
            state.addReference(ref, tx);         
         }
      }
      catch (Throwable t)
      {
         log.error("Failed to handle message", t);
         
         ref.release();
         
         return null;
      }

      // I might as well return null, the sender shouldn't care
      return new SimpleDelivery(sender, ref, true);
   }
   

   // DeliveryObserver implementation --------------------------

   public void acknowledge(Delivery d, Transaction tx)
   {
      if (tx == null)
      {
         // acknowledge non transactionally
         acknowledgeNoTx(d);
         return;
      }

      if (log.isTraceEnabled()){ log.trace("acknowledge " + d + (tx == null ? " non-transactionally" : " transactionally in " + tx)); }

      try
      {
         state.acknowledge(d, tx);         
      }
      catch (Throwable t)
      {
         log.error("Failed to remove delivery " + d + " from state", t);
      }
   }

   public void cancel(Delivery d) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("cancel " + d); }
      
      state.cancelDelivery(d);
      
      if (log.isTraceEnabled()) { log.trace(this + " marked message " + d.getReference() + " as undelivered"); }
   }

   // Distributor implementation ------------------------------------

   public boolean add(Receiver r)
   {
      if (log.isTraceEnabled()) { log.trace(this + ": attempting to add receiver " + r); }
      
      boolean added = router.add(r);

      if (log.isTraceEnabled()) { log.trace("receiver " + r + (added ? "" : " NOT") + " added"); }
      
      return added;
   }

   public boolean remove(Receiver r)
   {
      boolean removed = router.remove(r);

      if (log.isTraceEnabled()) { log.trace(this + (removed ? " removed ":" did NOT remove ") + r); }
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

   public Serializable getChannelID()
   {
      return channelID;
   }

   public boolean isRecoverable()
   {
      return state.isRecoverable();
   }
   
   public boolean acceptReliableMessages()
   {
      return state.acceptReliableMessages();
   }

   public List browse()
   {
      return browse(null);
   }

   public List browse(Filter f)
   {
      if (log.isTraceEnabled()) { log.trace(this + " browse" + (f == null ? "" : ", filter = " + f)); }

      List references = state.browse(f);

      // dereference pass
      ArrayList messages = new ArrayList(references.size());
      for(Iterator i = references.iterator(); i.hasNext();)
      {
         MessageReference ref = (MessageReference)i.next();
         messages.add(ref.getMessage());
      }
      return messages;
   }

   public MessageStore getMessageStore()
   {
      return ms;
   }


   public boolean deliver(Receiver r)
   {
      if (log.isTraceEnabled()){ log.trace(r != null ? r + " requested delivery on " + this : "generic delivery requested on " + this); }
      
      checkClosed();
      
      try
      {
         return deliver(this, r);
      }
      catch (Throwable t)
      {
         log.error("Failed to deliver message", t);
         return false;
      }
   }
   
   public void close()
   {
      if (state == null)
      {
         return;
      }

      router.clear();
      router = null;
      state.clear();
      state = null;
      channelID = null;
   }
  
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected MessageReference ref(Routable r)
   {
      MessageReference ref = null;

      //Convert to reference
      try
      {
         if (r.isReference())
         {
            //Make a copy - each channel has it's own copy of the reference - 
            //this is becaause the headers for a particular message may vary depending
            //on what channel it is in - e.g. deliveryCount
            ref = ms.reference((MessageReference)r);
         }
         else
         {
            //Reference it for the first time
            ref = ms.reference((Message)r);
         }
      
         return ref;
      }
      catch (Exception e)
      {
         log.error("Failed to reference routable", e);
         //FIXME - Swallowing exceptions
         return null;
      }      
   }

   /**
    * Give subclass a chance to process the message before storing it internally. Useful to get
    * rid of the REMOTE_ROUTABLE header in a distributed case, for example.
    */
   protected void processMessageBeforeStorage(MessageReference reference)
   {
      // by default a noop
   }
        
   /*
    * Delivery for the channel must be synchronized.
    * Otherwise we can end up with the same message being delivered more than once to the same consumer
    * (if deliver() is called concurrently) or messages being delivered in the wrong order.
    */
   protected synchronized boolean deliver(DeliveryObserver sender, Receiver receiver) throws Throwable
   {

      MessageReference ref = state.peekFirst();
      
      if (ref == null)
      {
         return false;
      }
      
      if (log.isTraceEnabled()){ log.trace(this + " delivering " + ref); }

      Delivery del = getDelivery(receiver, ref);

      if (del == null)
      {
         // no receiver, receiver that doesn't accept the message or broken receiver

         if (log.isTraceEnabled()){ log.trace(this + ": no delivery returned for message" + ref); }

         return false;
      }
      else
      {
         if (log.isTraceEnabled()){ log.trace(this + ": delivery returned for message:" + ref); }
         
         //We must synchronize here to cope with another race condition where message is cancelled/acked
         //in flight while the following few actions are being performed.
         //e.g. delivery could be cancelled acked after being removed from state but before delivery being added
         //(observed)
         synchronized (del)
         {
            if (log.isTraceEnabled()) { log.trace(this + " incrementing delivery count for " + del); }    
            
            //FIXME - It's actually possible the delivery could be cancelled before it reaches
            //here, in which case we wouldn't get a delivery but we still need to increment the
            //delivery count
            del.getReference().incrementDeliveryCount();
                        
            if (del.isCancelled())
            {
               return false;
            }
                        
            state.removeFirst();
            
            // delivered
            if (!del.isDone())
            {
               //Add the delivery to state
               state.addDelivery(del);
            }                                
            
            //Delivery successful
            return true;               
         }
         
      }
   }

   // Private -------------------------------------------------------

   private void checkClosed()
   {
      if (state == null)
      {
         throw new IllegalStateException(this + " closed");
      }
   }
   
   private Delivery getDelivery(Receiver receiver, MessageReference ref)
   {      
      Delivery d = null;
            
      if (receiver == null)
      {
         Set deliveries = router.handle(this, ref, null);
         
         if (deliveries.isEmpty())
         {
            return null;
         }
         
         //Sanity check - we shouldn't get more then one delivery - 
         //the Channel can only cope with one delivery per message reference
         //at any one time.
         //Eventually this will be enforced in the design of the core classes
         //but for now we just throw an Exception
         if (deliveries.size() > 1)
         {
            throw new IllegalStateException("More than one delivery returned from router!");
         }
         
         d = (Delivery)deliveries.iterator().next();
      }
      else
      {
         try
         {
            d = receiver.handle(this, ref, null);                          
         }
         catch(Throwable t)
         {
            // broken receiver - log the exception and ignore it
            log.error("The receiver " + receiver + " is broken", t);
         }
      }            
      
      return d;
   }
   

   private void acknowledgeNoTx(Delivery d)
   {
      checkClosed();

      if (log.isTraceEnabled()){ log.trace(this + " acknowledging non transactionally " + d); }

      try
      {
         //We remove the delivery from the state
         state.acknowledge(d);
         
         if (log.isTraceEnabled()) { log.trace(this + " delivery " + d + " completed and forgotten"); }
         
      }
      catch(Throwable t)
      {
         // a non transactional remove shouldn't throw any transaction
         log.error(this + " failed to remove delivery", t);
      }
   }

   // Inner classes -------------------------------------------------

}
