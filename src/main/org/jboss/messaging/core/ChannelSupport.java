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

import org.jboss.logging.Logger;
import org.jboss.messaging.core.tx.Transaction;

import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Collections;
import java.io.Serializable;

/**
 * A basic channel implementation. It supports atomicity, isolation and, if a non-null
 * PersistenceManager is available, it supports recoverability of reliable messages.
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
   protected PersistenceManager pm;
   protected MessageStore ms;

   // Constructors --------------------------------------------------

   /**
    * @param acceptReliableMessages - it only makes sense if pm is null. Otherwise ignored (a
    *        recoverable channel always accepts reliable messages)
    */
   protected ChannelSupport(Serializable channelID,
                            MessageStore ms,
                            PersistenceManager pm,
                            boolean acceptReliableMessages)
   {
      if (log.isTraceEnabled()) { log.trace("creating " + (pm != null ? "recoverable " : "non-recoverable ") + "channel[" + channelID + "]"); }

      this.channelID = channelID;
      this.ms = ms;
      this.pm = pm;
      if (pm == null)
      {
         state = new NonRecoverableState(this, acceptReliableMessages);
      }
      else
      {
         state = new RecoverableState(this, pm);
         // acceptReliableMessage ignored, the channel alwyas accepts reliable messages
      }
   }


   // Receiver implementation ---------------------------------------

   public Delivery handle(DeliveryObserver sender, Routable r, Transaction tx)
   {
      if (r == null)
      {
         return null;
      }

      if (log.isTraceEnabled()){ log.trace(this + " handles " + r + (tx == null ? " non-transactionally" : " in transaction: " + tx) ); }

      MessageReference ref = ref(r);

      if (tx == null)
      {
         Delivery del = push(null, ref);
         if (del == null)
         {
            //Not handled
            ref.release();
         }
         return del;
      }

      if (log.isTraceEnabled()){ log.trace("adding " + ref + " to state " + (tx == null ? "non-transactionally" : "in transaction: " + tx) ); }

      try
      {
         state.add(ref, tx);
      }
      catch (Throwable t)
      {
         log.error("Failed to add message reference " + ref + " to state", t);
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

   public boolean cancel(Delivery d) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("cancel " + d); }
      
      state.cancel(d);
      
      if (log.isTraceEnabled()) { log.trace(this + " marked message " + d.getReference() + " as undelivered"); }
      
      return true;
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
      return router.remove(r);
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
      return deliver(this, r);
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

   protected boolean deliver(DeliveryObserver sender, Receiver receiver)
   {
      checkClosed();

      //This removes the reference at the head of the queue from the in memory state.
      //Note: It *only* removes it from the in-memory NonRecoverableState - it does
      //not remove it from the persistent state.
      //This means that if the server crashes shortly after removing it, when it recovers
      //the message is still in the state and will be redelivered. :)
      MessageReference ref = state.removeFirst();

      if (!checkRef(ref))
      {
         return false;
      }

      if (log.isTraceEnabled()){ log.trace(this + " delivering " + ref); }

      Set deliveries = getDeliveries(receiver, ref);

      if (deliveries.isEmpty())
      {
         // no receivers, receivers that don't accept the message or broken receivers

         if (log.isTraceEnabled()){ log.trace(this + ": no deliveries returned for message; there are no receivers"); }

         //The message wasn't delivered - so we replace the message back to the front of the queue
         //Note we only do this to the in-memory nonrecoverable state!
         //This is not done to persistent state
         state.replaceFirst(ref);

         return false;
      }
      else
      {
         // there are receivers
         try
         {
            Set toAdd = new HashSet();

            for (Iterator i = deliveries.iterator(); i.hasNext(); )
            {
               Delivery d = (Delivery)i.next();
               if (!d.isDone())
               {
                  toAdd.add(d);
               }
            }

            //Atomically remove the ref and add the deliveries
            state.redeliver(toAdd);

            return true;
         }
         catch(Throwable t)
         {
            log.error(this + " cannot manage redelivery", t);
            return false;
         }
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

   // Private -------------------------------------------------------

   private void checkClosed()
   {
      if (state == null)
      {
         throw new IllegalStateException(this + " closed");
      }
   }
   
   private Set getDeliveries(Receiver receiver, MessageReference ref)
   {
      Set deliveries = Collections.EMPTY_SET;
      
      if (receiver == null)
      {
         if (log.isTraceEnabled()){ log.trace(this + " passing " + ref + " to router " + router); }
         deliveries = router.handle(this, ref, null);
      }
      else
      {
         try
         {
            Delivery d = receiver.handle(this, ref, null);

            if (d != null && !d.isCancelled())
            {
               deliveries = new HashSet(1);
               deliveries.add(d);
            }
         }
         catch(Throwable t)
         {
            // broken receiver - log the exception and ignore it
            log.error("The receiver " + receiver + " is broken", t);
         }
      }
      
      Iterator iter = deliveries.iterator();
      while (iter.hasNext())
      {         
         ref.incrementDeliveryCount();
         iter.next();
      }
      
      return deliveries;      
   }
   
   private boolean checkRef(MessageReference ref)
   {
      if (ref == null)
      {
         return false;
      }

      // don't even attempt synchronous delivery for a reliable message when we have an
      // non-recoverable state that doesn't accept reliable messages. If we do, we may get into the
      // situation where we need to reliably store an active delivery of a reliable message, which
      // in these conditions cannot be done.

      if (ref.isReliable() && !state.acceptReliableMessages())
      {
         log.error("Cannot handle reliable message " + ref +
                   " because the channel has a non-recoverable state!");
         return false;
      }
      
      return true;
   }

   private Delivery push(Receiver receiver, MessageReference ref)
   {
      checkClosed();
      
      if (!checkRef(ref))
      {
         return null;
      }
      
      Set deliveries = getDeliveries(receiver, ref);
      
      if (deliveries.isEmpty())
      {
         // no receivers, receivers that don't accept the message or broken receivers         
         if (log.isTraceEnabled()){ log.trace(this + ": no deliveries returned for message; there are no receivers"); }

         processMessageBeforeStorage(ref);

         try
         {                        
            state.add(ref);

            if (log.isTraceEnabled()){ log.trace("adding reference to state successfully"); }
         }
         catch(Throwable t)
         {
            // this channel cannot safely hold the message, so it doesn't accept it
            log.error("Cannot handle the message", t);
            return null;
         }
      }
      else
      {
         // there are receivers
         try
         {            
            for (Iterator i = deliveries.iterator(); i.hasNext(); )
            {
               Delivery d = (Delivery)i.next();
               if (!d.isDone())
               {
                  state.deliver(d);                                   
               }
            }          
         }
         catch(Throwable t)
         {
            // TODO: this should be done atomically
            log.error(this + " failed to manage the delivery, rejecting the message", t);
            return null;
         }
      }

      // the channel can safely assume responsibility for delivery
      return new SimpleDelivery(true);
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
