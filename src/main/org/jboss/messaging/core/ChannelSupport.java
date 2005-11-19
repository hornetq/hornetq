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
public class ChannelSupport implements Channel
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

   public final Delivery handle(DeliveryObserver sender, Routable r, Transaction tx)
   {
      if (r == null)
      {
         return null;
      }

      if (log.isTraceEnabled()){ log.trace(this + " handles " + r + (tx == null ? " non-transactionally" : " in transaction: " + tx) ); }

      MessageReference ref = ref(r);

      if (tx == null)
      {
         return handleNoTx(sender, null, r);
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
         state.remove(d, tx);
      }
      catch (Throwable t)
      {
         log.error("Failed to remove delivery " + d + " from state", t);
      }
   }



   public boolean cancel(Delivery d) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("cancel " + d); }

      if (!state.remove(d, null))
      {
         return false;
      }

      if (log.isTraceEnabled()) { log.trace(this + " canceled delivery " + d); }

      MessageReference ref = d.getReference();

      // mark it as redelivered
      ref.setRedelivered(true);

      // add the message at the top of the list
      state.addFirst(ref);

      if (log.isTraceEnabled()) { log.trace(this + " marked message " + d.getReference() + " as undelivered"); }
      return true;
   }

   public void redeliver(Delivery old, Receiver r) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace(this + " redelivery request for delivery " + old + " by receiver " + r); }

      // TODO must be done atomically
   
      if (state.remove(old, null))
      {
         if (log.isTraceEnabled()) { log.trace(this + " old delivery was active, canceled it"); }
         
         MessageReference ref = old.getReference();
         
         //FIXME - What if the message is only redelivered for one particular
         //receiver - won't this set it globally?
         if (log.isTraceEnabled()) { log.trace("Setting redelivered to true"); }
         ref.setRedelivered(true);

         Delivery newd = r.handle(this, ref, null);

         if (newd == null || newd.isDone())
         {
            return;
         }

         // TODO race condition: what if the receiver acknowledges right here v ?
         state.add(newd);
      }
   }

   // Distributor implementation ------------------------------------

   public boolean add(Receiver r)
   {
      if (log.isTraceEnabled()) { log.trace(this + ": attempting to add receiver to channel[" + getChannelID() + "]: " + r); }
      
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

   public void deliver()
   {
      if (log.isTraceEnabled()){ log.trace("attempting to deliver " + this + "'s messages"); }

      List messages = state.undelivered(null);
      for(Iterator i = messages.iterator(); i.hasNext(); )
      {

         MessageReference r = (MessageReference)i.next();

         try
         {

            state.remove(r);
         }
         catch (Throwable t)
         {
            log.error("Failed to remove ref", t);
            return;
         }

         // TODO for a reliable message this is dangerous, I can lose the persistent message if I crash now?
         if (log.isTraceEnabled()){ log.trace("removed " + r + " from state"); }
         
         handleNoTx(null, null, r);
      }
   }

   public void deliver(Receiver r)
   {
      if (log.isTraceEnabled()){ log.trace(r + " requested delivery on " + this); }

      try
      {
         MessageReference ref = state.remove();

         // TODO for a reliable message this is dangerous, I can lose the persistent message if I crash now?

         handleNoTx(null, r, ref);
      }
      catch(Throwable t)
      {
         log.error("delivery failed", t);
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
      
      if (r.isReference())
      {
         return (MessageReference)r;
      }
      else
      {
         //Convert to reference
         try
         {
            ref = ms.reference(r);
            return ref;
         }
         catch (Throwable t)
         {
            log.error("Failed to reference routable", t);
            return null;
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

   /**
    * @param sender - may be null, in which case the returned acknowledgment will probably be ignored.
    */
   private Delivery handleNoTx(DeliveryObserver sender, Receiver receiver, Routable r)
   {
      checkClosed();

      if (r == null)
      {
         return null;
      }

      // don't even attempt synchronous delivery for a reliable message when we have an
      // non-recoverable state that doesn't accept reliable messages. If we do, we may get into the
      // situation where we need to reliably store an active delivery of a reliable message, which
      // in these conditions cannot be done.

      if (r.isReliable() && !state.acceptReliableMessages())
      {
         log.error("Cannot handle reliable message " + r +
                   " because the channel has a non-recoverable state!");
         return null;
      }

      if (log.isTraceEnabled()){ log.trace(this + " handling non-transactionally " + r); }

      MessageReference ref = ref(r);
      Set deliveries = Collections.EMPTY_SET;

      if (receiver == null)
      {
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

      if (deliveries.isEmpty())
      {
         // no receivers, receivers that don't accept the message or broken receivers
         
         if (log.isTraceEnabled()){ log.trace("No deliveries returned for message; there are no receivers"); }
         
         try
         {
            state.add(ref, null);
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
                  state.add(d);
               }
            }
          
         }
         catch(Throwable t)
         {
            log.error(this + " cannot manage delivery, passing responsibility to the sender", t);

            // cannot manage this delivery, pass the responsibility to the sender
            // cannot split delivery, because in case of crash, the message must be recoverable
            // from one and only one channel

            // TODO this is untested
            return new CompositeDelivery(sender, deliveries);
         }
         
      }

      // the channel can safely assume responsibility for delivery
      return new SimpleDelivery(true);
   }


   private void acknowledgeNoTx(Delivery d)
   {
      checkClosed();

      if (log.isTraceEnabled()){ log.trace("acknowledging non transactionally " + d); }

      try
      {
         if (state.remove(d, null))
         {
            if (log.isTraceEnabled()) { log.trace(this + " delivery " + d + " completed and forgotten"); }
         }
      }
      catch(Throwable t)
      {
         // a non transactional remove shound't throw any transaction
         log.error(this + " failed to remove delivery", t);
      }
   }

   // Inner classes -------------------------------------------------

}
