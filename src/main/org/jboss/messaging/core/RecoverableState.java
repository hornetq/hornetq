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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.tx.Transaction;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RecoverableState extends NonRecoverableState
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RecoverableState.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private PersistenceManager pm;
   private Serializable channelID;
   private Serializable storeID;
   private MessageStore messageStore;

   // Constructors --------------------------------------------------

   public RecoverableState(Channel channel, PersistenceManager pm)
   {
      super(channel, true);
      if (pm == null)
      {
          throw new IllegalArgumentException("RecoverableState requires a " +
                                             "non-null persistence manager");
      }
      this.pm = pm;

      // the channel isn't going to change, so cache its id
      this.channelID = channel.getChannelID();
      
      this.storeID = channel.getMessageStore().getStoreID();
      
      this.messageStore = channel.getMessageStore();
      
   }

   // NonRecoverableState overrides -------------------------------------

   public boolean isRecoverable()
   {
      return true;
   }
   
   public void add(MessageReference ref, Transaction tx) throws Throwable
   {    
      super.add(ref, tx);

      if (ref.isReliable())
      {
         //Reliable message in a recoverable state - also add to db
         if (log.isTraceEnabled()) { log.trace("adding " + ref + (tx == null ? " to database non-transactionally" : " in transaction: " + tx)); }
         pm.addReference(channelID, ref, tx);
      }

      addAddReferenceCallback(tx); //FIXME What is the point of this??
      if (log.isTraceEnabled()) { log.trace("added an Add callback to transaction " + tx); }            
   }

   public void add(MessageReference ref) throws Throwable
   {  
      super.add(ref);

      if (ref.isReliable())
      {
         //Reliable message in a recoverable state - also add to db
         if (log.isTraceEnabled()) { log.trace("adding " + ref + " to database non-transactionally"); }
         pm.addReference(channelID, ref, null);
      }      
   }

   public void deliver(Delivery d) throws Throwable
   {
      // Note! Adding of deliveries to the state is NEVER done in a transactional context.
      // The only things that are done in a transactional context are sending of messages and
      // removing deliveries (acking).
        
      super.deliver(d);

      if (d.getReference().isReliable())
      {
         // also add delivery to persistent storage (reliable delivery in recoverable state)
         pm.deliver(channelID, d);
         if (log.isTraceEnabled()) { log.trace("added " + d + " to database"); }
      }      
   }
   
   /*
    * We have redelivered a pre-existing message, so we remove the ref and add the deliveries to state
    */
   public void redeliver(Set dels) throws Throwable
   {
      super.redeliver(dels);

      //TODO (BUG): how about a set of mixed deliveries, when some of them are reliable and some of
      //            them are not? If only the first delivery in the set is reliable, and the rest
      //            are not pm will be requested to redeliver non-reliable messages. Add a test case
      //            for this.
      
      // Solution to this is to restrict a Channel to having only one receiver then we only ever deal with
      // one delivery - this will also significantly simplify other parts of the code.
      // Allowing a Channel to have multiple receivers adds extra complexity and reduces
      // clarity and manageability.
      // It is my view that we should choose the simplest primitives and built up from there.
      // A channel with multiple receivers is not the simplest primitive.      

      if (dels.isEmpty())
      {
         return;
      }

      boolean isReliable = ((Delivery)dels.iterator().next()).getReference().isReliable();
      
      if (isReliable)
      {
         pm.redeliver(channelID, dels);
      }
   }
   
   /*
    * Cancel an outstanding delivery.
    * This removes the delivery and adds the message reference back into the state
    */
   public void cancel(Delivery del) throws Throwable
   {
      super.cancel(del);
      
      if (del.getReference().isReliable())
      {
         pm.cancel(channelID, del);
      }
   }
   
   public void acknowledge(Delivery d, Transaction tx) throws Throwable
   {      
      super.acknowledge(d, tx);

      if (d.getReference().isReliable())
      {
         pm.acknowledge(channelID, d, tx);            
      }
     
   }
   
   public void acknowledge(Delivery d) throws Throwable
   {   
      super.acknowledge(d);

      if (d.getReference().isReliable())
      {
         pm.acknowledge(channelID, d, null);            
      }     
   }
   
   public MessageReference removeFirst()
   {
      //We do not remove from persistent state
      return super.removeFirst();
   }
   
   public void replaceFirst(MessageReference ref)
   {
      //We do not replace in persistent state
      super.replaceFirst(ref);
   }
   
   public void clear()
   {
      super.clear();
      pm = null;
      // the persisted state remains
   }
   
   /**
    * Load the state from persistent storage
    */
   public void load() throws Exception
   {
      List refs = pm.messageRefs(storeID, channelID);
      
      Set ids = new HashSet();
      
      Iterator iter = refs.iterator();
      while (iter.hasNext())
      {
         String messageID = (String)iter.next();
         
         MessageReference ref = messageStore.reference(messageID);
         
         messageRefs.addLast(ref, ref.getPriority());
         
         ids.add(messageID);         
      }
      
      //Important note. When we reload deliveries we load them as MessageReferences,
      //*not* as Delivery instances.
      //This is because, after recovery, it is not possible that those Delivery instances
      //can be acknowledged, since the receivers no longer exist.
      //Therefore by loading the Deliveries as MessageReferences we are effectively
      //cancelling them
                  
      List dels = pm.deliveries(storeID, channelID);
      iter = dels.iterator();
      while (iter.hasNext())
      {
         String messageID = (String)iter.next();
         
         if (ids.add(messageID))
         {
            MessageReference ref = messageStore.reference(messageID);
            
            messageRefs.addFirst(ref, ref.getPriority());            
         }
      }
      
   }

   // Public --------------------------------------------------------
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
