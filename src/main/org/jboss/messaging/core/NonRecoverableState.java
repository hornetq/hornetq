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

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;


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

   // List <MessageReference>
   protected List messageRefs;
   // List <Delivery>
   protected List deliveries;

   protected Map txToAddReferenceTasks;
   protected Map txToRemoveDeliveryTasks;

   protected Channel channel;
   protected boolean acceptReliableMessages;

   // Constructors --------------------------------------------------

   public NonRecoverableState(Channel channel, boolean acceptReliableMessages)
   {
      this.channel = channel;
      this.acceptReliableMessages = acceptReliableMessages;
      messageRefs = Collections.synchronizedList(new ArrayList());
      deliveries = Collections.synchronizedList(new ArrayList());
      txToAddReferenceTasks = new ConcurrentReaderHashMap();
      txToRemoveDeliveryTasks = new ConcurrentReaderHashMap();
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

   public void add(MessageReference ref, Transaction tx) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("adding " + ref + (tx == null ? " non-transactionally" : " in transaction: " + tx)); }

      if (tx == null)
      {
         if (ref.isReliable() && !acceptReliableMessages)
         {
            throw new IllegalStateException("Reliable reference " + ref +
                                            " cannot be added to non-recoverable state");
         }

         messageRefs.add(ref);
         if (log.isTraceEnabled()) { log.trace("added " + ref + " in memory [" + messageRefs.size() + "]"); }
         return;
      }

      // transactional add

      if (ref.isReliable() && !acceptReliableMessages)
      {
         // this transaction has no chance to succeed, since a reliable message cannot be
         // safely stored by a non-recoverable state, so doom the transaction
         if (log.isTraceEnabled()) { log.trace("cannot handle reliable messages, dooming the transaction"); }
         tx.setRollbackOnly();
      }
      else
      {
         //Transactional so add to post commit task
         AddReferenceTask task = addAddReferenceTask(tx);
         task.addReference(ref);
         if (log.isTraceEnabled()) { log.trace("added transactionally " + ref + " in memory"); }
      }
   }

   public void addFirst(MessageReference ref) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("adding " + ref + "at the top of the list in memory"); }

      if (ref.isReliable() && !acceptReliableMessages)
      {
         throw new IllegalStateException("Reliable reference " + ref +
                                         " cannot be added to non-recoverable state");
      }

      messageRefs.add(0, ref);
      if (log.isTraceEnabled()) { log.trace("added " + ref + " at the top of the list in memory [" + messageRefs.size() + "]"); }
      return;
   }

   public boolean remove(MessageReference ref) throws Throwable
   {
      boolean removed = messageRefs.remove(ref);
      if (removed && log.isTraceEnabled()) { log.trace("removed " + ref + " from memory [" + messageRefs.size() + "]"); }

      return removed;
   }

   public MessageReference remove() throws Throwable
   {
      MessageReference result = null;
      if (!messageRefs.isEmpty())
      {
         result = (MessageReference)messageRefs.remove(0);
      }

      if (log.isTraceEnabled()) { log.trace("removing the oldest message in memory returns " + result); }
      return result;
   }

   public void add(Delivery d) throws Throwable
   {
      if (d.getReference().isReliable() && !acceptReliableMessages)
      {
         throw new IllegalStateException("Delivery " + d + " of a reliable reference " +
                                         " cannot be added to non-recoverable state");
      }

      // Note! Adding of deliveries to the state is NEVER done in a transactional context.
      // The only things that are done in a transactional context are sending of messages
      // and removing deliveries (acking).
      
      deliveries.add(d);
      if (log.isTraceEnabled()) { log.trace("added " + d + " to memory"); }
   }

   public boolean remove(Delivery d, Transaction tx) throws Throwable
   {
      if (tx != null)
      {
         //Transactional so add a post commit task to remove after tx commit
         RemoveDeliveryTask task = addRemoveDeliveryTask(tx);
         task.addDelivery(d);
         if (log.isTraceEnabled()) { log.trace("added " + d + " to memory on transaction " + tx); }
         return true;
      }

      boolean memory = deliveries.remove(d);
      if (memory && log.isTraceEnabled()) { log.trace("removed " + d + " from memory"); }
      return memory;
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
      if (log.isTraceEnabled()) {  log.trace("the non-recoverable state has " + delivering.size() + " messages being delivered"); }
      return delivering;
   }

   public List undelivered(Filter filter)
   {
      List undelivered = new ArrayList();
      synchronized(messageRefs)
      {
         for(Iterator i = messageRefs.iterator(); i.hasNext(); )
         {
            MessageReference r = (MessageReference)i.next();

            // TODO: I need to dereference the message each time I apply the filter. Refactor so the message reference will also contain JMS properties
            if (filter == null || filter.accept(r.getMessage()))
            {
               undelivered.add(r);
            }
            else
            {
               if (log.isTraceEnabled()) { log.trace(r + " NOT accepted by filter so won't add to list"); }
            }
         }
      }
      if (log.isTraceEnabled()) { log.trace("undelivered() returns a list of " + undelivered.size() + " undelivered memory messages"); }
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

   // Public --------------------------------------------------------
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   /**
    * Add an AddRefsTask, if it doesn't exist already and return its handle.
    */
   protected AddReferenceTask addAddReferenceTask(Transaction tx)
   {
      Long key = new Long(tx.getID());
      AddReferenceTask task = (AddReferenceTask)txToAddReferenceTasks.get(key);
      if (task == null)
      {
         task = new AddReferenceTask();
         txToAddReferenceTasks.put(key, task);
         tx.addPostCommitTask(task);
      }
      return task;
   }

   /**
    * Add a RemoveDeliveryTask, if it doesn't exist already and return its handle.
    */
   protected RemoveDeliveryTask addRemoveDeliveryTask(Transaction tx)
   {
      Long key = new Long(tx.getID());
      RemoveDeliveryTask task = (RemoveDeliveryTask)txToRemoveDeliveryTasks.get(key);
      if (task == null)
      {
         task = new RemoveDeliveryTask();
         txToRemoveDeliveryTasks.put(key, task);
         tx.addPostCommitTask(task);
      }
      return task;
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------  
   
   public class AddReferenceTask implements Runnable
   {
      private List refs = new ArrayList();

      void addReference(MessageReference ref)
      {
         refs.add(ref);
      }

      public void run()
      {
         for(Iterator i = refs.iterator(); i.hasNext(); )
         {
            MessageReference ref = (MessageReference)i.next();
            if (log.isTraceEnabled()) { log.trace("adding " + ref + " to non-recoverable state"); }
            messageRefs.add(ref);
         }

         channel.deliver();
      }            
   }


   public class RemoveDeliveryTask implements Runnable
   {
      private List dels = new ArrayList();

      void addDelivery(Delivery d)
      {
         dels.add(d);
      }

      public void run()
      {
         for(Iterator i = dels.iterator(); i.hasNext(); )
         {
            Delivery d = (Delivery)i.next();
            if (log.isTraceEnabled()) { log.trace("removing " + d + " from non-recoverable state"); }
            deliveries.remove(d);
         }
      }            
   }

   
}
