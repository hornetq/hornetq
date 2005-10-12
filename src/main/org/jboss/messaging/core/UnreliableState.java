/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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
 * Unreliable (in-memory), non transactional channel state implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class UnreliableState implements State
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(UnreliableState.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected List messageRefs;
   protected List deliveries;

   protected Map txToAddRefTasks;
   protected Map txToRemoveDelTasks;

   protected Channel channel;

   // Constructors --------------------------------------------------

   public UnreliableState(Channel channel)
   {
      this.channel = channel;
      messageRefs = Collections.synchronizedList(new ArrayList());
      deliveries = Collections.synchronizedList(new ArrayList());
      txToAddRefTasks = new ConcurrentReaderHashMap();
      txToRemoveDelTasks = new ConcurrentReaderHashMap();
   }

   // State implementation -----------------------------------

   public boolean isReliable()
   {
      return false;
   }


   public void add(Delivery d) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("adding " + d); }

      //Note! Adding of deliveries to the state is NEVER done
      //in a transactional context.
      //The only things that are done in a transactional context
      //are sending of messages and removing deliveries (acking)
      
      //Add to in-memory cache now
      deliveries.add(d);  
   }

   public boolean remove(Delivery d, Transaction tx) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("removing " + d); }

      if (tx != null)
      {
         //Transactional so add a post commit task to remove after tx commit
         RemoveDeliveryTask task = getRemoveDelsTask(tx);
         task.addDelivery(d);
         return true;
      }
      else
      {
         boolean removed = deliveries.remove(d);
         return removed;
      }
   }

   public void add(MessageReference ref, Transaction tx) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("Adding " + ref); }
      
      if (tx != null)
      {
         //Transactional - add to post commit task
         AddReferenceTask task = getAddRefsTask(tx);
         task.addReference(ref);
      }
      else
      {
         messageRefs.add(ref);
      }
   }

   public boolean remove(MessageReference ref) throws Throwable
   {
      boolean removed = messageRefs.remove(ref);
      return removed;
   }

   public List undelivered(Filter filter)
   {
      if (log.isTraceEnabled())
      {
         log.trace("Getting undelivered unreliable messages");
      }
      List undelivered = new ArrayList();
      synchronized(messageRefs)
      {
         for(Iterator i = messageRefs.iterator(); i.hasNext(); )
         {
            Routable r = (Routable)i.next();
            if (filter == null || filter.accept(r))
            {
               if (log.isTraceEnabled()) { log.trace("Accepted by filter so adding to list"); }
               undelivered.add(r);
            }
            else
            {
               if (log.isTraceEnabled()) { log.trace("NOT Accepted by filter so not adding to list"); }
            }
         }
      }
      if (log.isTraceEnabled())
      {
         log.trace("I have " + undelivered.size() + " undelivered messages");
      }
      return undelivered;
   }

   public List browse(Filter filter)
   {
      List result = delivering(filter);
      
      if (log.isTraceEnabled()) { log.trace("Delivering is: " + result); }
               
      List undel = undelivered(filter);
      
      if (log.isTraceEnabled()) { log.trace("undelivered is: " + undel); }
      
      if (log.isTraceEnabled()) { log.trace("I have " + undel.size() + " undelivered"); }
      result.addAll(undel);
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
            Routable r = d.getReference();
            if (filter == null || filter.accept(r))
            {
               delivering.add(r);
            }
         }
      }
      return delivering;
   }

   public void clear()
   {
      messageRefs.clear();
      messageRefs = null;
      channel = null;
   }

   // Public --------------------------------------------------------
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected AddReferenceTask getAddRefsTask(Transaction tx)
   {
      AddReferenceTask task = (AddReferenceTask)txToAddRefTasks.get(new Long(tx.getID()));
      if (task == null)
      {
         task = new AddReferenceTask();
         txToAddRefTasks.put(new Long(tx.getID()), task);
         tx.addPostCommitTasks(task);
      }
      return task;
   }
   
   protected RemoveDeliveryTask getRemoveDelsTask(Transaction tx)
   {
      RemoveDeliveryTask task = (RemoveDeliveryTask)txToRemoveDelTasks.get(new Long(tx.getID()));
      if (task == null)
      {
         task = new RemoveDeliveryTask();
         txToRemoveDelTasks.put(new Long(tx.getID()), task);
         tx.addPostCommitTasks(task);
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
         Iterator iter = refs.iterator();
         while (iter.hasNext())
         {
            MessageReference ref = (MessageReference)iter.next();

            messageRefs.add(ref);
            
         }
         channel.deliver();
      }            
   }


   public class RemoveDeliveryTask implements Runnable
   {
      private List dels = new ArrayList();

      void addDelivery(Delivery del)
      {
         dels.add(del);
      }

      public void run()
      {  
         Iterator iter = dels.iterator();
         while (iter.hasNext())
         {
            Delivery del = (Delivery)iter.next();
            if (!del.getReference().isReliable())
            {
               deliveries.remove(del);
            }
         }
      }            
   }

   
}
