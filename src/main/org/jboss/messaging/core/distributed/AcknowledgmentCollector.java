/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.messaging.core.MessageStore;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.util.Map;
import java.util.Observer;
import java.util.Observable;
import java.util.Set;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

import EDU.oswego.cs.dl.util.concurrent.Mutex;

/**
 * Keeps lists with messages that need to be acknowledged (one list per ReceiverOutput instance)
 * and attempts retransmission for unacknowledged messages.
 * <p>
 * To enforce exclusive access, you must explicitely aquire the collector's lock.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class AcknowledgmentCollector implements AcknowledgmentCollectorServerDelegate, Observer
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(AcknowledgmentCollector.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   protected ReplicatorPeer peer;

   protected Mutex mutex;

   /** <output peer ID - List of unacked message IDs> */
   protected Map unacked;

   /** Only one instance per the message is kept in store. When the last output peer acknowledges
    * the message, the message is removed from the store. */
   // TODO introduce the concepet of ReliableStore - one that survives the Replicator crash
   protected MessageStore store;


   // Constructors --------------------------------------------------

   public AcknowledgmentCollector(ReplicatorPeer peer)
   {
      this.peer = peer;
      unacked = new HashMap();
      store = new MessageStore();
      mutex = new Mutex();
   }

   // Observer implementation ---------------------------------------

   /**
    * This method is called every time the topology changes. The method is thread safe since it
    * aquires internally the collector's lock.
    *
    * @param topology - the ReplicantTopology instance.
    * @param o - ignored.
    */
   public void update(Observable topology, Object o)
   {
      try
      {
         while(!acquireLock())
         {
            log.warn("could not aquire " + this + "'s lock");
         }

         Set newView = peer.getTopology().getView();

         // identify the newcomers
         for(Iterator i = newView.iterator(); i.hasNext(); )
         {
            Serializable id = (Serializable)i.next();
            if (!unacked.containsKey(id))
            {
               // new replicator output added, create a map entry for it
               unacked.put(id, new ArrayList());
               log.debug(this + " notified of topology change: added " + id);
            }
         }

         // identify the ones who left the view
         for(Iterator i = unacked.keySet().iterator(); i.hasNext(); )
         {
            Serializable id = (Serializable)i.next();
            if (!newView.contains(id))
            {
               // a replicator output left the replicator
               List unackedMessageIDs = (List)unacked.remove(id);
               if (!unackedMessageIDs.isEmpty())
               {
                  // TODO what do I do with the unacknowledged messages of an output that goes away?
                  log.debug(this + " notified of topology change: removed " + id);
                  throw new NotYetImplementedException();
               }
            }
         }
      }
      finally
      {
         releaseLock();
      }
   }

   // AcknowledgmentCollectorServerDelegate implementation ----------

   public Serializable getID()
   {
      // unique collector per peer
      return peer.getID();
   }

   public void acknowledge(Serializable messageID, Serializable outputPeerID, Boolean positive)
   {
      if (log.isTraceEnabled())
      {
         log.trace("message " + messageID +
                   (positive.booleanValue() ? " POSITIVELY" : " NEGATIVELY" ) +
                   " acked by "+outputPeerID);
      }

      // TODO deal with negative acknowledgments
      if (!positive.booleanValue())
      {
         throw new NotYetImplementedException("Don't know to deal with negative ACK");
      }

      try
      {
         while(!acquireLock())
         {
            log.warn("could not aquire " + this + "'s lock");
         }
         List l = (List)unacked.get(outputPeerID);
         if (l != null && l.remove(messageID))
         {
            store.remove(messageID);
         }
      }
      finally
      {
         releaseLock();
      }
   }

   // Public --------------------------------------------------------

   /**
    * Acquiring the lock insures exclusive access to the collector. The metod can wait forever
    * if the lock was previously released.
    *
    * @return true if the lock was acquired, false if the waiting thread was interrupted.
    */
   public boolean acquireLock()
   {
      try
      {
         mutex.acquire();
         if (log.isTraceEnabled()) { log.trace("acquired " + this + "'s lock"); }
         return true;
      }
      catch(InterruptedException e)
      {
         log.warn("failed to acquire " + this + "'s lock");
         return false;
      }
   }

   /**
    * Releases a previously acquired lock.
    */
   public void releaseLock()
   {
      mutex.release();
      if (log.isTraceEnabled()) { log.trace("released " + this + "'s lock"); }
   }

   /**
    * Add a message that needs to receive acknowledgment from the output peers. The acknowledgment
    * collector will hold the message/attempt retransmission until all output peers of the
    * view the message was originally sent in acknowledge, or the message expires.
    *
    * To do this operation in a concurrent-safe way, you should previously acquire the collector's
    * lock.
    *
    * TODO implement Time To Live and message expiration
    *
    */
   public void add(Message m)
   {
      synchronized(unacked)
      {
         for(Iterator i = unacked.keySet().iterator(); i.hasNext(); )
         {
            Serializable outputPeerID = (Serializable)i.next();
            List l = (List)unacked.get(outputPeerID);
            l.add(m.getID());
            store.add(m);
            if (log.isTraceEnabled())
            {
               log.trace("added " + m.getID() + " to " + outputPeerID + "'s unack list");
            }
         }
      }
   }

   /**
    * Returns true if the peer doesn't currently hold messages that haven't been acknowledged.
    * @return
    */
   public boolean isEmpty()
   {
      return store.isEmpty();
   }

   /**
    * Frees up resources.
    */
   public void stop()
   {
      if (!isEmpty())
      {
         throw new NotYetImplementedException("The MessageStore contains UNACKNOWLEDGED MESSAGES!");
      }
      store = null;
      unacked = null;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("Collector[");
      sb.append(peer.getID());
      sb.append("]");
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
