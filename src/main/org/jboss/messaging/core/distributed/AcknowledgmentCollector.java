/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.messaging.core.util.RpcServerCall;
import org.jboss.messaging.core.util.Lockable;
import org.jboss.messaging.core.Routable;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.Address;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class AcknowledgmentCollector
      extends Lockable implements AcknowledgmentCollectorServerDelegate
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(AcknowledgmentCollector.class);

   public static final int DELIVERY_RETRIES = 5;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   protected Replicator peer;

   /** <ticket - Set of outputPeerIDs that did NOT acknowledge yet> */
   protected Map unacked;

   // Constructors --------------------------------------------------

   public AcknowledgmentCollector(Replicator peer)
   {
      this.peer = peer;
      unacked = new HashMap();
   }

   // AcknowledgmentCollectorServerDelegate implementation ----------

   public Serializable getID()
   {
      // unique collector per peer
      return peer.getPeerID();
   }

   public void acknowledge(Serializable messageID, Serializable outputPeerID, Boolean positive)
   {
      lock();

      try
      {
         if (log.isTraceEnabled())
         {
            log.trace("message " + messageID +
                      (positive.booleanValue() ? " POSITIVELY" : " NEGATIVELY" ) +
                      " acked by " + outputPeerID);
         }

         for(Iterator i = unacked.keySet().iterator(); i.hasNext(); )
         {
            Ticket t = (Ticket)i.next();
            if (t.getRoutable().getMessageID().equals(messageID))
            {
               if (positive.booleanValue())
               {
                  Set s = (Set)unacked.get(t);
                  s.remove(outputPeerID);
                  if (s.isEmpty())
                  {
                     t.release(true);
                     i.remove();
                  }
               }
               else
               {
                  // negative acknowlegment, if a sycnchronous call is waiting on this ticket, it
                  // will be also nacked
                  t.release(false);
               }
            }
         }
      }
      finally
      {
         unlock();
      }
   }

   // Public --------------------------------------------------------

   public void acknowledge(Serializable messageID, Set outputPeerIDs, Boolean positive)
   {
      lock();

      try
      {
         for(Iterator i = outputPeerIDs.iterator(); i.hasNext(); )
         {
            acknowledge(messageID, (Serializable)i.next(), positive);
         }
      }
      finally
      {
         unlock();
      }

   }

   /**
    * TODO implement Time To Live and message expiration
    * TODO hack for the Replicator's asynchronous handling
    */
   public Ticket addNACK(Routable r, Set outputPeerIDs)
   {
      lock();

      try
      {
         Ticket t = new Ticket(r);
         unacked.put(t, outputPeerIDs);
         if (log.isTraceEnabled()) { log.trace("added " + outputPeerIDs + " to " + t); }
         return t;
      }
      finally
      {
         unlock();
      }

   }

   /**
    * TODO hack for the Replicator's asynchronous handling
    */
   public void addNACK(Routable r, Serializable outputPeerID)
   {
      lock();

      try
      {
         Ticket ticket = null;
         Set s = null;
         for(Iterator i = unacked.keySet().iterator(); i.hasNext(); )
         {
            Ticket t = (Ticket)i.next();
            if (t.getRoutable().getMessageID().equals(r.getMessageID()))
            {
               ticket = t;
            }
         }
         if (ticket == null)
         {
            ticket = new Ticket(r);
            s = new HashSet();
            unacked.put(ticket, s);
         }


         if (s == null)
         {
            s = (Set)unacked.get(ticket);
         }

         s.add(outputPeerID);
         if (log.isTraceEnabled()) { log.trace("added " + outputPeerID + " to " + ticket); }
      }
      finally
      {
         unlock();
      }

   }
   /**
    * TODO hack
    */
   public Set getMessageIDs()
   {
      lock();

      try
      {
         Set s = new HashSet();
         for(Iterator i = unacked.keySet().iterator(); i.hasNext();)
         {
            s.add(((Ticket)i.next()).getRoutable().getMessageID());
         }
         return s;
      }
      finally
      {
         unlock();
      }
   }


   /**
    * Returns true if the peer doesn't currently hold messages that haven't been acknowledged.
    */
   public boolean isEmpty()
   {
      lock();

      try
      {
         return unacked.isEmpty();
      }
      finally
      {
         unlock();
      }
   }

   public void clear()
   {
      lock();

      try
      {
         unacked.clear();
      }
      finally
      {
         unlock();
      }
   }



   /**
    * Frees up resources.
    */
   public void stop()
   {
      if (!isEmpty())
      {
         throw new NotYetImplementedException("The MessageStoreImpl contains UNACKNOWLEDGED MESSAGES!");
      }
      unacked = null;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("Collector[");
      sb.append(peer.getPeerID());
      sb.append("]");
      return sb.toString();
   }

   // Package protected ---------------------------------------------


   // Protected -----------------------------------------------------


   // TODO VERY inefficient implementation - for each peer that did not acknowledge, retry
   // TODO unicast delivery;
   protected boolean deliver(RpcDispatcher dispatcher)
   {

      // try redelivery several times
      redelivery: for(int redeliveryCnt = 0; redeliveryCnt < DELIVERY_RETRIES; redeliveryCnt++)
      {
         try
         {
            try
            {
               Thread.sleep(redeliveryCnt * 200);
            }
            catch(InterruptedException e)
            {
               log.warn(e);
            }

            lock();

            if (unacked.isEmpty())
            {
               return true;
            }

            if (log.isTraceEnabled()) { log.trace(this + " deliver(), attempt " + (redeliveryCnt + 1)); }

            // TODO scan the collector the other way around and try to redeliver synchronously
            // TODO more than one message to the current output peer
            for(Iterator i = unacked.keySet().iterator(); i.hasNext();)
            {
               Ticket t = (Ticket)i.next();
               Routable r = t.getRoutable();

               if (System.currentTimeMillis() > r.getExpirationTime())
               {
                  // message expired
                  log.warn("Message " + r.getMessageID() + " expired by " + (System.currentTimeMillis() - r.getExpirationTime()) + " ms");
                  i.remove();
                  continue;
               }

               Set outputPeerIDs = (Set)unacked.get(t);
               for(Iterator j = outputPeerIDs.iterator(); j.hasNext(); )
               {
                  Serializable peerID = (Serializable)j.next();
                  Address address = peer.getTopology().getAddress(peerID);

                  // try unicast delivery

                  String methodName = "handle";
                  RpcServerCall call =
                        new RpcServerCall(peerID,
                                          methodName,
                                          new Object[] {r},
                                          new String[] {"org.jboss.messaging.core.Routable"});

                  try
                  {
                     if (log.isTraceEnabled()) { log.trace("Calling remotely " + methodName +
                                                           "() on " + address + "." + peerID); }

                     if(((Boolean)call.remoteInvoke(dispatcher, address, 3000)).booleanValue())
                     {
                        j.remove();
                     }
                     else
                     {
                        continue redelivery;
                     }
                  }
                  catch(Throwable tr)
                  {
                     log.warn("Remote call " + methodName + "() on " + address +  "." + peerID +
                              " failed", tr);
                     continue redelivery;
                  }
               }
               if (outputPeerIDs.isEmpty())
               {
                  i.remove();
               }
            }
         }
         finally
         {
            unlock();
         }
      }

      lock();
      
      try
      {
         return unacked.isEmpty();
      }
      finally
      {
         unlock();
      }

   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

   /**
    * Waiting areas for synchronous calls. This is an optimization to be used for synchronous
    * calls only.
    */
   class Ticket
   {
      private Routable routable;
      protected boolean ack = false;

      public Ticket(Routable r)
      {
         routable = r;
      }

      public Routable getRoutable()
      {
         return routable;
      }

      /**
       * @return true if all negative acknowlegments were cancelled in time, false if not or
       *         if a negative acknowlegment was reinforced.
       */
      public boolean waitForAcknowledgments(long timeout)
      {
         synchronized(this)
         {
            try
            {
               if (log.isTraceEnabled()) { log.trace("waiting for acknowledgment on " + this); }
               this.wait(timeout);
            }
            catch(InterruptedException e)
            {
               // TODO incomplete implementation, the list of outputPeerIDs and the ticket itself
               // TODO remain hanging in unacked
               log.warn("Interrupted", e);
            }
            if (log.isTraceEnabled()) { log.trace("released from waiting on " + this + ": " + ack); }
            return ack;
         }
      }

      /**
       * Releases any thread that waits for all acknowledgements to come, with a positive or a
       * negative acknowledgment.
       * @param ack - if positive, waitForAcknowledgments() will exit with a positive acknowledgment
       *        negative otherwise.
       */
      public void release(boolean ack)
      {
         synchronized(this)
         {
            this.ack = ack;
            notify();
         }
      }

      public String toString()
      {
         return "ticket[" + routable.getMessageID() + "]";
      }

   }
}
