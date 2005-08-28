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
import java.util.Collections;

/**
 * TODO - have AcknoweldgmentCollector implement AcknowledgmentStore. Currently the
 *        AcknowledgmentCollector only keeps the id of the receivers that NACKed.
 *        An AcknowledgmentStore keeps Acknowledgments (ACKs, NACKs and Channel NACKs).
 *
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

   // <ticket - Set of outputPeerIDs that did NOT acknowledge yet or null for ChannleNACK>
   protected Map unacked;

   // TODO This is a hack
   // <ticket - Set of outputPeerIDs that positively acknowledged using acknowledge()
   protected Map acked;

   // <Routable - NonCommitted>
   protected Map nonCommitted;


   // Constructors --------------------------------------------------

   public AcknowledgmentCollector(Replicator peer)
   {
      this.peer = peer;
      unacked = new HashMap();
      acked = new HashMap();
      nonCommitted = Collections.synchronizedMap(new HashMap());
   }

   // AcknowledgmentCollectorServerDelegate implementation ----------

   public Serializable getID()
   {
      // unique collector per peer
      return peer.getPeerID();
   }

   public void acknowledge(Serializable messageID, Serializable outputPeerID,
                           Serializable receiverID, Boolean positive)
   {
      lock();

      try
      {
         if (log.isTraceEnabled())
         {
            log.trace("message " + messageID +
                      (positive.booleanValue() ? " POSITIVELY" : " NEGATIVELY" ) +
                      " acked by " + outputPeerID + " on behalf of " + receiverID);
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

         // TODO This is a hack
         for(Iterator i = acked.keySet().iterator(); i.hasNext(); )
         {
            Ticket t = (Ticket)i.next();
            if (t.getRoutable().getMessageID().equals(messageID))
            {
               if (!positive.booleanValue())
               {
                  // negative acknowlegment, cancel the ACK
                  Set s = (Set)acked.get(t);
                  s.remove(receiverID);
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
         {                                        // TODO hack
                                                  //        |
                                                  //        v
            acknowledge(messageID, (Serializable)i.next(), null, positive);
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
    * TODO - necessary to connect the new acknowledgment storage system with this Collector.
    *
    * @param acks contains Acknowledgments or NonCommitted. Empty set means Channel NACK.
    */
   public void update(Routable r, Set acks)
   {
      for(Iterator i = acks.iterator(); i.hasNext(); )
      {
         Object o = i.next();
         // TODO - review core refactoring 2
//         if (o instanceof NonCommitted)
//         {
//            nonCommitted.put(r, o);
//            continue;
//         }
//         Acknowledgment a = (Acknowledgment)i.next();
//         addNACK(r, a.getReceiverID());
      }
   }

   public void acknowledge(Serializable messageID, Serializable receiverID)
   {
      //
      // TODO This whole method implementation is a hack to get tests passing
      //

      lock();

      acknowledge(messageID, null, receiverID, Boolean.TRUE);


      try
      {
         Ticket ticket = null;
         Set s = null;
         for(Iterator i = acked.keySet().iterator(); i.hasNext(); )
         {
            Ticket t = (Ticket)i.next();
            if (t.getRoutable().getMessageID().equals(messageID))
            {
               ticket = t;
            }
         }
         if (ticket == null)
         {
            // TODO - review core refactoring 2
//            ticket = new Ticket(new RoutableSupport(messageID));
            s = new HashSet();
            acked.put(ticket, s);
         }
         if (s == null)
         {
            s = (Set)acked.get(ticket);
         }

         s.add(receiverID);
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
   // TODO no support for ChannelNACK
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

               if (r.isExpired())
               {
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

   protected void enableNonCommitted(String txID)
   {
      for(Iterator i = nonCommitted.keySet().iterator(); i.hasNext();)
      {
         Routable r = (Routable)i.next();
         // TODO - review core refactoring 2
//         NonCommitted nc = (NonCommitted)nonCommitted.get(r);
//         if (nc != null && nc.getTxID().equals(txID))
//         {
//            i.remove();
//            unacked.put(new Ticket(r), Collections.EMPTY_LIST);
//         }
      }
   }

   protected void discardNonCommitted(String txID)
   {
      for(Iterator i = nonCommitted.keySet().iterator(); i.hasNext();)
      {
         Routable r = (Routable)i.next();
         // TODO - review core refactoring 2
//         NonCommitted nc = (NonCommitted)nonCommitted.get(r);
//         if (nc != null && nc.getTxID().equals(txID))
//         {
//            i.remove();
//         }
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
