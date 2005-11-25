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
package org.jboss.messaging.core.distributed.replicator;

import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.distributed.PeerSupport;
import org.jboss.messaging.core.distributed.ViewKeeper;
import org.jboss.messaging.core.distributed.Distributed;
import org.jboss.messaging.core.distributed.Peer;
import org.jboss.messaging.core.distributed.RemotePeerInfo;
import org.jboss.messaging.core.distributed.RemotePeer;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;
import org.jgroups.blocks.RpcDispatcher;

import java.util.Set;
import java.util.Iterator;
import java.util.Collections;
import java.util.HashSet;
import java.io.Serializable;


/**
 * A Replicator is a distributed receiver that replicates synchronously or asynchronously a message
 * to multiple receivers living  <i>in different address spaces</i>. A replicator could have
 * multiple inputs and multiple outputs. Messages sent by an input are replicated to every output.
 * <p>
 * The replication of messages is done efficiently by multicasting, but message acknowledment is
 * handled by the replicator (so far) in a point-to-point manner. For that reason, each replicator
 * peer must be able to synchronously reach any other peer. When it is configured to be synchronous,
 * the replicator works pretty much like a distributed PointToMultipointRouter.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Replicator extends PeerSupport implements Distributed, Receiver
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Replicator.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected AcknowledgmentCollector collector;
   protected MessageStore ms;

   // Constructors --------------------------------------------------

   /**
    * Creates a replicator peer. The peer is not initially connected to the distributed replication
    * group.
    */
   public Replicator(ViewKeeper viewKeeper, RpcDispatcher dispatcher, MessageStore ms)
   {
      super(viewKeeper, dispatcher);
      this.ms = ms;
   }

   // Distributed implementation -------------------------

   public Peer getPeer()
   {
      return this;
   }

   public void close() throws DistributedException
   {
      leave();
   }

   // Receiver implementation ---------------------------------------

   public Delivery handle(DeliveryObserver observer, Routable routable, Transaction tx)
   {
      if (!joined)
      {
         return null;
      }

      if (log.isTraceEnabled()) { log.trace(this + " handles " + routable); }

      Set outputs = getOutputs();
      if (outputs.isEmpty())
      {
         if (log.isTraceEnabled()) { log.trace(this + " has no outputs, rejecting message"); }
         return null;
      }

      MessageReference ref = ms.reference(routable);


      // TODO cancelOnMessagesRejection=false is only valid for topics
      CompositeDelivery d = new CompositeDelivery(observer, ref, false, outputs);
      collector.startCollecting(d);

      routable.putHeader(Routable.REPLICATOR_ID, getReplicatorID());
      routable.putHeader(Routable.COLLECTOR_ID, collector.getID());

      try
      {
         dispatcher.getChannel().send(null, null, routable);
         if (log.isTraceEnabled()) { log.trace(this + " multicast " + routable); }
      }
      catch(Throwable t)
      {
         log.error("Failed to put the message on the channel", t);
         collector.remove(d);
         return null;
      }

      return d;
   }

   // Public --------------------------------------------------------

   public Serializable getReplicatorID()
   {
      return getGroupID();
   }

   /**
    * Return a set of PeerIdentities corresponding to the replicator's outputs. The set may be empty
    * but never null.
    */
   public Set getOutputs()
   {
      Set outputs = Collections.EMPTY_SET;
      for(Iterator i = viewKeeper.iterator(); i.hasNext(); )
      {
         RemotePeer rp = (RemotePeer)i.next();
         if (rp instanceof RemoteReplicatorOutput)
         {
            if (outputs.isEmpty())
            {
               outputs = new HashSet();
            }
            outputs.add(rp.getPeerIdentity());
         }
      }
      return outputs;
   }


   public String toString()
   {
      return "Replicator[" + getPeerIdentity() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // PeerSupport overrides -----------------------------------------

   protected void doJoin() throws DistributedException
   {
      collector = new AcknowledgmentCollector(new GUID().toString(), dispatcher);
      collector.start();

      if (!rpcServer.registerUnique(peerID, collector))
      {
         throw new IllegalStateException("There is already another server delegate registered " +
                                         "under category " + peerID);
      }

      rpcServer.register(viewKeeper.getGroupID(), this);

   }

   protected void doLeave() throws DistributedException
   {
      rpcServer.unregister(viewKeeper.getGroupID(), this);
      collector.stop();
      collector = null;
   }

   protected RemotePeer createRemotePeer(RemotePeerInfo thatPeerInfo)
   {
      PeerIdentity remotePeerIdentity = thatPeerInfo.getPeerIdentity();
      if (log.isTraceEnabled()) { log.trace(this + " adding remote peer " + remotePeerIdentity); }

      if (thatPeerInfo instanceof ReplicatorPeerInfo)
      {
         return new RemoteReplicator(remotePeerIdentity);
      }
      else if (thatPeerInfo instanceof ReplicatorOutputPeerInfo)
      {
         return new RemoteReplicatorOutput(remotePeerIdentity);
      }
      else
      {
         throw new IllegalArgumentException("Unknown RemotePeerInfo type " + thatPeerInfo);
      }
   }

   protected RemotePeerInfo getRemotePeerInfo()
   {
      return new ReplicatorPeerInfo(getPeerIdentity());
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}


// TODO - when refactoring Replicator, I won't need an acknowledge() method here, because the collector will be a AcknolwledgmentStore
//   public void acknowledge(Serializable messageID, Serializable receiverID)
//   {
//      //
//      // TODO: I totally override ChannelSupport.acknowledge() because I am not using
//      //       localAcknowledgmentStore and super.acknowledge() will throw NPE. However,
//      //       when AcknowledgmentCollector2 will implement AcknolwedgmentStore, I will
//      //       just delegate to super.acknowledge()
//      //
//      collector.acknowledge(messageID, receiverID);
//   }

//   public boolean handleNoTx(Routable r)
//   {
//      // TODO - review core refactoring 2
//      lock();
//
//      try
//      {
//         if (!started)
//         {
//            log.warn("Cannot handle, replicator peer not started");
//            return false;
//         }
//
//         r.putHeader(Routable.REPLICATOR_ID, replicatorID);
//         r.putHeader(Routable.REPLICATOR_INPUT_ID, peerID);
//
//         Set view = topology.getView();
//         AcknowledgmentCollector2.Ticket ticket = null;
//
//         collector.lock();
//
//         try
//         {
//            try
//            {
//               dispatcher.getChannel().send(null, null, r);
//               if (log.isTraceEnabled()) { log.trace("sent " + r); }
//            }
//            catch(Throwable t)
//            {
//               log.error("Failed to send the message to the channel", t);
//               return false;
//            }
//
//            if (isSynchronous())
//            {
//               // optimization for a more efficient synchronous handling
//               ticket = collector.addNACK(r, view);
//            }
//            else
//            {
//               // store the messages and the NACKs in a reliable way
//               Set nacks = new HashSet();
//               for(Iterator i = view.iterator(); i.hasNext(); )
//               {
//                  // TODO if message storing fails for one output peer, the others remain in the
//                  //      store as garbage
//                  nacks.add(new AcknowledgmentImpl((Serializable)i.next(), false));
//               }
//               if (!updateAcknowledgments(r, new StateImpl(nacks)))
//               {
//                  return false;
//               }
//            }
//         }
//         finally
//         {
//            collector.unlock();
//         }
//
//         if (ticket == null)
//         {
//            // asynchronous handling
//            return true;
//         }
//
//         // only for synchronous handling
//         boolean synchronouslyAcked = ticket.waitForAcknowledgments(30000);
//         if (!synchronouslyAcked)
//         {
//            collector.clear();
//         }
//         return synchronouslyAcked;
//      }
//      finally
//      {
//         unlock();
//      }
//      return false;
//   }
//
//
//
//
///**
// * Always called from a synchronized block, no need to synchronize. It can throw unchecked
// * exceptions, the caller is prepared to deal with them.
// *
// * @param acks - Set of Acknowledgments or NonCommitted. Empty set (or null) means Channel NACK.
// */
//protected void updateLocalAcknowledgments(Routable r, Set acks)
//{
//   if(log.isTraceEnabled()) { log.trace("updating acknowledgments " + r + " locally"); }
//
//   // the channel's lock is already acquired when invoking this method
//   collector.update(r, acks);
//}
//
//protected void removeLocalMessage(Serializable messageID)
//{
//   throw new NotYetImplementedException();
//}
//
//protected void enableNonCommitted(String txID)
//{
//   collector.enableNonCommitted(txID);
//}
//
//protected void discardNonCommitted(String txID)
//{
//   collector.discardNonCommitted(txID);
//}


