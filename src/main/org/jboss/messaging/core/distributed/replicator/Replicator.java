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
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.distributed.PeerSupport;
import org.jboss.messaging.core.distributed.ViewKeeper;
import org.jboss.messaging.core.distributed.DistributedDestination;
import org.jboss.messaging.core.distributed.Peer;
import org.jboss.messaging.core.distributed.RemotePeerInfo;
import org.jboss.messaging.core.distributed.RemotePeer;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.ChannelListener;
import org.jgroups.Address;


/**
 * A Replicator is a distributed receiver that replicates synchronously or asynchronously a message
 * to multiple receivers living  <i>in different address spaces</i> synchronously or asynchronously.
 * A replicator could have multiple inputs and multiple outputs. Messages sent by an input are
 * replicated to every output.
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
public class Replicator extends PeerSupport implements DistributedDestination, Receiver
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Replicator.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ChannelListener channelListener;
//   protected AcknowledgmentCollector collector;

   // Constructors --------------------------------------------------

   /**
    * Creates a replicator peer. The peer is not initially connected to the distributed replication
    * group.
    */
   public Replicator(ViewKeeper viewKeeper, RpcDispatcher dispatcher)
   {
      super(viewKeeper, dispatcher);
   }

   // DistributedDestination implementation -------------------------

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
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "Replicator[" + getPeerIdentity() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // PeerSupport overrides -----------------------------------------

   protected void doJoin() throws DistributedException
   {
//      collector = new AcknowledgmentCollector(this);
//
//      if (!rpcServer.registerUnique(peerID, collector))
//      {
//         throw new IllegalStateException("There is already another server delegate registered " +
//                                         "under the category " + peerID);
//      }
//
//      collector.start();

      rpcServer.register(viewKeeper.getGroupID(), this);

      if (channelListener == null)
      {
         channelListener = new ChannelListenerImpl();
         dispatcher.addChannelListener(channelListener);
      }
   }

   protected void doLeave() throws DistributedException
   {
      rpcServer.unregister(viewKeeper.getGroupID(), this);
//      rpcServer.unregister(peerID, collector);
//      collector.stop();
//      collector = null;
   }

   protected RemotePeer createRemotePeer(RemotePeerInfo ack)
   {
      throw new NotYetImplementedException();
   }

   protected RemotePeerInfo getRemotePeerInfo()
   {
      return new RemotePeerInfo(getPeerIdentity());
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

   protected class ChannelListenerImpl implements ChannelListener
   {
      public void channelConnected(org.jgroups.Channel channel)
      {
//         log.debug(Replicator.this + " channel connected");
//         try
//         {
//            start();
//         }
//         catch(Exception e)
//         {
//            log.error("the replicator cannot be restarted", e);
//         }
         throw new NotYetImplementedException();
      }

      public void channelDisconnected(org.jgroups.Channel channel)
      {
//         log.debug(Replicator.this + " channel disconnected");
//         stop();
         throw new NotYetImplementedException();
      }

      public void channelClosed(org.jgroups.Channel channel)
      {
//         log.debug(Replicator.this + " channel closed");
//         stop();
         throw new NotYetImplementedException();
      }

      public void channelShunned()
      {
         log.debug(Replicator.this + " channel shunned");
      }

      public void channelReconnected(Address address)
      {
         log.debug(Replicator.this + " channel reconnected");
      }
   }
}


// TODO - when refactoring Replicator, I won't need an acknowledge() method here, because the collector will be a AcknolwledgmentStore
//   public void acknowledge(Serializable messageID, Serializable receiverID)
//   {
//      //
//      // TODO: I totally override ChannelSupport.acknowledge() because I am not using
//      //       localAcknowledgmentStore and super.acknowledge() will throw NPE. However,
//      //       when AcknowledgmentCollector will implement AcknolwedgmentStore, I will
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
//         AcknowledgmentCollector.Ticket ticket = null;
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


