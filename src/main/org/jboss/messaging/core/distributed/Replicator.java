/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.core.util.RpcServer;
import org.jboss.messaging.core.util.AcknowledgmentImpl;
import org.jboss.messaging.core.util.StateImpl;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.local.MultipleOutputChannelSupport;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.ChannelListener;
import org.jgroups.Address;

import java.io.Serializable;
import java.util.Random;
import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;


/**
 *
 * TODO - review the design to incoroporate the new acknowledgment handling
 *
 *
 * A Replicator is a distributed channel that replicates a message to multiple receivers living
 * <i>in different address spaces</i> synchronously or asynchronously. A replicator can have
 * multiple inputs and multiple outputs. Messages sent by an input are replicated to every output.
 * <p>
 * The replication of messages is done efficiently by multicasting, but message acknowledment
 * is handled by the replicator (so far) in a point-to-point manner. For that reason, each
 * replicator peer must be able to reach <i>synchronously and efficiently</i> any other peer. In
 * this respect the replicator peers are "tightly coupled". If you want a looser coupling, use a
 * Destination.
 * <p>
 * When it is configured to be synchronous, the Replicator works pretty much like a distributed
 * PointToMultipointRouter.
 * <p>
 * The Replicator's main reason to exist is to allow to sender to synchronously send a message
 * to different address spaces and be sure that the message was received (and it is acknowledged)
 * when the handle() method returns, all this in an efficient way.
 * <p>
 *
 * TODO THE IMPLEMENTATION MUST BE REVIEWED
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Replicator extends MultipleOutputChannelSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Replicator.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected boolean started;

   /** The ID of the replicator. A replicator usually comprises multiple peers. */
   protected Serializable replicatorID;

   /** The ID of this replicator peer. Must be unique across the replicator */
   protected Serializable peerID;

   /** The dispatcher this replicator peer delegates the transport to */
   protected RpcDispatcher dispatcher;
   protected ChannelListener channelListener;
   protected RpcServer rpcServer;

   protected ReplicatorTopology topology;
   protected AcknowledgmentCollector collector;

   // Constructors --------------------------------------------------

   /**
    * Creates a replicator peer. The peer is not initially connected to the distributed replicator.
    *
    * @param replicatorID
    *
    * @exception IllegalStateException - thrown if the RpcDispatcher does not come pre-configured
    *            with an RpcServer.
    */
   public Replicator(RpcDispatcher dispatcher, Serializable replicatorID)
   {
      Object serverObject = dispatcher.getServerObject();
      if (!(serverObject instanceof RpcServer))
      {
         throw new IllegalStateException("The RpcDispatcher does not have an RpcServer installed");
      }
      rpcServer = (RpcServer)serverObject;
      this.dispatcher = dispatcher;
      this.replicatorID = replicatorID;
      // I don't need the default localAcknoweldgmentStore. GC it
      localAcknowledgmentStore = null;
      started = false;
   }

   // Channel implementation ----------------------------------------

   public Serializable getReceiverID()
   {
      return replicatorID;
   }

   public boolean deliver()
   {
      return collector.deliver(dispatcher);
   }

   public boolean hasMessages()
   {
      return !collector.isEmpty();
   }

   public Set getUndelivered()
   {
      return collector.getMessageIDs();
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

   // ChannelSupport implementation ---------------------------------

   public boolean handleNoTx(Routable r)
   {
      lock();

      try
      {
         if (!started)
         {
            log.warn("Cannot handle, replicator peer not started");
            return false;
         }

         r.putHeader(Routable.REPLICATOR_ID, replicatorID);
         r.putHeader(Routable.REPLICATOR_INPUT_ID, peerID);

         Set view = topology.getView();
         AcknowledgmentCollector.Ticket ticket = null;

         collector.lock();
         
         try
         {
            try
            {
               dispatcher.getChannel().send(null, null, r);
               if (log.isTraceEnabled()) { log.trace("sent " + r); }
            }
            catch(Throwable t)
            {
               log.error("Failed to send the message to the channel", t);
               return false;
            }

            if (isSynchronous())
            {
               // optimization for a more efficient synchronous handling
               ticket = collector.addNACK(r, view);
            }
            else
            {
               // store the messages and the NACKs in a reliable way
               Set nacks = new HashSet();
               for(Iterator i = view.iterator(); i.hasNext(); )
               {
                  // TODO if message storing fails for one output peer, the others remain in the
                  //      store as garbage
                  nacks.add(new AcknowledgmentImpl((Serializable)i.next(), false));
               }
               if (!updateAcknowledgments(r, new StateImpl(nacks)))
               {
                  return false;
               }
            }
         }
         finally
         {
            collector.unlock();
         }

         if (ticket == null)
         {
            // asynchronous handling
            return true;
         }

         // only for synchronous handling
         boolean synchronouslyAcked = ticket.waitForAcknowledgments(30000);
         if (!synchronouslyAcked)
         {
            collector.clear();
         }
         return synchronouslyAcked;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Always called from a synchronized block, no need to synchronize. It can throw unchecked
    * exceptions, the caller is prepared to deal with them.
    *
    * @param acks - Set of Acknowledgments or NonCommitted. Empty set (or null) means Channel NACK.
    */
   protected void updateLocalAcknowledgments(Routable r, Set acks)
   {
      if(log.isTraceEnabled()) { log.trace("updating acknowledgments " + r + " locally"); }

      // the channel's lock is already acquired when invoking this method
      collector.update(r, acks);
   }

   protected void removeLocalMessage(Serializable messageID)
   {
      throw new NotYetImplementedException();
   }

   protected void enableNonCommitted(String txID)
   {
      collector.enableNonCommitted(txID);
   }

   protected void discardNonCommitted(String txID)
   {
      collector.discardNonCommitted(txID);
   }


   // Public --------------------------------------------------------

   /**
    * May return null if the peer is not connected.
    */
   public Serializable getPeerID()
   {
      return peerID;
   }

   public Serializable getReplicatorID()
   {
      return replicatorID;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   /**
    * Lifecycle method. Connects the peer to the distributed replicator and starts the peer. The
    * underlying JChannel must be connected when this method is invoked.
    *
    * @exception DistributedException - a wrapper for exceptions thrown by the distributed layer
    *            (JGroups). The original exception, if any, is nested.
    */
   public synchronized void start() throws DistributedException
   {
      if(started)
      {
         return;
      }

      if (!dispatcher.getChannel().isConnected())
      {
         throw new DistributedException("The underlying JGroups channel not connected");
      }

      log.debug(this + " connecting");

      // get an unique peer ID
      peerID = getUniquePeerID();

      collector = new AcknowledgmentCollector(this);
      topology = new ReplicatorTopology(this);

      //topology.addObserver(collector);
      topology.aquireInitialTopology(dispatcher);

      if (!rpcServer.registerUnique(peerID, collector))
      {
         throw new IllegalStateException("The category " + peerID +
                                         "has already a server delegate registered");
      }
      rpcServer.register(replicatorID, topology);

      if (channelListener == null)
      {
         channelListener = new ChannelListenerImpl();
         dispatcher.addChannelListener(channelListener);
      }
      started = true;
   }

   /**
    * Lifecycle method.
    * TODO add test cases
    */
   public synchronized void stop()
   {
      if (!started)
      {
         return;
      }

      started = false;
      // the channel listener stays registered with the jChannel to restart the peer in case
      // the channel starts
      rpcServer.unregister(replicatorID, topology);
      rpcServer.unregister(peerID, collector);
      topology.stop();
      topology = null;
      collector.stop();
      collector = null;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("Replicator[");
      sb.append(replicatorID);
      sb.append(".");
      sb.append(peerID);
      sb.append("]");
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected ReplicatorTopology getTopology()
   {
      return topology;
   }

   protected static Random peerIDGenerator = new Random();

   /**
    * Coordinate with the existing peers and generate an unique ID per replicator.
    */
   protected Serializable getUniquePeerID()
   {
      // TODO quick and dirty implementation - implement it properly - this is not guaranteed
      // TODO to be unique accross the distributed replicator.
      long v = peerIDGenerator.nextLong();
      if (v < 0)
      {
         v = -v;
      }
      return new Long(v);
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

   protected class ChannelListenerImpl implements ChannelListener
   {
      public void channelConnected(org.jgroups.Channel channel)
      {
         log.debug(Replicator.this + " channel connected");
         try
         {
            start();
         }
         catch(Exception e)
         {
            log.error("the replicator cannot be restarted", e);
         }
      }

      public void channelDisconnected(org.jgroups.Channel channel)
      {
         log.debug(Replicator.this + " channel disconnected");
         stop();
      }

      public void channelClosed(org.jgroups.Channel channel)
      {
         log.debug(Replicator.this + " channel closed");
         stop();
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
