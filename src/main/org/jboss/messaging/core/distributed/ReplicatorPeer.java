/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.util.RpcServer;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.interfaces.Channel;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.ChannelListener;
import org.jgroups.Address;

import java.io.Serializable;
import java.util.Random;


/**
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
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReplicatorPeer implements Channel
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicatorPeer.class);

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
   public ReplicatorPeer(RpcDispatcher dispatcher, Serializable replicatorID)
   {
      Object serverObject = dispatcher.getServerObject();
      if (!(serverObject instanceof RpcServer))
      {
         throw new IllegalStateException("The RpcDispatcher does not have an RpcServer installed");
      }
      rpcServer = (RpcServer)serverObject;
      this.dispatcher = dispatcher;
      this.replicatorID = replicatorID;
      started = false;
   }

   // Channel implementation ----------------------------------------

   /**
    * TODO if I don't have a reliable store, it might just not be possible to handle the
    *      message synchronously
    */
   public boolean handle(Message m)
   {
      synchronized(this)
      {
         if (!started)
         {
            log.warn("Cannot handle, replicator peer not started");
            return false;
         }
      }

      m.putHeader(Message.REPLICATOR_ID, replicatorID);
      m.putHeader(Message.REPLICATOR_INPUT_ID, peerID);

      try
      {
         if (!collector.acquireLock())
         {
            return false;
         }
         dispatcher.getChannel().send(null, null, m);
         if (log.isTraceEnabled()) { log.trace("sent " + m); }

         // keep it until all output peers from the current view acknowledge
         collector.add(m);
      }
      catch(Exception e)
      {
         log.error("Failed to send the message to the channel", e);
         return false;
      }
      finally
      {
         collector.releaseLock();
      }

      // I accepted the responsibility for this message, I must keep it until all acknowledgment
      // for the current view are received and possibly retry delivery

      return true;
   }

   public boolean deliver()
   {
      throw new NotYetImplementedException();
   }

   public boolean hasMessages()
   {
      return !collector.isEmpty();
   }

   public boolean setSynchronous(boolean b)
   {
      throw new NotYetImplementedException();
   }

   public boolean isSynchronous()
   {
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   /**
    * May return null if the peer is not connected.
    */
   public Serializable getID()
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
      topology.addObserver(collector);
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
      StringBuffer sb = new StringBuffer("ReplicatorPeer[");
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
         log.debug(ReplicatorPeer.this + " channel connected");
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
         log.debug(ReplicatorPeer.this + " channel disconnected");
         stop();
      }

      public void channelClosed(org.jgroups.Channel channel)
      {
         log.debug(ReplicatorPeer.this + " channel closed");
         stop();
      }

      public void channelShunned()
      {
         log.debug(ReplicatorPeer.this + " channel shunned");
      }

      public void channelReconnected(Address address)
      {
         log.debug(ReplicatorPeer.this + " channel reconnected");
      }
   }
}
