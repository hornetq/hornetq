/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.core.util.RpcServer;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.local.ChannelSupport;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.local.ChannelSupport;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.ChannelListener;
import org.jgroups.Address;

import java.io.Serializable;
import java.util.Random;
import java.util.Set;
import java.util.Iterator;


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
 * TODO THE IMPLEMENTATION MUST BE REVIEWED
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Replicator extends ChannelSupport
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
      started = false;
   }

   // Channel implementation ----------------------------------------

   public Serializable getReceiverID()
   {
      return replicatorID;
   }

   public boolean handle(Routable r)
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
               // optimisation for a more efficient synchronous handling
               ticket = collector.addNACK(r, view);
            }
            else
            {
               // store the messages and the NACKs in a reliable way
               for(Iterator i = view.iterator(); i.hasNext(); )
               {
                  // TODO if message storing fails for one output peer, the others remain in the
                  // TODO store as garbage
                  if (!storeNACKedMessage(r, (Serializable)i.next()))
                  {
                     return false;
                  }
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

   public boolean deliver()
   {
      return collector.deliver(dispatcher);
   }

   public boolean hasMessages()
   {
      return !collector.isEmpty();
   }

   public Set getUnacknowledged()
   {
      return collector.getMessageIDs();
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

   protected void storeNACKedMessageLocally(Routable r, Serializable receiverID)
   {

      // TODO This is a hack to have a quick implementation, so ReceiverTest would pass; review
      collector.addNACK(r, receiverID);

   }

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
