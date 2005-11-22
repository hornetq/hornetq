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

import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.messaging.core.distributed.PeerSupport;
import org.jboss.messaging.core.distributed.RemotePeer;
import org.jboss.messaging.core.distributed.RemotePeerInfo;
import org.jboss.messaging.core.distributed.Distributed;
import org.jboss.messaging.core.distributed.Peer;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;
import org.jgroups.MessageListener;
import org.jgroups.ChannelListener;
import org.jgroups.Address;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;
import java.util.Random;



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
public class ReplicatorOutput
      extends PeerSupport
      implements Distributed, ReplicatorOutputFacade, Runnable
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicatorOutput.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Receiver receiver;
   protected ChannelListener channelListener;
   protected MessageListener messageListener;


   // Constructors --------------------------------------------------

   public ReplicatorOutput(Serializable replicatorID, RpcDispatcher dispatcher, Receiver receiver)
   {
      super(new ReplicatorOutputView(replicatorID), dispatcher);

      this.receiver = receiver;
      log.debug(this + " created");
   }

   // PeerSupport overrides -----------------------------------------


   protected void doJoin() throws DistributedException
   {
      if (channelListener == null)
      {
         channelListener = new ChannelListenerImpl();
         dispatcher.addChannelListener(channelListener);
      }
   }

   protected void doLeave() throws DistributedException
   {
      dispatcher.removeChannelListener(channelListener);
      channelListener = null;
   }

   protected RemotePeer createRemotePeer(RemotePeerInfo newRemotePeerInfo)
   {
      if (!(newRemotePeerInfo instanceof ReplicatorPeerInfo))
      {
         return null;
      }

      return new RemoteReplicator(newRemotePeerInfo.getPeerIdentity());
   }

   protected RemotePeerInfo getRemotePeerInfo()
   {
      return new ReplicatorOutputPeerInfo(getPeerIdentity());
   }

   // Distributed implementation ------------------------------------

   public void close() throws DistributedException
   {
      leave();
   }

   public Peer getPeer()
   {
      return this;
   }

   // ReplicatorOutputFacade implementation -----------------

   public Serializable getID()
   {
//      return peerID;
      throw new NotYetImplementedException();
   }

   public boolean handle(Routable m)
   {
        // TODO - review core refactoring 2
//      // try to acquire the lock and if I am not able, immediately NACK. This is to avoid
//      // distributed dead-lock in a race condition with the acknowledgment method.
//      if (!lock(0))
//      {
//         return false;
//      }
//      try
//      {
//         // Mark the message as being received from a remote endpoint
//         m.putHeader(Routable.REMOTE_ROUTABLE, Routable.REMOTE_ROUTABLE);
//         return receiver.handle(m);
//      }
//      catch(Throwable t)
//      {
//         log.error("The receiver connected to " + this + " is unable to handle the message: " + t);
//         return false;
//      }
//      finally
//      {
//         unlock();
//      }
      return false;
   }

   // Runnable implementation ---------------------------------------

   /**
    * Runs on the acknowledgment thread.
    */
   public void run()
   {
//      while(ackThreadActive)
//      {
//         // TODO distributed core refactoring
//         try
//         {
//            MessageAcknowledgment ack = (MessageAcknowledgment)acknowledgmentQueue.take();
//            acknowledge(ack);
//         }
//         catch(InterruptedException e)
//         {
//            log.debug("Thread interrupted while trying to take an acknowledgment from queue");
//         }
//      }
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   public Serializable getReplicatorID()
   {
      return viewKeeper.getGroupID();
   }

   public Receiver getReceiver()
   {
      return receiver;
   }

   /**
    * Lifecycle method. Connects the peer to the distributed replicator. The underlying JChannel
    * must be connected when this method is invoked.
    *
    * @exception org.jboss.messaging.core.distributed.DistributedException - a wrapper for exceptions thrown by the distributed layer
    *            (JGroups). The original exception, if any, is nested.
    */
   public void start() throws DistributedException
   {
//      lock();
//
//      try
//      {
//         if(started)
//         {
//            return;
//         }
//
//         if (!dispatcher.getChannel().isConnected())
//         {
//            throw new DistributedException("The underlying JGroups channel not connected");
//         }
//
//         log.debug(this + " connecting");
//
//         // get an unique peer ID
//         peerID = getUniquePeerID();
//
//         // establish the topology - announce myself to the input peers, they'll use my id to
//         // collect acknowledgements
//
//         RpcServerCall rpcServerCall =
//               new RpcServerCall(replicatorID,
//                                 "outputPeerJoins",
//                                 new Object[] {peerID, dispatcher.getChannel().getLocalAddress()},
//                                 new String[] {"java.io.Serializable", "org.jgroups.Address"});
//
//         // TODO use the timout when I'll change the send() signature or deal with the timeout
//         Collection responses = rpcServerCall.remoteInvoke(dispatcher, 30000);
//
//         log.debug(this + ".outputPeerJoins() received " + responses.size() + " responses from input peers");
//
//         ServerResponse response = null;
//         try
//         {
//            // all input peers must acknowledge
//            for(Iterator i = responses.iterator(); i.hasNext(); )
//            {
//               response = (ServerResponse)i.next();
//               log.debug(this + " received join acknowledgment: " + response);
//               Object result = response.getInvocationResult();
//               if (result instanceof NoSuchMethodException)
//               {
//                  // OK, I called an output peer
//                  log.debug(response.getSubordinateID() + " is an output peer");
//               }
//               else if (result instanceof Throwable)
//               {
//                  throw (Throwable)result;
//               }
//            }
//         }
//         catch(Throwable t)
//         {
//            String msg = "One of the peers (" +
//                         RpcServer.subordinateToString(response.getCategory(),
//                                                       response.getSubordinateID(),
//                                                       response.getAddress()) +
//                         ") prevented this peer (" + this + ") from joining the replicator";
//            log.error(msg, t);
//            throw new DistributedException(msg, t);
//         }
//
//         // delegate also to the existing listener
//         MessageListener l = dispatcher.getMessageListener();
//         if (l != null)
//         {
//            delegateListener = l;
//         }
//         dispatcher.setMessageListener(this);
//
//         // register to the rpcDispatcher so I can also receive synchronous calls.
//         // I need this for retransmissions of NACKed messages.
//
//         if (!rpcServer.registerUnique(getID(), this))
//         {
//            throw new DistributedException("The category " + getID() + " already registered");
//         }
//
//         // register my identity delegate too
//         identityDelegate = new IdentityServerDelegateImpl();
//         rpcServer.register(replicatorID, identityDelegate);
//
//
//         acknowledgmentThread = new Thread(this, this + " Acknowlegment Thread");
//         ackThreadActive = true;
//         acknowledgmentThread.start();
//
//         if (channelListener == null)
//         {
//            channelListener = new ChannelListenerImpl();
//            dispatcher.addChannelListener(channelListener);
//         }
//
//         started = true;
//      }
//      finally
//      {
//         unlock();
//      }
      throw new NotYetImplementedException();
   }

   /**
    * Lifecycle method.
    * TODO add test cases
    */
   public void stop()
   {
//      lock();
//
//      try
//      {
//         if (!started)
//         {
//            return;
//         }
//         // TODO deal more carefully with the inflight messages (the one handled by handle())
//         started = false;
//
//         rpcServer.unregister(peerID, this);
//         rpcServer.unregister(replicatorID, identityDelegate);
//         identityDelegate = null;
//
//         // the channel listener stays registered with the jChannel to restart the peer in case
//         // the channel starts
//         ackThreadActive = false;
//         acknowledgmentThread.interrupt();
//         acknowledgmentThread = null;
//         // detach the listener
//         dispatcher.setMessageListener(delegateListener);
//         delegateListener = null;
//
//         // TODO Group RPC outputPeerLeaves()
//
//         peerID = null;
//      }
//      finally
//      {
//         unlock();
//      }
      throw new NotYetImplementedException();
   }

   public String toString()
   {
      return "ReplicatorOutput[" + getPeerIdentity() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // TODO distributed core refactoring
//   /**
//    * TODO incomplete implementation
//    *
//    * Positively or negatively acknowledge a message to the sender.
//    */
//   protected void acknowledge(MessageAcknowledgment ack)
//   {
//      // TODO VERY inefficient implementation
//      // TODO Sliding Window?
//
//      lock();
//
//      try
//      {
//         RpcServerCall call = new RpcServerCall(ack.getInputPeerID(),
//                                                "acknowledge",
//                                                new Object[] {ack.getMessageID(),
//                                                              peerID,
//                                                              ack.getReceiverID(),
//                                                              ack.isPositive()},
//                                                new String[] {"java.io.Serializable",
//                                                              "java.io.Serializable",
//                                                              "java.io.Serializable",
//                                                              "java.lang.Boolean"});
//
//         // TODO deal with the timeout
//         try
//         {
//            if (log.isTraceEnabled()) { log.trace("Calling remotely acknowledge() on " +
//                                                  ack.getSender() + "." + ack.getInputPeerID()); }
//
//            call.remoteInvoke(dispatcher, ack.getSender(), 30000);
//
//            if (log.isTraceEnabled()) { log.trace("sent " + ack); }
//         }
//         catch(Throwable t)
//         {
//            log.error("Failed to send acknowledgment", t);
//            // resubmit the acknowlegment to the queue
//            // TODO: find something better than that. Deal with Time to Live.
//            while(true)
//            {
//               try
//               {
//                  acknowledgmentQueue.put(ack);
//                  break;
//               }
//               catch(InterruptedException ie)
//               {
//                  log.warn("Thread interrupted while trying to put an acknowledgment in queue", ie);
//               }
//            }
//         }
//      }
//      finally
//      {
//         unlock();
//      }
//   }

   protected Serializable generateMessageID()
   {
      // TODO Naive implementation
      return new Long(new Random().nextLong());
   }


   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

   protected class ChannelListenerImpl implements ChannelListener
   {
      public void channelConnected(org.jgroups.Channel channel)
      {
         log.debug(ReplicatorOutput.this + " channel connected");
//         try
//         {
//            start();
//         }
//         catch(Exception e)
//         {
//            log.error("the replicator output cannot be restarted", e);
//         }
      }

      public void channelDisconnected(org.jgroups.Channel channel)
      {
         log.debug(ReplicatorOutput.this + " channel disconnected");
//         stop();
      }

      public void channelClosed(org.jgroups.Channel channel)
      {
         log.debug(ReplicatorOutput.this + " channel closed");
//         stop();
      }

      public void channelShunned()
      {
         log.debug(ReplicatorOutput.this + " channel shunned");
      }

      public void channelReconnected(Address address)
      {
         log.debug(ReplicatorOutput.this + " channel reconnected");
      }
   }


   protected class MessageListenerImpl implements MessageListener
   {

      // MessageListener implementation --------------------------------

      public void receive(org.jgroups.Message jgroupsMessage)
      {
         Object  o = jgroupsMessage.getObject();

         if (!(o instanceof Routable))
         {
            if (log.isTraceEnabled()) { log.trace(this + " received a non-routable (" + o + "), discarding ..."); }
            return;
         }

         Routable r = (Routable)o;
         Object replicatorID = r.getHeader(Routable.REPLICATOR_ID);
         if (!getGroupID().equals(replicatorID))
         {
            if (log.isTraceEnabled()) { log.trace(this + " received a routable from a different replicator (" + replicatorID + "), discarding ..."); }
            return;
         }

         if (log.isTraceEnabled()) { log.trace(this + " received message " + r); }


         r.removeHeader(Routable.REPLICATOR_ID);
         Serializable collectorID = r.removeHeader(Routable.COLLECTOR_ID);

         // Mark the message as being received from a remote endpoint
         r.putHeader(Routable.REMOTE_ROUTABLE, Routable.REMOTE_ROUTABLE);

         Delivery d = null;
         try
         {
            if (receiver != null)
            {
               d = receiver.handle(null, r, null);
            }
         }
         catch(Throwable t)
         {
            log.warn(this + "'s receiver is broken", t);
         }


//         MessageAcknowledgment ack = new MessageAcknowledgment(jgroupsMessage.getSrc(),
//                                                               inputPeerID, receiverID,
//                                                               r.getMessageID(), acked);
//         while(true)
//         {
//            try
//            {
//               acknowledgmentQueue.put(ack);
//               break;
//            }
//            catch(InterruptedException e)
//            {
//               log.warn("Thread interrupted while trying to put an acknowledgment in queue", e);
//            }
//         }
//         // do not forward the message to the delegate listener
//         return;
//
//         if (delegateListener != null)
//         {
//            delegateListener.receive(jgroupsMessage);
//         }
      }

      public byte[] getState()
      {
//      if (delegateListener != null)
//      {
//         return delegateListener.getState();
//      }
//      return null;
         throw new NotYetImplementedException();
      }

      public void setState(byte[] state)
      {
//      if (delegateListener != null)
//      {
//         delegateListener.setState(state);
//      }
         throw new NotYetImplementedException();
      }


   }


   protected class IdentityServerDelegateImpl implements IdentityServerDelegate
   {
      public Serializable getID()
      {
//         return peerID;
         throw new NotYetImplementedException();
      }

      public PeerIdentity getIdentity()
      {
//         return new PeerIdentity(replicatorID, peerID, dispatcher.getChannel().getLocalAddress());
         throw new NotYetImplementedException();
      }
   }
}
