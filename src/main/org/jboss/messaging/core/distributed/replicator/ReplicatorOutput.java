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
import org.jboss.messaging.core.distributed.RemotePeerInfo;
import org.jboss.messaging.core.distributed.Distributed;
import org.jboss.messaging.core.distributed.Peer;
import org.jboss.messaging.core.distributed.util.DelegatingMessageListener;
import org.jboss.messaging.core.distributed.util.DelegatingMessageListenerSupport;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.messaging.util.Util;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;
import org.jboss.jms.server.plugin.contract.MessageStoreDelegate;
import org.jgroups.MessageListener;
import org.jgroups.ChannelListener;
import org.jgroups.Address;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;
import java.util.Set;
import java.util.Iterator;



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
public class ReplicatorOutput extends ReplicatorPeer implements Distributed, ReplicatorOutputFacade
{
   // Constants -----------------------------------------------------

   public static final String REPLICATOR_OUTPUT_COLLECTOR_ADDRESS =
         "REPLICATOR_OUTPUT_COLLECTOR_ADDRESS";

   private static final Logger log = Logger.getLogger(ReplicatorOutput.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected MessageStoreDelegate ms;
   protected Receiver receiver;
   protected ChannelListener channelListener;
   protected DelegatingMessageListener messageListener;
   protected Serializable ignoredReplicatorPeerID;

   // Constructors --------------------------------------------------

   public ReplicatorOutput(Serializable replicatorID, RpcDispatcher dispatcher,
                           MessageStoreDelegate ms, Receiver receiver)
   {
      super(new GUID().toString(), replicatorID, dispatcher);
      this.ms = ms;
      this.receiver = receiver;
      log.debug(this + " created");
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
      return peerID;
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
    * The replicator output can be configured to discard message coming from a certain replicator
    * input. This feature is an optimization for certain topologies such a distributed topic, where
    * the local replicator output doesn't need to process messages multicast by the associated
    * replicator.
    *
    * TODO this is an experimental feature, currently discards messages coming from only ONE replicator
    */
   public void ignore(Serializable replicatorPeerID)
   {
      this.ignoredReplicatorPeerID = replicatorPeerID;
   }

   public String toString()
   {
      return "ReplicatorOutput[" + getPeerIdentity() + "]";
   }

   // Package protected ---------------------------------------------

   // PeerSupport overrides -----------------------------------------

   protected void doJoin() throws DistributedException
   {
      if (channelListener == null)
      {
         channelListener = new ChannelListenerImpl();
         dispatcher.addChannelListener(channelListener);
      }

      // delegate to the existing listener, if any
      messageListener = new MessageListenerImpl(dispatcher.getMessageListener());
      dispatcher.setMessageListener(messageListener);

      rpcServer.register(viewKeeper.getGroupID(), this);
   }

   protected void doLeave() throws DistributedException
   {
      rpcServer.unregister(viewKeeper.getGroupID(), this);

      dispatcher.removeChannelListener(channelListener);
      channelListener = null;

      // remove the message listener from dispatcher
      DelegatingMessageListener dl = (DelegatingMessageListener)dispatcher.getMessageListener();
      if (dl == messageListener)
      {
         dispatcher.setMessageListener(dl.getDelegate());
      }
      else
      {
         dl.remove(messageListener);
      }
   }

   protected RemotePeerInfo getRemotePeerInfo()
   {
      return new ReplicatorOutputPeerInfo(getPeerIdentity());
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

   protected class ChannelListenerImpl implements ChannelListener
   {
      // Constants -----------------------------------------------------

      // Static --------------------------------------------------------

      // Attributes ----------------------------------------------------

      // Constructors --------------------------------------------------

      // ChannelListener implementation --------------------------------

      public void channelConnected(org.jgroups.Channel channel)
      {
         log.debug(ReplicatorOutput.this + " channel connected");
      }

      public void channelDisconnected(org.jgroups.Channel channel)
      {
         log.debug(ReplicatorOutput.this + " channel disconnected");
      }

      public void channelClosed(org.jgroups.Channel channel)
      {
         log.debug(ReplicatorOutput.this + " channel closed");
      }

      public void channelShunned()
      {
         log.debug(ReplicatorOutput.this + " channel shunned");
      }

      public void channelReconnected(Address address)
      {
         log.debug(ReplicatorOutput.this + " channel reconnected");
      }

      // Public --------------------------------------------------------

      // Package protected ---------------------------------------------

      // Protected -----------------------------------------------------

      // Private -------------------------------------------------------

      // Inner classes -------------------------------------------------
   }

   protected class MessageListenerImpl
      extends DelegatingMessageListenerSupport implements DeliveryObserver
   {
      // Constants -----------------------------------------------------

      // Static --------------------------------------------------------

      // Attributes ----------------------------------------------------

      // Constructors --------------------------------------------------

      public MessageListenerImpl(MessageListener delegate)
      {
         super(delegate);
      }

      // MessageListener implementation --------------------------------

      public void receive(org.jgroups.Message jgroupsMessage)
      {
         Object  o = jgroupsMessage.getObject();

         try
         {
            if (!(o instanceof Routable))
            {
               if (log.isTraceEnabled()) { log.trace(this + " discarding non-routable (" + o + ")"); }
               return;
            }

            Routable r = (Routable)o;
            Object replicatorID = r.getHeader(Routable.REPLICATOR_ID);
            if (!getGroupID().equals(replicatorID))
            {
               if (log.isTraceEnabled()) { log.trace(this + " discarding routable from a different replicator (" + replicatorID + ")"); }
               return;
            }

            if (log.isTraceEnabled()) { log.trace(this + " received " + r); }

            Object collectorID = r.getHeader(Routable.COLLECTOR_ID);
            boolean isReliable = r.isReliable();
            Address collectorAddress = null;
            if (isReliable)
            {
               // we only need to send asynchronous acknowledgments back for reliable messages
               Set ids = viewKeeper.getRemotePeers();
               for(Iterator i = ids.iterator(); i.hasNext(); )
               {
                  PeerIdentity pid = (PeerIdentity)i.next();
                  if (pid.getPeerID().equals(collectorID))
                  {
                     collectorAddress = pid.getAddress();
                     break;
                  }
               }

               if (collectorAddress == null)
               {
                  log.error("Collector " + Util.guidToString(collectorID) +
                            " not found among the peers of " + ReplicatorOutput.this);
                  return;
               }
            }

            if (ignoredReplicatorPeerID != null && ignoredReplicatorPeerID.equals(collectorID))
            {
               if (log.isTraceEnabled()) { log.trace(this + ": message " + r + " sent by an ignored replicator, discarding"); }

               // However, I need to send an acknowledgment back for reliable messages, otherwise
               // the originator replicator input will never get rid of the associated delivery
               // from its RemoteTopic state.
               //
               // TODO: this solution is inneficient and confusing. A more efficient mechanism would
               //       be for to code the replicator input to not even *WAIT* for acknowledments
               //       from ignored outputs.
               if (isReliable)
               {
                  sendAsynchronousResponse(collectorAddress, r.getMessageID(), Acknowledgment.ACCEPTED);
               }
               return;
            }

            if (receiver == null)
            {
               if (isReliable)
               {
                  // no receiver to forward to, asynchronously reject the message
                  sendAsynchronousResponse(collectorAddress, r.getMessageID(), Acknowledgment.REJECTED);
               }
               return;
            }

            MessageReference ref;
            if (r.isReference())
            {
               ref = ms.reference((MessageReference)r);
            }
            else
            {
               ref = ms.reference((Message)r);
            }
            
            // Mark the reference as being received from a remote endpoint
            ref.putHeader(Routable.REMOTE_ROUTABLE, Routable.REMOTE_ROUTABLE);
            ref.removeHeader(Routable.REPLICATOR_ID);
            ref.removeHeader(Routable.COLLECTOR_ID);

            Delivery d = null;
            try
            {
               ref.putHeader(REPLICATOR_OUTPUT_COLLECTOR_ADDRESS, collectorAddress);
               d = receiver.handle(this, ref, null);
               //If the receiver is a simple Topic then the returned delivery
               //will have been created with new SimpleDelivery(true)
               //hence won't have a reference
               if (d.getReference() == null)
               {
                  d = new SimpleDelivery(ref, d.isDone());
               }
               if (log.isTraceEnabled()) { log.trace(this + ": receiver " + receiver + " handled " + r + " and returned " + d); }
            }
            catch(Throwable t)
            {
               log.warn(ReplicatorOutput.this + "'s receiver is broken", t);

               if (isReliable)
               {
                  // broken receiver, asynchronously reject the message
                  sendAsynchronousResponse(collectorAddress, r.getMessageID(), Acknowledgment.REJECTED);
               }
            }

            if (isReliable)
            {
               // we only need to send asynchronous acknowledgments back for reliable messages

               if (d.isDone())
               {
                  acknowledge(d, null);
               }
               else if (d.isCancelled())
               {
                  try
                  {
                     cancel(d);
                  }
                  catch(Throwable t)
                  {
                     // will never throw Throwable
                  }
               }
               else
               {
                  if (log.isTraceEnabled()) { log.trace(this + " starts waiting asynchronous acknowledgment from " + receiver); }
               }
            }
         }
         finally
         {
            if (delegate != null)
            {
               // forward the message to delegate, there may be other ReplicatorOutput listeners
               // entitled to get the message or other generic listeners
               if (log.isTraceEnabled()) { log.trace(this + " forwards message to delegate listener " + delegate); }
               delegate.receive(jgroupsMessage);
            }
         }
      }

      public byte[] getState()
      {
         if (delegate != null)
         {
            return delegate.getState();
         }
         return null;
      }

      public void setState(byte[] state)
      {
         if (delegate != null)
         {
            delegate.setState(state);
         }
      }

      // DeliveryObserver implementation -------------------------------

      public void acknowledge(Delivery d, Transaction tx)
      {
         if (tx != null)
         {
            throw new NotYetImplementedException();
         }

         MessageReference ref = d.getReference();

         if (!ref.isReliable())
         {
            return; // no acknowledgment sent back
         }

         if (log.isTraceEnabled()) { log.trace(this + " acknowledging " + d); }
         Address collectorAddress = (Address)ref.removeHeader(REPLICATOR_OUTPUT_COLLECTOR_ADDRESS);
         sendAsynchronousResponse(collectorAddress, ref.getMessageID(), Acknowledgment.ACCEPTED);
      }

      public void cancel(Delivery d) throws Throwable
      {
         MessageReference ref = d.getReference();

         if (!ref.isReliable())
         {
            return; // no cancellation is sent back
         }

         if (log.isTraceEnabled()) { log.trace(this + " cancelling " + d); }
         Address collectorAddress = (Address)ref.removeHeader(REPLICATOR_OUTPUT_COLLECTOR_ADDRESS);
         sendAsynchronousResponse(collectorAddress, ref.getMessageID(),
                                  Acknowledgment.CANCELLED);
         return;
      }

      // Public --------------------------------------------------------

      public String toString()
      {
         return ReplicatorOutput.this + ".Listener";
      }

      // Package protected ---------------------------------------------

      // Protected -----------------------------------------------------

      // Private -------------------------------------------------------

      private boolean sendAsynchronousResponse(Address collectorAddress,
                                               Serializable messageID,
                                               int state)
      {
         if (collectorAddress == null)
         {
            throw new IllegalArgumentException("null collectorAddress");
         }

         try
         {
            Acknowledgment ack = new Acknowledgment(getID(), messageID, state);
            dispatcher.getChannel().send(collectorAddress, null, ack);
            if (log.isTraceEnabled()) { log.trace(this + " sent " + Acknowledgment.stateToString(state) + " to " + collectorAddress + " for " + messageID); }
            return true;
         }
         catch(Throwable t)
         {
            log.error("Failed to put the acknowledment on the channel", t);
            return false;
         }
      }

      // Inner classes -------------------------------------------------

   }
}
