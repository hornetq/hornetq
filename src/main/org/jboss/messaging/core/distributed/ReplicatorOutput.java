/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.util.RpcServerCall;
import org.jboss.messaging.util.RpcServer;
import org.jboss.messaging.util.ServerResponse;
import org.jboss.messaging.interfaces.Receiver;
import org.jboss.messaging.interfaces.Routable;
import org.jboss.messaging.interfaces.Message;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.MessageListener;
import org.jgroups.ChannelListener;
import org.jgroups.Address;

import java.io.Serializable;
import java.util.Random;
import java.util.Collection;
import java.util.Iterator;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;


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
public class ReplicatorOutput implements MessageListener, Runnable
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicatorOutput.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected boolean started;

   /** The ID of the replicator. A replicator usually comprises multiple input and output peers. */
   protected Serializable replicatorID;

   /** The ID of this replicator peer. Must be unique across the replicator */
   protected Serializable peerID;

   /** The dispatcher this replicator peer delegates the transport to */
   protected RpcDispatcher dispatcher;
   protected ChannelListener channelListener;

   /** the original MessageListener of the RpcDispatcher, to which I am delegating now */
   protected MessageListener delegateListener;

   protected LinkedQueue acknowledgmentQueue;
   protected Thread acknowledgmentThread;
   protected volatile boolean ackThreadActive;

   protected Receiver receiver;



   // Constructors --------------------------------------------------

   /**
    * Creates a replicator peer that is not connected to the replicator yet.
    *
    * @param replicatorID
    *
    * @exception IllegalStateException - thrown if the RpcDispatcher does not come pre-configured
    *            with an RpcServer.
    */
   public ReplicatorOutput(RpcDispatcher dispatcher, Serializable replicatorID)
   {
      Object serverObject = dispatcher.getServerObject();
      if (!(serverObject instanceof RpcServer))
      {
         throw new IllegalStateException("The RpcDispatcher does not have an RpcServer installed");
      }
      this.dispatcher = dispatcher;
      this.replicatorID = replicatorID;
      acknowledgmentQueue = new LinkedQueue();
      started = false;
   }

   // MessageListener implementation --------------------------------

   public void receive(org.jgroups.Message jgroupsMessage)
   {
      Object  o = jgroupsMessage.getObject();
      if (o instanceof Routable)
      {
         Routable m = (Routable)o;
         if (replicatorID.equals(m.getHeader(Routable.REPLICATOR_ID)))
         {
            // TODO ((Message)m).getMessageID() is a hack! Added to pass the tests. Change it!
            if (log.isTraceEnabled()) { log.trace(this + " received message, ID=" + ((Message)m).getMessageID()); }
            m.removeHeader(Routable.REPLICATOR_ID);
            Serializable inputPeerID = m.removeHeader(Routable.REPLICATOR_INPUT_ID);
            // Mark the message as being received from a remote endpoint
            m.putHeader(Routable.REMOTE_ROUTABLE, Routable.REMOTE_ROUTABLE);
            boolean acked = false;
            try
            {
               acked = receiver.handle(m);
            }
            catch(Exception e)
            {
               log.warn(this + "'s receiver did not acknowledge the message", e);
               acked = false;
            }
            MessageAcknowledgment ack =
                  new MessageAcknowledgment(jgroupsMessage.getSrc(), inputPeerID, ((Message)m).getMessageID(), acked);   // TODO ((Message)m).getMessageID() is a hack! Added to pass the tests. Change it!
            while(true)
            {
               try
               {
                  acknowledgmentQueue.put(ack);
                  break;
               }
               catch(InterruptedException e)
               {
                  log.warn("Thread interrupted while trying to put an acknowledgment in queue", e);
               }
            }
            // do not forward the message to the delegate listener
            return;
         }
      }
      if (delegateListener != null)
      {
         delegateListener.receive(jgroupsMessage);
      }
   }

   public byte[] getState()
   {
      if (delegateListener != null)
      {
         return delegateListener.getState();
      }
      return null;
   }

   public void setState(byte[] state)
   {
      if (delegateListener != null)
      {
         delegateListener.setState(state);
      }
   }

   // Runnable implementation ---------------------------------------

   /**
    * Runs on the acknowledgment thread.
    */
   public void run()
   {
      while(ackThreadActive)
      {
         try
         {
            MessageAcknowledgment ack = (MessageAcknowledgment)acknowledgmentQueue.take();
            acknowledge(ack);
         }
         catch(InterruptedException e)
         {
            log.debug("Thread interrupted while trying to take an acknowledgment from queue");
         }
      }
   }

   // Public --------------------------------------------------------

   public Serializable getReplicatorID()
   {
      return replicatorID;
   }

   public Serializable getID()
   {
      return peerID;
   }

   /**
    * Lifecycle method. Connects the peer to the distributed replicator. The underlying JChannel
    * must be connected when this method is invoked.
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

      // establish the topology - announce myself to the input peers, they'll use my id to collect
      // acknowledgements

      RpcServerCall rpcServerCall =
            new RpcServerCall(replicatorID,
                              "outputPeerJoins",
                              new Object[] {peerID},
                              new String[] {"java.io.Serializable"});

      // TODO use the timout when I'll change the send() signature or deal with the timeout
      Collection responses = rpcServerCall.remoteInvoke(dispatcher, 30000);

      if (log.isDebugEnabled())
      {
         log.debug(this + ".outputPeerJoins() received " + responses.size() +
                   " responses from input peers");
      }

      ServerResponse response = null;
      try
      {
         // all input peers must acknowledge
         for(Iterator i = responses.iterator(); i.hasNext(); )
         {
            response = (ServerResponse)i.next();
            if (log.isDebugEnabled())
            {
               log.debug(this + " received join acknowledgment: " + response);
            }
            Object result = response.getInvocationResult();
            if (result instanceof Throwable)
            {
               throw (Throwable)result;
            }
         }
      }
      catch(Throwable t)
      {
         String msg = "One of the peers (" +
                      RpcServer.serverDelegateToString(response.getAddress(),
                                                       response.getCategory(),
                                                       response.getServerDelegateID()) +
                      ") prevented this peer (" + this + ") from joining the replicator";
         log.error(msg, t);
         throw new DistributedException(msg, t);
      }

      // delegate also to the existing listener
      MessageListener l = dispatcher.getMessageListener();
      if (l != null)
      {
         delegateListener = l;
      }
      dispatcher.setMessageListener(this);

      acknowledgmentThread = new Thread(this, this + " Acknowledgment Thread");
      ackThreadActive = true;
      acknowledgmentThread.start();

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
      // TODO deal more carefully with the inflight messages (the one handled by handle())
      started = false;
      // the channel listener stays registered with the jChannel to restart the peer in case
      // the channel starts
      ackThreadActive = false;
      acknowledgmentThread.interrupt();
      acknowledgmentThread = null;
      // detach the listener
      dispatcher.setMessageListener(delegateListener);
      delegateListener = null;

      // TODO Group RPC outputPeerLeaves()

      peerID = null;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   /**
    * @return the receiver connected to the replicator or null if there is no Receiver.
    */
   public Receiver getReceiver()
   {
      return receiver;
   }

   /**
    * Connect a receiver to the output end of the replicator.
    */
   public void setReceiver(Receiver r)
   {
       receiver = r;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("ReplicatorOutput[");
      sb.append(replicatorID);
      sb.append(".");
      sb.append(peerID);
      sb.append("]");
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

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

   /**
    * TODO incomplete implementation
    *
    * Positively or negatively acknowledge a message to the sender.
    */
   protected void acknowledge(MessageAcknowledgment ack)
   {
      // TODO VERY inefficient implementation
      // TODO Sliding Window?

      RpcServerCall call = new RpcServerCall(ack.getInputPeerID(), "acknowledge",
                                             new Object[] {ack.getMessageID(),
                                                           peerID,
                                                           ack.isPositive()},
                                             new String[] {"java.io.Serializable",
                                                           "java.io.Serializable",
                                                           "java.lang.Boolean"});

      // TODO deal with the timeout
      try
      {
         call.remoteInvoke(dispatcher, ack.getSender(), 30000);
         if (log.isTraceEnabled()) { log.trace("sent " + ack); }
      }
      catch(Exception e)
      {
         log.error("Failed to send acknowledgment", e);
         // resubmit the acknowlegment to the queue
         // TODO: find something better than that. Deal with Time to Live.
         while(true)
         {
            try
            {
               acknowledgmentQueue.put(ack);
            }
            catch(InterruptedException ie)
            {
               log.warn("Thread interrupted while trying to put an acknowledgment in queue", ie);
            }
         }
      }
   }

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
         try
         {
            start();
         }
         catch(Exception e)
         {
            log.error("the replicator output cannot be restarted", e);
         }
      }

      public void channelDisconnected(org.jgroups.Channel channel)
      {
         log.debug(ReplicatorOutput.this + " channel disconnected");
         stop();
      }

      public void channelClosed(org.jgroups.Channel channel)
      {
         log.debug(ReplicatorOutput.this + " channel closed");
         stop();
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

}
