/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.distributed.util.RpcServer;
import org.jboss.messaging.core.distributed.util.RpcServerCall;
import org.jboss.messaging.core.distributed.util.ServerResponse;
import org.jboss.messaging.core.distributed.pipe.DistributedPipe;
import org.jboss.messaging.core.distributed.pipe.DistributedPipeOutput;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.Address;

import javax.transaction.TransactionManager;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

/**
 * A distributed queue peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DistributedQueue extends Queue implements Peer, QueueFacade
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DistributedQueue.class);

   private static final long TIMEOUT = 3000;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable peerID;
   protected Serializable pipeID;
   protected RpcDispatcher dispatcher;
   protected RpcServer rpcServer;

   protected boolean joined = false;


   // Constructors --------------------------------------------------

   /**
    * An unreliable distributed queue.
    */
   public DistributedQueue(String name,
                           MessageStore ms,
                           RpcDispatcher dispatcher)
   {
      this(name, ms, null, dispatcher);
   }


   public DistributedQueue(String name,
                           MessageStore ms, 
                           PersistenceManager pm,
                           RpcDispatcher dispatcher)
   {
      super(name, ms, pm);

      Object so = dispatcher.getServerObject();
      if (!(so instanceof RpcServer))
      {
         throw new IllegalStateException("RpcDispatcher must have a pre-installed RpcServer");
      }

      this.dispatcher = dispatcher;
      rpcServer = (RpcServer)so;

      // TODO - Do I need to have two different ids? Can't I just use one?
      this.peerID = generateUniqueID();
      this.pipeID = generateUniqueID();
      joined = false;
   }

   // QueueFacade implementation ------------------------------------

   public Serializable getID()
   {
      return peerID;
   }

   public Acknowledgment join(Address address, Serializable id, Serializable pipeID)
         throws Throwable
   {
      // I will never receive my own call, since the server objects are not registered
      // at the time of call

      log.debug("peer " + PeerIdentity.identityToString(getChannelID(), id, address) +
                " wants to join");

      DistributedPipe p =  new DistributedPipe(pipeID, dispatcher, address);

      // TODO what happens if this peer receives this very moment a message to be
      // TODO delivered to the queue? Seding to the joining peer will fail, since its distributed
      // TODO pipe isn't completely functional yet. To add test case.

      add(p);
      return new Acknowledgment(dispatcher.getChannel().getLocalAddress(), id);
   }

   // Peer implementation -------------------------------------------

   public PeerIdentity getPeerIdentity()
   {
      return new PeerIdentity(getChannelID(), peerID, dispatcher.getChannel().getLocalAddress());
   }

   /**
    * Connects the peer to the distributed queue. The underlying JChannel must be connected at the
    * time of the call.
    *
    * @exception DistributedException - a wrapper for exceptions thrown by the distributed layer.
    */
   public synchronized void join() throws DistributedException
   {
      if(joined)
      {
         return;
      }

      if (!dispatcher.getChannel().isConnected())
      {
         throw new DistributedException("The JGroups channel not connected");
      }

      log.debug(this + " joining distributed queue " + getChannelID());

      // multicast the intention to join the queue
      RpcServerCall rpcServerCall =
            new RpcServerCall(getChannelID(), "join",
                              new Object[] {dispatcher.getChannel().getLocalAddress(),
                                            peerID,
                                            pipeID},
                              new String[] {"org.jgroups.Address",
                                            "java.io.Serializable",
                                            "java.io.Serializable"});

      // TODO use the timout when I'll change the send() signature or deal with the timeout
      Collection responses = rpcServerCall.remoteInvoke(dispatcher, TIMEOUT);

      log.debug(this + " received " + responses.size() + " response(s)");

      ServerResponse r = null;

      try
      {
         // all peers must acknowledge
         for(Iterator i = responses.iterator(); i.hasNext(); )
         {
            r = (ServerResponse)i.next();

            log.debug(this + " received: " + r);

            Object o = r.getInvocationResult();
            if (o instanceof Throwable)
            {
               throw (Throwable)o;
            }

            Acknowledgment ack = (Acknowledgment)o;
            pipeToPeer(ack);
         }
      }
      catch(Throwable t)
      {
         String msg = RpcServer.
               subordinateToString(r.getCategory(), r.getSubordinateID(),r.getAddress()) +
               " vetoed " + this + " to join the queue";
         log.error(msg, t);
         throw new DistributedException(msg, t);
      }

      DistributedPipeOutput output = new DistributedPipeOutput(pipeID, this);
      if (!rpcServer.registerUnique(pipeID, output))
      {
         throw new IllegalStateException("More than one server subordinates tried " +
                                         "to registers using id=" + pipeID);
      }

      rpcServer.register(getChannelID(), this);
      joined = true;
   }

   public synchronized boolean hasJoined()
   {
      return joined;
   }

   /**
    * Stops the peer and disconnects it from the distributed queue.
    *
    * @exception DistributedException - a wrapper for exceptions thrown by the distributed layer.
    */
   public synchronized void leave() throws DistributedException
   {
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "DistributedQueue[" + getPeerIdentity() + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   /**
    * Create the distributed pipe to the peer that acknowledged. \
    */
   private void pipeToPeer(Acknowledgment ack)
   {
      // I will never receive an acknowledgment from myself, since my server objects are not
      // registered yet, so I can safely link to peer.

      DistributedPipe p = new DistributedPipe(ack.getPipeID(), dispatcher, ack.getAddress());
      add(p);
   }

   private Serializable generateUniqueID()
   {
      return new GUID().toString();
   }

   // Inner classes -------------------------------------------------
}
