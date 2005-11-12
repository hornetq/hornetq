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
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.distributed.util.RpcServer;
import org.jboss.messaging.core.distributed.util.RpcServerCall;
import org.jboss.messaging.core.distributed.util.ServerResponse;
import org.jboss.messaging.core.distributed.pipe.DistributedPipe;
import org.jboss.messaging.core.distributed.pipe.DistributedPipeOutput;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.Address;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

/**
 * A distributed queue peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class QueuePeer extends Queue implements Peer, QueueFacade
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(QueuePeer.class);

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
    * An non-recoverable queue peer.
    */
   public QueuePeer(String name, MessageStore ms, RpcDispatcher dispatcher)
   {
      this(name, ms, null, dispatcher);
   }


   /**
    * A recoverable queue peer.
    */
   public QueuePeer(String name, MessageStore ms, PersistenceManager pm, RpcDispatcher dispatcher)
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

   public Acknowledgment join(Address remoteAddress,
                              Serializable remotePeerID,
                              Serializable remotePipeID)
         throws Throwable
   {
      // I will never receive my own call, since the server objects are not registered
      // at the time of call

      PeerIdentity remotePeerIdentity =
         new PeerIdentity(getChannelID(), remotePeerID, remoteAddress);

      log.debug("peer " + remotePeerIdentity + " wants to join");

      DistributedPipe p =  new DistributedPipe(remotePipeID, dispatcher, remoteAddress);
      RemotePeer rp = new RemotePeer(remotePeerIdentity, p);

      // TODO what happens if this peer receives this very moment a message to be
      // TODO delivered to the queue? Seding to the joining peer will fail, since its distributed
      // TODO pipe isn't completely functional yet. To add test case.

      add(rp);
      PeerIdentity pi =
         new PeerIdentity(getChannelID(), this.peerID, dispatcher.getChannel().getLocalAddress());
      return new Acknowledgment(pi, p.getID());
   }


   public void leave(PeerIdentity remotePeerIdentity)
   {
      if (getPeerIdentity().equals(remotePeerIdentity))
      {
         // ignore my own requests
         return;
      }

      log.debug("peer " + remotePeerIdentity + " wants to leave");

      // TODO synchronization
      for(Iterator i = iterator(); i.hasNext(); )
      {
         Object receiver = i.next();
         if (receiver instanceof RemotePeer)
         {
            RemotePeer rp = (RemotePeer)receiver;
            if (rp.getPeerIdentity().equals(remotePeerIdentity))
            {
               i.remove();
               break;
            }
         }
      }
   }

   // Peer implementation -------------------------------------------

   /**
    * Connects the peer to the distributed queue. The underlying JChannel must be connected
    * at the time of the call.
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

   /**
    * Stops the peer and disconnects it from the distributed queue.
    *
    * @exception DistributedException - a wrapper for exceptions thrown by the distributed layer.
    */
   public synchronized void leave() throws DistributedException
   {
      if(!joined)
      {
         return;
      }

      log.debug(this + " leaving distributed queue " + getChannelID());

      // multicast the intention to leave the queue
      RpcServerCall rpcServerCall =
            new RpcServerCall(getChannelID(), "leave",
                              new Object[] {getPeerIdentity()},
                              new String[] {"org.jboss.messaging.core.distributed.PeerIdentity"});

      // TODO use the timout when I'll change the send() signature or deal with the timeout
      rpcServerCall.remoteInvoke(dispatcher, TIMEOUT);
      log.debug("synchronous remote invocation ended");

      // unregister my pipe output
      rpcServer.unregister(pipeID);
      rpcServer.unregister(getChannelID(), this);
      joined = false;
   }

   public PeerIdentity getPeerIdentity()
   {
      return new PeerIdentity(getChannelID(), peerID, dispatcher.getChannel().getLocalAddress());
   }

   public synchronized boolean hasJoined()
   {
      return joined;
   }

   public Set getView()
   {
      if (!joined)
      {
         return Collections.EMPTY_SET;
      }

      Set result = new HashSet();

      for(Iterator i = iterator(); i.hasNext(); )
      {
         Object receiver = i.next();
         if (receiver instanceof RemotePeer)
         {
            RemotePeer rp = (RemotePeer)receiver;
            result.add(rp.getPeerIdentity());
         }
      }

      result.add(getPeerIdentity());
      return result;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "QueuePeer[" + getPeerIdentity() + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   /**
    * Create the distributed pipe to the peer that acknowledged.
    */
   private void pipeToPeer(Acknowledgment ack)
   {
      // I will never receive an acknowledgment from myself, since my server objects are not
      // registered yet, so I can safely link to peer.

      PeerIdentity remotePeerIdentity = ack.getPeerIdentity();
      DistributedPipe p =
         new DistributedPipe(ack.getPipeID(), dispatcher, remotePeerIdentity.getAddress());
      RemotePeer rp = new RemotePeer(remotePeerIdentity, p);
      add(rp);
   }

   private Serializable generateUniqueID()
   {
      return new GUID().toString();
   }

   // Inner classes -------------------------------------------------
}
