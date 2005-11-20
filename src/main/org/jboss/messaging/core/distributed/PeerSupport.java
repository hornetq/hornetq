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

import org.jboss.messaging.core.distributed.util.RpcServer;
import org.jboss.messaging.core.distributed.util.RpcServerCall;
import org.jboss.messaging.core.distributed.util.ServerResponse;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

/**
 * The class that mediates the access of a distributed destination instance to the group. Provides
 * generic behavior in top of which subclasses must layer their specific behavior.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
abstract class PeerSupport implements Peer, PeerFacade
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PeerSupport.class);

   protected static final long TIMEOUT = 30000000; // TODO make this configurable

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected RpcDispatcher dispatcher;
   protected RpcServer rpcServer;
   DistributedDestination destination;
   protected Serializable peerID;

   protected boolean joined = false;

   // Constructors --------------------------------------------------

   /**
    * @throws IllegalStateException in case the dispatcher is not configured with an RpcServer
    */
   public PeerSupport(DistributedDestination destination, RpcDispatcher dispatcher)
   {
      Object so = dispatcher.getServerObject();
      if (!(so instanceof RpcServer))
      {
         throw new IllegalStateException("RpcDispatcher must have a pre-installed RpcServer");
      }

      this.dispatcher = dispatcher;
      rpcServer = (RpcServer)so;

      this.destination = destination;

      // TODO - Do I need to have two different ids? Can't I just use one?
      this.peerID = generateUniqueID();
      joined = false;
   }

   // Peer implementation -------------------------------------------

   public Serializable getGroupID()
   {
      return destination.getDestinationID();
   }

   public PeerIdentity getPeerIdentity()
   {
      return new PeerIdentity(getGroupID(), peerID, dispatcher.getChannel().getLocalAddress());
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

      Set result = destination.getRemotePeers();

      result.add(getPeerIdentity());
      return result;
   }

   public synchronized void join() throws DistributedException
   {
      if (!dispatcher.getChannel().isConnected())
      {
         throw new DistributedException("The JGroups channel not connected");
      }

      log.debug(this + " joining distributed destination " + destination.getDestinationID());

      RpcServerCall rpcServerCall = createJoinCall();

      // multicast the intention to join the queue
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

            // I will never receive an acknowledgment from myself, since my server objects are not
            // registered yet, so I can safely link to peer.
            addRemotePeer(ack);
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
   }

   public synchronized void leave() throws DistributedException
   {
      log.debug(this + " leaving distributed destination " + destination.getDestinationID());

      // multicast the intention to leave the queue
      RpcServerCall rpcServerCall =
            new RpcServerCall(destination.getDestinationID(), "leave",
                              new Object[] {getPeerIdentity()},
                              new String[] {"org.jboss.messaging.core.distributed.PeerIdentity"});

      // TODO use the timout when I'll change the send() signature or deal with the timeout
      rpcServerCall.remoteInvoke(dispatcher, TIMEOUT);
      log.debug("synchronous remote invocation ended");
   }

   public Set ping() throws DistributedException
   {
      if (!joined)
      {
         return Collections.EMPTY_SET;
      }

      log.debug(this + " multicasting ping request");

      Set result = new HashSet();

      RpcServerCall rpcServerCall =
            new RpcServerCall(destination.getDestinationID(), "ping",
                              new Object[] {getPeerIdentity()},
                              new String[] {"org.jboss.messaging.core.distributed.PeerIdentity"});

      // TODO use the timout when I'll change the send() signature or deal with the timeout
      Collection responses = rpcServerCall.remoteInvoke(dispatcher, TIMEOUT);

      log.debug(this + " received " + responses.size() + " response(s)");

      ServerResponse r = null;
      try
      {
         for(Iterator i = responses.iterator(); i.hasNext(); )
         {
            r = (ServerResponse)i.next();
            log.debug(this + " received: " + r);

            Object o = r.getInvocationResult();
            if (o instanceof Throwable)
            {
               throw (Throwable)o;
            }

            PeerIdentity pid = (PeerIdentity)o;
            result.add(pid);
         }
      }
      catch(Throwable t)
      {
         String msg = RpcServer.
               subordinateToString(r.getCategory(), r.getSubordinateID(), r.getAddress()) +
               " failed to answer ping request";
         log.error(msg, t);
         throw new DistributedException(msg, t);
      }

      return result;
   }

   // PeerFacade implementation ------------------------------------

   public Serializable getID()
   {
      return peerID;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected abstract void addRemotePeer(Acknowledgment ack);

   protected abstract RpcServerCall createJoinCall();

   // Private -------------------------------------------------------

   private Serializable generateUniqueID()
   {
      return new GUID().toString();
   }

   // Inner classes -------------------------------------------------
}
