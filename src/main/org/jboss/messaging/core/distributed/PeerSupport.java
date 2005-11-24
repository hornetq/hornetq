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
import java.util.List;
import java.util.ArrayList;

/**
 * The class that mediates the access of a distributed destination instance to the group. Provides
 * generic behavior in top of which subclasses must layer their specific behavior.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class PeerSupport implements Peer, PeerFacade
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PeerSupport.class);

   protected static final long TIMEOUT = 30000000; // TODO make this configurable

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected RpcDispatcher dispatcher;
   protected RpcServer rpcServer;
   protected ViewKeeper viewKeeper;
   protected Serializable peerID;

   protected boolean joined = false;

   // Constructors --------------------------------------------------

   /**
    * @throws IllegalStateException in case the dispatcher is not configured with an RpcServer
    */
   public PeerSupport(ViewKeeper viewKeeper, RpcDispatcher dispatcher)
   {
      Object so = dispatcher.getServerObject();
      if (!(so instanceof RpcServer))
      {
         throw new IllegalStateException("RpcDispatcher must have a pre-installed RpcServer");
      }

      this.dispatcher = dispatcher;
      rpcServer = (RpcServer)so;
      this.viewKeeper = viewKeeper;
      this.peerID = generateUniqueID();
      joined = false;
   }

   // Peer implementation -------------------------------------------

   public Serializable getGroupID()
   {
      return viewKeeper.getGroupID();
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

      Set result = viewKeeper.getRemotePeers();
      result.add(getPeerIdentity());
      return result;
   }

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

      log.debug(this + " joining group " + viewKeeper.getGroupID());

      RemotePeerInfo thisPeerInfo = getRemotePeerInfo();
      RpcServerCall rpcServerCall =
         new RpcServerCall(viewKeeper.getGroupID(), "include",
                           new Object[] {thisPeerInfo},
                           new String[] {"org.jboss.messaging.core.distributed.RemotePeerInfo"});

      // multicast the intention to join the group
      // TODO use the timout when I'll change the send() signature or deal with the timeout
      Collection responses = rpcServerCall.remoteInvoke(dispatcher, TIMEOUT);

      log.debug(this + " received " + responses.size() + " response(s)");

      List remotePeers = new ArrayList();
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

            RemotePeerInfo thatPeerInfo = (RemotePeerInfo)o;
            remotePeers.add(thatPeerInfo);
         }
      }
      catch(Throwable t)
      {
         // TODO - what happens with the existent peers that already created distributed pipes
         //        (or whatever), to me? I should multicase a leave() request.

         String msg = RpcServer.
               subordinateToString(r.getCategory(), r.getSubordinateID(),r.getAddress()) +
               " vetoed " + this + " to join the group";
         log.error(msg, t);
         throw new DistributedException(msg, t);
      }

      for(Iterator i = remotePeers.iterator(); i.hasNext(); )
      {
         RemotePeerInfo thatPeerInfo = (RemotePeerInfo)i.next();
         RemotePeer rp = createRemotePeer(thatPeerInfo);
         viewKeeper.addRemotePeer(rp);
      }

      doJoin(); // do peer-specific join

      joined = true;

      log.debug(this + " successfully joined distributed destination " + getGroupID());
   }

   public synchronized void leave() throws DistributedException
   {
      if(!joined)
      {
         return;
      }

      log.debug(this + " leaving group " + viewKeeper.getGroupID());

      // multicast the intention to leave the group
      RpcServerCall rpcServerCall =
            new RpcServerCall(viewKeeper.getGroupID(), "exclude",
                              new Object[] {getPeerIdentity()},
                              new String[] {"org.jboss.messaging.core.distributed.PeerIdentity"});

      // TODO use the timout when I'll change the send() signature or deal with the timeout
      rpcServerCall.remoteInvoke(dispatcher, TIMEOUT);
      if (log.isTraceEnabled()) { log.trace("synchronous remote invocation successfully finished"); }

      doLeave(); // do destination-specific stuff

      joined = false;

      log.debug(this + " successfully left group " + getGroupID());
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
            new RpcServerCall(viewKeeper.getGroupID(), "ping",
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

   public RemotePeerInfo include(RemotePeerInfo newPeerInfo) throws Throwable
   {
      // I will never receive my own call, since the server objects are not registered
      // at the time of call

      Serializable groupID = viewKeeper.getGroupID();
      PeerIdentity remotePeerIdentity = newPeerInfo.getPeerIdentity();

      if (!groupID.equals(remotePeerIdentity.getGroupID()))
      {
         throw new IllegalArgumentException(newPeerInfo + " does not represent a peer of this " +
                                            "group (" + groupID + ")");
      }

      log.debug(this + ": peer " + remotePeerIdentity + " wants to be included in the group");
      RemotePeer rp = createRemotePeer(newPeerInfo);
      viewKeeper.addRemotePeer(rp);
      return getRemotePeerInfo();
   }

   public void exclude(PeerIdentity originator)
   {
      if (getPeerIdentity().equals(originator))
      {
         // ignore my own requests
         if (log.isTraceEnabled()) { log.trace(this + " got leave request from myself, ignoring ..."); }
         return;
      }

      log.debug(this +": peer " + originator + " wants to leave");

      viewKeeper.removeRemotePeer(originator);
   }

   public PeerIdentity ping(PeerIdentity originator)
   {
      if (log.isTraceEnabled()) { log.trace(this + " answering ping request from " + originator); }
      return getPeerIdentity();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected abstract void doJoin() throws DistributedException;
   protected abstract void doLeave() throws DistributedException;

   protected abstract RemotePeer createRemotePeer(RemotePeerInfo newRemotePeerInfo);
   protected abstract RemotePeerInfo getRemotePeerInfo();

   // Private -------------------------------------------------------

   private Serializable generateUniqueID()
   {
      return new GUID().toString();
   }

   // Inner classes -------------------------------------------------
}
