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
package org.jboss.messaging.core.distributed.queue;

import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.distributed.pipe.DistributedPipe;
import org.jboss.messaging.core.distributed.pipe.DistributedPipeOutput;
import org.jboss.messaging.core.distributed.util.RpcServerCall;
import org.jboss.messaging.core.distributed.util.ServerResponse;
import org.jboss.messaging.core.distributed.util.RpcServer;
import org.jboss.messaging.core.distributed.PeerSupport;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.distributed.RemotePeer;
import org.jboss.messaging.core.distributed.RemotePeerInfo;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * The class that mediates the access of a distributed queue instance to the group.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class QueuePeer extends PeerSupport implements QueueFacade
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(QueuePeer.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected Serializable pipeID;
   protected DistributedQueue queue;

   // Constructors --------------------------------------------------

   public QueuePeer(DistributedQueue queue, RpcDispatcher dispatcher)
   {
      super(new GUID().toString(), queue.getViewKeeper(), dispatcher);
      this.pipeID = new GUID().toString();
      this.queue = queue;
   }

   // QueueFacade implementation ------------------------------------

   public List remoteBrowse(PeerIdentity originator, Filter filter)
   {
      if (getPeerIdentity().equals(originator))
      {
         // ignore my own requests
         if (log.isTraceEnabled()) { log.trace(this + " got remote browse request from myself, ignoring ..."); }
         return Collections.EMPTY_LIST;
      }

      if (log.isTraceEnabled()) { log.trace(this + " got remote browse request" + (filter == null ? "" : ", filter = " + filter)); }
      return queue.localBrowse(filter);
   }

   public boolean forward(PeerIdentity targetID)
   {
      if (getPeerIdentity().equals(targetID))
      {
         // ignore my own requests
         if (log.isTraceEnabled()) { log.trace(this + " got forward request from myself, ignoring ..."); }
         return false;
      }

      if (log.isTraceEnabled()) { log.trace(this + " got forward request from " + targetID); }

      RemoteQueue target = ((DistributedQueue.QueueViewKeeper)viewKeeper).getRemoteQueue(targetID);
      return queue.deliver(target);
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "QueuePeer[" + getPeerIdentity() + "]";
   }

   // Package protected ---------------------------------------------

   /**
    * Multicast a browse request to the group.
    */
   List doRemoteBrowse(Filter filter)
   {
      if (log.isTraceEnabled()) { log.trace(this + " remote browse" + (filter == null ? "" : ", filter = " + filter)); }

      RpcServerCall rpcServerCall =
            new RpcServerCall(queue.getName(), "remoteBrowse",
                              new Object[] {getPeerIdentity(), filter},
                              new String[] {"org.jboss.messaging.core.distributed.PeerIdentity",
                                            "org.jboss.messaging.core.Filter"});

      // TODO use the timout when I'll change the send() signature or deal with the timeout
      Collection responses = rpcServerCall.remoteInvoke(dispatcher, TIMEOUT);

      if (log.isTraceEnabled()) { log.trace(this + " received " + responses.size() + " response(s) on browse request"); }

      List messages = new ArrayList();
      ServerResponse r = null;

      try
      {
         for(Iterator i = responses.iterator(); i.hasNext(); )
         {
            r = (ServerResponse)i.next();

            if (log.isTraceEnabled()) { log.trace(this + " received: " + r); }

            Object o = r.getInvocationResult();
            if (o instanceof Throwable)
            {
               throw (Throwable)o;
            }

            List l = (List)o;
            messages.addAll(l);
         }
      }
      catch(Throwable t)
      {
         String msg = RpcServer.
               subordinateToString(r.getCategory(), r.getSubordinateID(), r.getAddress()) +
               " failed to handle the browse request";

         log.error(msg, t);
         // TODO currently I just throw an unchecked exception, but I should modify browse() signature to throw a Throwable
         //throw new DistributedException(msg, t);
         throw new RuntimeException(msg, t);
      }
      return messages;
   }

   /**
    * TODO: experimental
    *
    * Multicast a forward request to the group.
    *
    * @return true if at least on member 'promises' to forward, false otherwise.
    * @exception DistributedException thrown in case of unsuccessful distributed RPC
    */
   boolean requestForward() throws DistributedException
   {
      if (log.isTraceEnabled()) { log.trace(this + " initiating a forward request"); }

      RpcServerCall rpcServerCall =
            new RpcServerCall(queue.getName(), "forward",
                              new Object[] {getPeerIdentity()},
                              new String[] {"org.jboss.messaging.core.distributed.PeerIdentity"});

      // TODO use the timout when I'll change the send() signature or deal with the timeout
      Collection responses = rpcServerCall.remoteInvoke(dispatcher, TIMEOUT);

      if (log.isTraceEnabled()) { log.trace(this + " received " + responses.size() + " response(s) on forward request"); }

      ServerResponse r = null;
      try
      {
         for(Iterator i = responses.iterator(); i.hasNext(); )
         {
            r = (ServerResponse)i.next();

            if (log.isTraceEnabled()) { log.trace(this + " received: " + r); }

            Object o = r.getInvocationResult();
            if (o instanceof Throwable)
            {
               throw (Throwable)o;
            }

            Boolean initiated = (Boolean)o;
            if (initiated.booleanValue())
            {
               // at least one peer initiated forwarding
               return true;
            }
         }
      }
      catch(Throwable t)
      {
         String msg = RpcServer.
            subordinateToString(r.getCategory(), r.getSubordinateID(), r.getAddress()) +
            " failed to handle the forward request";
         log.error(msg, t);
         throw new DistributedException(msg, t);
      }
      return false;
   }

   // PeerSupport overrides -----------------------------------------

   protected void doJoin() throws DistributedException
   {
      DistributedPipeOutput output = new DistributedPipeOutput(pipeID, queue);

      if (!rpcServer.registerUnique(pipeID, output))
      {
         throw new IllegalStateException("More than one server subordinates tried " +
                                         "to registers using id=" + pipeID);
      }

      rpcServer.register(queue.getName(), this);

      // also, register the peer individually to allow point-to-point calls
      rpcServer.register(peerID, this);
   }

   protected void doLeave() throws DistributedException
   {
      // unregister my pipe output
      rpcServer.unregister(pipeID);
      rpcServer.unregister(queue.getName(), this);
   }

   protected RemotePeer createRemotePeer(RemotePeerInfo thatPeerInfo)
   {
      QueuePeerInfo qpi = (QueuePeerInfo)thatPeerInfo;
      PeerIdentity remotePeerIdentity = qpi.getPeerIdentity();

      if (log.isTraceEnabled()) { log.trace(this + " adding remote peer " + remotePeerIdentity); }

      DistributedPipe p =
         new DistributedPipe(qpi.getPipeID(), dispatcher, remotePeerIdentity.getAddress());
      return new RemoteQueue(remotePeerIdentity, p);
   }

   protected RemotePeerInfo getRemotePeerInfo()
   {
      return new QueuePeerInfo(getPeerIdentity(), pipeID);
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
