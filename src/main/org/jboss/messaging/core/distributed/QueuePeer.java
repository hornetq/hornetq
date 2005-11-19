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

import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.distributed.pipe.DistributedPipe;
import org.jboss.messaging.core.distributed.pipe.DistributedPipeOutput;
import org.jboss.messaging.core.distributed.util.RpcServerCall;
import org.jboss.messaging.core.distributed.util.ServerResponse;
import org.jboss.messaging.core.distributed.util.RpcServer;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.Address;

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
      super(queue, dispatcher);
      this.pipeID = new GUID().toString();
      this.queue = queue;
   }

   // Peer overrides ------------------------------------------------

   public synchronized void join() throws DistributedException
   {
      if(joined)
      {
         return;
      }

      super.join(); // do the generic stuff first

      DistributedPipeOutput output = new DistributedPipeOutput(pipeID, queue);

      if (!rpcServer.registerUnique(pipeID, output))
      {
         throw new IllegalStateException("More than one server subordinates tried " +
                                         "to registers using id=" + pipeID);
      }

      rpcServer.register(queue.getDestinationID(), this);
      joined = true;

      log.debug(this + " successfully joined distributed queue " + queue.getDestinationID());
   }

   public synchronized void leave() throws DistributedException
   {
      if(!joined)
      {
         return;
      }

      super.join(); // do the generic stuff first

      // unregister my pipe output
      rpcServer.unregister(pipeID);
      rpcServer.unregister(queue.getDestinationID(), this);
      joined = false;
   }

   // QueueFacade implementation ------------------------------------

   public Acknowledgment join(Address remoteAddress,
                              Serializable remotePeerID,
                              Serializable remotePipeID) throws Throwable
   {
      // I will never receive my own call, since the server objects are not registered
      // at the time of call

      Serializable destID = queue.getDestinationID();

      PeerIdentity remotePeerIdentity = new PeerIdentity(destID, remotePeerID, remoteAddress);

      log.debug("peer " + remotePeerIdentity + " wants to join");

      DistributedPipe p =  new DistributedPipe(remotePipeID, dispatcher, remoteAddress);
      RemoteReceiver rr = new RemoteReceiver(remotePeerIdentity, p);

      // TODO what happens if this peer receives this very moment a message to be
      // TODO delivered to the queue? Seding to the joining peer will fail, since its distributed
      // TODO pipe isn't completely functional yet. To add test case.

      queue.add(rr);

      PeerIdentity pi = new PeerIdentity(destID, peerID, dispatcher.getChannel().getLocalAddress());
      return new Acknowledgment(pi, p.getID());
   }

   public void leave(PeerIdentity originator)
   {
      if (getPeerIdentity().equals(originator))
      {
         // ignore my own requests
         return;
      }

      log.debug(this +": peer " + originator + " wants to leave");

      queue.removeRemotePeer(originator);
   }

   public List remoteBrowse(PeerIdentity originator, Filter filter)
   {
      if (getPeerIdentity().equals(originator))
      {
         // ignore my own requests
         return Collections.EMPTY_LIST;
      }

      if (log.isTraceEnabled()) { log.trace(this + " got remote browse request" + (filter == null ? "" : ", filter = " + filter)); }
      return queue.browse(filter);
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
   public List doRemoteBrowse(Filter filter)
   {
      RpcServerCall rpcServerCall =
            new RpcServerCall(queue.getDestinationID(), "remoteBrowse",
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

   // Protected -----------------------------------------------------

   protected void addRemotePeer(Acknowledgment ack)
   {
      PeerIdentity remotePeerIdentity = ack.getPeerIdentity();

      if (log.isTraceEnabled()) { log.trace(this + " adding remote peer " + remotePeerIdentity); }

      DistributedPipe p =
         new DistributedPipe(ack.getPipeID(), dispatcher, remotePeerIdentity.getAddress());
      RemoteReceiver rr = new RemoteReceiver(remotePeerIdentity, p);
      queue.add(rr);
   }

   protected RpcServerCall createJoinCall()
   {
      return new RpcServerCall(destination.getDestinationID(), "join",
                               new Object[] {dispatcher.getChannel().getLocalAddress(),
                                             peerID,
                                             pipeID},
                               new String[] {"org.jgroups.Address",
                                             "java.io.Serializable",
                                             "java.io.Serializable"});
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
