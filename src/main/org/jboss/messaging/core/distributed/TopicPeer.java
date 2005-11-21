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

import org.jboss.messaging.core.distributed.util.RpcServerCall;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.Address;

import java.io.Serializable;

/**
 * The class that mediates the access of a distributed topic instance to the group.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class TopicPeer extends PeerSupport implements TopicFacade
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TopicPeer.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
//   protected Serializable pipeID;
   protected DistributedTopic topic;

   // Constructors --------------------------------------------------

   public TopicPeer(DistributedTopic topic, RpcDispatcher dispatcher)
   {
      super(topic.getViewKeeper(), dispatcher);
//      this.pipeID = new GUID().toString();
      this.topic = topic;
   }

   // Peer overrides ------------------------------------------------

   public synchronized void join() throws DistributedException
   {
      if(joined)
      {
         return;
      }

      super.join(); // do the generic stuff first

//      DistributedPipeOutput output = new DistributedPipeOutput(pipeID, queue);
//
//      if (!rpcServer.registerUnique(pipeID, output))
//      {
//         throw new IllegalStateException("More than one server subordinates tried " +
//                                         "to registers using id=" + pipeID);
//      }
//
//      rpcServer.register(queue.getName(), this);
//      joined = true;
//
      log.debug(this + " successfully joined distributed topic " + topic.getName());
      throw new NotYetImplementedException();
   }

   public synchronized void leave() throws DistributedException
   {
      if(!joined)
      {
         return;
      }

      super.leave(); // do the generic stuff first

//      // unregister my pipe output
//      rpcServer.unregister(pipeID);
      rpcServer.unregister(topic.getName(), this);
      joined = false;
      throw new NotYetImplementedException();
   }

   // TopicFacade implementation ------------------------------------

   public Acknowledgment join(Address remoteAddress,
                              Serializable remotePeerID,
                              Serializable remotePipeID) throws Throwable
   {
      // I will never receive my own call, since the server objects are not registered
      // at the time of call

      Serializable destID = topic.getName();

      PeerIdentity remotePeerIdentity = new PeerIdentity(destID, remotePeerID, remoteAddress);

      log.debug(this + ": peer " + remotePeerIdentity + " wants to join");

//      DistributedPipe p =  new DistributedPipe(remotePipeID, dispatcher, remoteAddress);
//      RemoteReceiver rr = new RemoteReceiver(remotePeerIdentity, p);
//
//      // TODO what happens if this peer receives this very moment a message to be
//      // TODO delivered to the queue? Seding to the joining peer will fail, since its distributed
//      // TODO pipe isn't completely functional yet. To add test case.
//
//      queue.add(rr);
//
//      PeerIdentity pi = new PeerIdentity(destID, peerID, dispatcher.getChannel().getLocalAddress());
//      return new Acknowledgment(pi, p.getID());
       throw new NotYetImplementedException();
   }

   public void leave(PeerIdentity originator)
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

   public String toString()
   {
      return "TopicPeer[" + getPeerIdentity() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void addRemotePeer(Acknowledgment ack)
   {
      PeerIdentity remotePeerIdentity = ack.getPeerIdentity();

      if (log.isTraceEnabled()) { log.trace(this + " adding remote peer " + remotePeerIdentity); }

//      DistributedPipe p =
//         new DistributedPipe(ack.getPipeID(), dispatcher, remotePeerIdentity.getAddress());
//      RemoteReceiver rr = new RemoteReceiver(remotePeerIdentity, p);
//      queue.add(rr);
      throw new NotYetImplementedException();
   }

   protected RpcServerCall createJoinCall()
   {
//      return new RpcServerCall(queue.getName(), "join",
//                               new Object[] {dispatcher.getChannel().getLocalAddress(),
//                                             peerID,
//                                             pipeID},
//                               new String[] {"org.jgroups.Address",
//                                             "java.io.Serializable",
//                                             "java.io.Serializable"});
      throw new NotYetImplementedException();
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
