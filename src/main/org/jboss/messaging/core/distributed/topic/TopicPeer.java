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
package org.jboss.messaging.core.distributed.topic;

import org.jboss.messaging.core.distributed.PeerSupport;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.distributed.RemotePeer;
import org.jboss.messaging.core.distributed.RemotePeerInfo;
import org.jboss.messaging.core.distributed.ViewKeeper;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.messaging.core.distributed.replicator.ReplicatorOutput;
import org.jboss.messaging.core.distributed.replicator.Replicator;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jgroups.blocks.RpcDispatcher;

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

   protected DistributedTopic topic;

   protected Serializable replicatorID;
   protected ViewKeeper replicatorViewKeeper;
   protected Replicator replicator;
   protected ReplicatorOutput replicatorOutput;

   // Constructors --------------------------------------------------

   public TopicPeer(Serializable peerID, DistributedTopic topic, RpcDispatcher dispatcher)
   {
      super(peerID, topic.getViewKeeper(), dispatcher);
      this.replicatorID = topic.getName() + ".Replicator";
      this.topic = topic;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "TopicPeer[" + getPeerIdentity() + "]";
   }

   // Package protected ---------------------------------------------

   Replicator getReplicator()
   {
      return replicator;
   }

   // PeerSupport overrides -----------------------------------------

   protected void doJoin() throws DistributedException
   {
      MessageStore ms = topic.getMessageStore();
      replicator = new Replicator(replicatorID, dispatcher, ms, false);
      replicator.join();
      log.debug(replicator + " successfully joined the group");

      topic.addRemoteTopic();

      replicatorOutput = new ReplicatorOutput(replicatorID, dispatcher, ms, topic);
      replicatorOutput.ignore(replicator.getPeer().getPeerIdentity().getPeerID());
      replicatorOutput.join();
      log.debug(replicatorOutput + " successfully joined the group");

      rpcServer.register(topic.getName(), this);
      if (log.isTraceEnabled()) { log.trace(this + " registered"); }
   }

   protected void doLeave() throws DistributedException
   {
      topic.removeRemoteTopic();

      replicatorOutput.leave();
      log.debug(replicatorOutput + " successfully left the group");

      replicator.leave();
      log.debug(replicator + " successfully left the group");

      rpcServer.unregister(topic.getName(), this);
      if (log.isTraceEnabled()) { log.trace(this + " unregistered"); }
   }

   protected RemotePeer createRemotePeer(RemotePeerInfo thatPeerInfo)
   {
      TopicPeerInfo tpi = (TopicPeerInfo)thatPeerInfo;
      PeerIdentity pid = tpi.getPeerIdentity();
      return new RemotePeer(pid);
   }

   protected RemotePeerInfo getRemotePeerInfo()
   {
      return new TopicPeerInfo(getPeerIdentity());
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
