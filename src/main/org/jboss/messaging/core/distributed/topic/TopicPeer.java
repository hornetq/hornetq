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

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.messaging.core.distributed.topic.DistributedTopic;
import org.jboss.messaging.core.distributed.PeerSupport;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.distributed.RemotePeer;
import org.jboss.messaging.core.distributed.RemotePeerInfo;
import org.jboss.logging.Logger;
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
public class TopicPeer extends PeerSupport implements TopicFacade
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TopicPeer.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected Serializable replicatorID;
   protected DistributedTopic topic;

   // Constructors --------------------------------------------------

   public TopicPeer(DistributedTopic topic, RpcDispatcher dispatcher)
   {
      super(topic.getViewKeeper(), dispatcher);
      this.replicatorID = topic.getName() + "." + "Replicator";
      this.topic = topic;
   }

   // TopicFacade implementation ------------------------------------

   // Public --------------------------------------------------------

   public String toString()
   {
      return "TopicPeer[" + getPeerIdentity() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void doJoin() throws DistributedException
   {
      throw new NotYetImplementedException();
   }

   protected void doLeave() throws DistributedException
   {
      throw new NotYetImplementedException();
   }

   protected RemotePeer createRemotePeer(RemotePeerInfo newRemotePeerInfo)
   {
      throw new NotYetImplementedException();
   }

   protected RemotePeerInfo getRemotePeerInfo()
   {
      throw new NotYetImplementedException();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
