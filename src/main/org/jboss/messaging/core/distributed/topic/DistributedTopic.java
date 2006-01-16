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

import org.jboss.messaging.core.local.Topic;
import org.jboss.messaging.core.distributed.Distributed;
import org.jboss.messaging.core.distributed.ViewKeeper;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.distributed.Peer;
import org.jboss.messaging.core.distributed.ViewKeeperSupport;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.plugin.contract.TransactionLogDelegate;
import org.jboss.messaging.core.plugin.contract.TransactionLogDelegate;
import org.jboss.messaging.util.Util;
import org.jboss.messaging.util.SelectiveIterator;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Collections;

/**
 * A distributed topic.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DistributedTopic extends Topic implements Distributed
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DistributedTopic.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected TopicPeer peer;
   protected ViewKeeper viewKeeper;
   protected TransactionLogDelegate pm;

   // Constructors --------------------------------------------------

   public DistributedTopic(String name, MessageStore ms,
                           TransactionLogDelegate pm, RpcDispatcher dispatcher)
   {
      super(name, ms);
      this.pm = pm;
      viewKeeper = new TopicViewKeeper(name);
      peer = new TopicPeer(new GUID().toString(), this, dispatcher);
      log.debug(this + " created");
   }

   // Topic overrides -----------------------------------------------

   public Iterator iterator()
   {
      return new SelectiveIterator(super.iterator(), RemoteTopic.class);
   }

   // Distributed implementation --------------------------

   public void join() throws DistributedException
   {
      peer.join();
      log.debug(this + " successfully joined the group");
   }

   public void leave() throws DistributedException
   {
      peer.leave();
      log.debug(this + " successfully left the group");
   }

   public void close() throws DistributedException
   {
      leave();
      // TODO - additional cleanup
   }

   public Peer getPeer()
   {
      return peer;
   }

   // Public --------------------------------------------------------

   /**
    * List of Messages in process of being delivered (for which this peer didn't get an
    * acknowledgment) or partially rejected/cancelled by some peers from the view in which it was
    * sent.
    *
    * TODO: i am not sure this method belongs here. Review.
    */
   public List browse()
   {
      RemoteTopic remoteTopic = getRemoteTopic();
      if (remoteTopic == null)
      {
         return Collections.EMPTY_LIST;
      }
      return remoteTopic.browse();
   }

   public String toString()
   {
      return "DistributedTopic[" + getName() + ":" + Util.guidToString(peer.getID()) + "]";
   }

   // Package protected ---------------------------------------------

   MessageStore getMessageStore()
   {
      return ms;
   }

   void addRemoteTopic()
   {
      // The distributed topic is represented on each peer by a *single* RemoteTopic instance,
      // because the RemoteTopic instance will delegate to a multicasting replicator.

      if (getRemoteTopic() != null)
      {
         if (log.isTraceEnabled()) { log.trace(this + ": remote topic already registered, returning"); }
         return;
      }

      RemoteTopic remoteTopic = new RemoteTopic(getName(), ms, pm, peer.getReplicator());
      router.add(remoteTopic);
      if (log.isTraceEnabled()) { log.trace(this + " added access to the distributed topic"); }
   }

   void removeRemoteTopic()
   {
      for(Iterator i = router.iterator(); i.hasNext(); )
      {
         Object o = i.next();
         if (o instanceof RemoteTopic)
         {
            if (log.isTraceEnabled()) { log.trace(this + " removing " + o); }
            i.remove();
            return;
         }
      }
      log.warn(this + ": NO remote topic to remove");
   }

   RemoteTopic getRemoteTopic()
   {
      for(Iterator i = router.iterator(); i.hasNext(); )
      {
         Object o = i.next();
         if (o instanceof RemoteTopic)
         {
            return (RemoteTopic)o;
         }
      }
      return null;
   }

   ViewKeeper getViewKeeper()
   {
      return viewKeeper;
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   /**
    * The inner class that manages the local representation of the distributed destination view.
    */
   private class TopicViewKeeper extends ViewKeeperSupport
   {
      // Constants -----------------------------------------------------

      // Static --------------------------------------------------------

      // Attributes ----------------------------------------------------

      // Constructors --------------------------------------------------

      public TopicViewKeeper(Serializable name)
      {
         super(name);
      }

      // Public --------------------------------------------------------

      public String toString()
      {
         return "DistributedTopic[" + getName() + ":" +
                Util.guidToString(peer.getID()) + "].ViewKeeper";
      }

      // Package protected ---------------------------------------------

      // Protected -----------------------------------------------------

      // Private -------------------------------------------------------

      // Inner classes -------------------------------------------------
   }
}
