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
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.util.SelectiveIterator;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.List;

/**
 * A distributed queue peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DistributedQueue extends Queue implements DistributedDestination
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DistributedQueue.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected QueuePeer peer;

   // Constructors --------------------------------------------------

   /**
    * An non-recoverable queue peer.
    */
   public DistributedQueue(String name, MessageStore ms, RpcDispatcher dispatcher)
   {
      this(name, ms, null, dispatcher);
   }

   /**
    * A recoverable queue peer.
    */
   public DistributedQueue(String name, MessageStore ms, PersistenceManager pm,
                           RpcDispatcher dispatcher)
   {
      super(name, ms, pm);
      peer = new QueuePeer(this, dispatcher);
   }

   // Queue overrides -----------------------------------------------

   public Iterator iterator()
   {
      return new SelectiveIterator(super.iterator(), RemoteReceiver.class);
   }

   // Channel overrides ---------------------------------------------

   public List browse(Filter f)
   {
      if (log.isTraceEnabled()) { log.trace(this + " browse" + (f == null ? "" : ", filter = " + f)); }

      // multicast the browse request
      List messages = peer.doRemoteBrowse(f);

      List local = super.browse(f);
      messages.addAll(local);

      return messages;
   }

   // TODO - override deliver(Receiver r) for a clustered case
//   public void deliver(Receiver r)
//   {
//      if (log.isTraceEnabled()){ log.trace(r + " requested delivery on " + this); }
//   }

   // DistributedDestination implementation -------------------------

   // TODO I don't really need this method to be public. It must pe package protected
   public Serializable getDestinationID()
   {
      return getChannelID();
   }

   // TODO I don't really need this method to be public. It must pe package protected
   public void removeRemotePeer(PeerIdentity remotePeerIdentity)
   {
      if (log.isTraceEnabled()) { log.trace(this + " removing remote peer " + remotePeerIdentity); }

      // TODO synchronization
      for(Iterator i = router.iterator(); i.hasNext(); )
      {
         Object receiver = i.next();
         if (receiver instanceof RemoteReceiver)
         {
            RemoteReceiver rr = (RemoteReceiver)receiver;
            if (rr.getPeerIdentity().equals(remotePeerIdentity))
            {
               i.remove();
               break;
            }
         }
      }
   }

   // TODO I don't really need this method to be public. It must pe package protected
   public Set getRemotePeers()
   {
      Set result = new HashSet();
      for(Iterator i = router.iterator(); i.hasNext(); )
      {
         Object receiver = i.next();
         if (receiver instanceof RemoteReceiver)
         {
            RemoteReceiver rr = (RemoteReceiver)receiver;
            result.add(rr.getPeerIdentity());
         }
      }
      return result;
   }

   // Public --------------------------------------------------------

   public void join() throws DistributedException
   {
      peer.join();
   }

   public void leave() throws DistributedException
   {
      peer.leave();
   }

   public QueuePeer getPeer()
   {
      return peer;
   }

   public String toString()
   {
      return "DistributedQueue[" + getChannelID() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
