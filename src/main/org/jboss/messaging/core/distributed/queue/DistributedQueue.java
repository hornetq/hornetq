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

import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.distributed.Distributed;
import org.jboss.messaging.core.distributed.ViewKeeper;
import org.jboss.messaging.core.distributed.RemotePeer;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.distributed.Peer;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.messaging.util.SelectiveIterator;
import org.jboss.messaging.util.Util;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.List;

/**
 * A distributed queue.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DistributedQueue extends Queue implements Distributed
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DistributedQueue.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected QueuePeer peer;
   protected ViewKeeper viewKeeper;

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
   public DistributedQueue(String name, MessageStore ms, PersistenceManager tl,
                           RpcDispatcher dispatcher)
   {
      super(-1, ms, tl);
      viewKeeper = new QueueViewKeeper();
      peer = new QueuePeer(this, dispatcher);
   }

   // Queue overrides -----------------------------------------------

   public Iterator iterator()
   {
      return new SelectiveIterator(super.iterator(), RemotePeer.class);
   }

   // Channel overrides ---------------------------------------------

   public List browse(Filter f)
   {
      if (log.isTraceEnabled()) { log.trace(this + " browse" + (f == null ? "" : ", filter = " + f)); }

      List messages = peer.doRemoteBrowse(f);

      List local = super.browse(f);
      messages.addAll(local);

      return messages;
   }

   public boolean deliver(Receiver r)
   {
      boolean delivered = super.deliver(r);

      if (delivered)
      {
         return true;
      }

      // this peer has not redelivered any message but other peers may have messages to redeliver

      if (r instanceof RemoteQueue)
      {
         // this was a remote forward request, but there are no local messages to forward so return
         return false;
      }

      // no message in the local state, try remotely
      boolean peerWillForward = false;
      try
      {
         peerWillForward = peer.requestForward();
      }
      catch(DistributedException e)
      {
         log.error("Redelivery request failed", e);
         return false;
      }

      if (!peerWillForward)
      {
         return false;
      }

      // at least one peer will forward a message
      while(!delivered)
      {
         // TODO: experimental. Also see QueuePeer.requestForward() and QueueFacade.forward()
         try
         {
            Thread.sleep(200);
         }
         catch(InterruptedException e)
         {
            // ignore
         }

         if (log.isTraceEnabled()) { log.trace("retrying redelivery"); }
         delivered = super.deliver(r);
      }

      return delivered;
   }

   public void close()
   {
      try
      {
         leave();
      }
      catch(Exception e)
      {
         log.error("Distributed queue was not cleanly closed", e);
      }

      super.close();
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

   public Peer getPeer()
   {
      return peer;
   }

   // Public --------------------------------------------------------

   public List localBrowse(Filter filter)
   {
      if (log.isTraceEnabled()) { log.trace(this + " local browse" + (filter == null ? "" : ", filter = " + filter)); }
      return super.browse(filter);
   }

   public String toString()
   {
      return "DistributedQueue[" + getChannelID() + ":" + Util.guidToString(peer.getID()) + "]";
   }

   // Package protected ---------------------------------------------

   // ChannelSupport overrides --------------------------------------

   protected void processMessageBeforeStorage(MessageReference ref)
   {
      // clean REMOTE_ROUTABLE header before storage
      Message m = ref.getMessage();
      m.putHeader(Routable.REMOTE_ROUTABLE, null);
   }

   // Protected -----------------------------------------------------

   protected ViewKeeper getViewKeeper()
   {
      return viewKeeper;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   /**
    * The inner class that manages the local representation of the distributed destination view.
    */
   class QueueViewKeeper implements ViewKeeper
   {
      // Constants -----------------------------------------------------

      private final Logger log = Logger.getLogger(QueueViewKeeper.class);

      // Static --------------------------------------------------------

      // Attributes ----------------------------------------------------

      // Constructors --------------------------------------------------

      // ViewKeeper implementation -------------------------------------

      public Serializable getGroupID()
      {
         return "FIXME";
      }

      public void addRemotePeer(RemotePeer remotePeer)
      {
         if (log.isTraceEnabled()) { log.trace(this + " adding remote peer " + remotePeer); }

         router.add((RemoteQueue)remotePeer);
      }

      public RemotePeer removeRemotePeer(PeerIdentity remotePeerIdentity)
      {
         if (log.isTraceEnabled()) { log.trace(this + " removing remote peer " + remotePeerIdentity); }

         // TODO synchronization
         for(Iterator i = router.iterator(); i.hasNext(); )
         {
            Object receiver = i.next();
            if (receiver instanceof RemotePeer)
            {
               RemotePeer rp = (RemotePeer)receiver;
               if (rp.getPeerIdentity().equals(remotePeerIdentity))
               {
                  i.remove();
                  return rp;
               }
            }
         }
         return null;
      }

      public Set getRemotePeers()
      {
         Set result = new HashSet();
         for(Iterator i = router.iterator(); i.hasNext(); )
         {
            Object receiver = i.next();
            if (receiver instanceof RemotePeer)
            {
               RemotePeer rr = (RemotePeer)receiver;
               result.add(rr.getPeerIdentity());
            }
         }
         return result;
      }

      public Iterator iterator()
      {
         return router.iterator();
      }

      // Public --------------------------------------------------------

      public RemoteQueue getRemoteQueue(PeerIdentity peerIdentity)
      {
         // TODO synchronization
         for(Iterator i = router.iterator(); i.hasNext(); )
         {
            Object receiver = i.next();
            if (receiver instanceof RemoteQueue)
            {
               RemoteQueue rq = (RemoteQueue)receiver;
               if (rq.getPeerIdentity().equals(peerIdentity))
               {
                  return rq;
               }
            }
         }
         return null;
      }


      public String toString()
      {
         return "DistributedQueue[" + getChannelID() + "." + Util.guidToString(peer.getID()) + "].ViewKeeper";
      }

      // Package protected ---------------------------------------------

      // Protected -----------------------------------------------------

      protected ViewKeeper getViewKeeper()
      {
         return viewKeeper;
      }

      // Private -------------------------------------------------------

      // Inner classes -------------------------------------------------
   }
}
