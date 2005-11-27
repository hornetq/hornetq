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

import org.jboss.logging.Logger;

import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class ViewKeeperSupport implements ViewKeeper
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ViewKeeperSupport.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Serializable groupID;

   // Map<peerIdentity - remotePeer>
   protected Map remotePeers;

   // Constructors --------------------------------------------------

   public ViewKeeperSupport(Serializable groupID)
   {
      this.groupID = groupID;
      remotePeers = Collections.synchronizedMap(new HashMap());
   }

   // ViewKeeper implementation -------------------------------------

   public Serializable getGroupID()
   {
      return groupID;
   }

   public void addRemotePeer(RemotePeer remotePeer)
   {
      PeerIdentity pid = remotePeer.getPeerIdentity();
      if (remotePeers.keySet().contains(pid))
      {
         throw new IllegalStateException(pid + " already added");
      }
      remotePeers.put(pid, remotePeer);
      if (log.isTraceEnabled()) { log.trace(this + " added " + remotePeer); }
   }

   public RemotePeer removeRemotePeer(PeerIdentity remotePeerIdentity)
   {
      RemotePeer removed = (RemotePeer)remotePeers.remove(remotePeerIdentity);
      if (log.isTraceEnabled()) { log.trace(this + " removed " + this); }
      return removed;
   }

   public Set getRemotePeers()
   {
      Set identities = new HashSet();
      for(Iterator i = remotePeers.keySet().iterator(); i.hasNext(); )
      {
         identities.add(i.next());
      }
      return identities;
   }

   public Iterator iterator()
   {
      return remotePeers.values().iterator();
   }

   // Public --------------------------------------------------------

   /**
    * Must return a string that contains the groupID and the associated peer ID.
    */
   public abstract String toString();

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
