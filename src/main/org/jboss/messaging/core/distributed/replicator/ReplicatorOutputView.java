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
package org.jboss.messaging.core.distributed.replicator;

import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.messaging.core.distributed.ViewKeeper;
import org.jboss.messaging.core.distributed.RemotePeer;

import java.io.Serializable;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ReplicatorOutputView implements ViewKeeper
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Serializable replicatorID;
   private Set remotePeers;

   // Constructors --------------------------------------------------

   public ReplicatorOutputView(Serializable replicatorID)
   {
      this.replicatorID = replicatorID;
      this.remotePeers = new HashSet();
   }

   // ViewKeeper implementation -------------------------------------

   public Serializable getGroupID()
   {
      return replicatorID;
   }

   public void addRemotePeer(RemotePeer remotePeer)
   {
      remotePeers.add(remotePeer);
   }

   public void removeRemotePeer(PeerIdentity remotePeerIdentity)
   {
      for(Iterator i = remotePeers.iterator(); i.hasNext(); )
      {
         RemotePeer rp = (RemotePeer)i.next();
         if (rp.getPeerIdentity().equals(remotePeerIdentity))
         {
            i.remove();
            break;
         }
      }
   }

   public Set getRemotePeers()
   {
      Set ids = new HashSet();
      for(Iterator i = remotePeers.iterator(); i.hasNext(); )
      {
         RemotePeer rp = (RemotePeer)i.next();
         ids.add(rp.getPeerIdentity());
      }
      return ids;
   }

   public Iterator iterator()
   {
      return remotePeers.iterator();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
