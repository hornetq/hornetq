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
package org.jboss.test.messaging.core.distributed;

import org.jboss.messaging.core.distributed.ViewKeeper;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.messaging.core.distributed.RemotePeer;
import org.jboss.logging.Logger;

import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SimpleViewKeeper implements ViewKeeper
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SimpleViewKeeper.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Serializable groupID;
   // set of PeerIdentity instances
   protected Set identities;

   // Constructors --------------------------------------------------

   public SimpleViewKeeper(Serializable groupID)
   {
      this.groupID = groupID;
      identities = Collections.synchronizedSet(new HashSet());
   }

   // ViewKeeper implementation -------------------------------------

   public Serializable getGroupID()
   {
      return groupID;
   }

   public void addRemotePeer(RemotePeer remotePeer)
   {
      log.debug("adding " + remotePeer.getPeerIdentity());
      identities.add(remotePeer.getPeerIdentity());

   }

   public void removeRemotePeer(PeerIdentity remotePeerIdentity)
   {
      log.debug("removing remote peer " + remotePeerIdentity);
      identities.remove(remotePeerIdentity);
   }

   public Set getRemotePeers()
   {
      return identities;
   }

   // Public --------------------------------------------------------

   public void clear()
   {
      identities.clear();
   }

   public String toString()
   {
      return "SimpleViewKeeper[" + groupID + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
