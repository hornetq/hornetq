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

import org.jboss.messaging.core.distributed.util.ServerFacade;

/**
 * Exposes methods to be invoked remotely by queue peers.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface PeerFacade extends ServerFacade
{
   /**
    * Method invocation received by all group peers when a new peer wants to join the group.
    *
    * @return a RemotePeerInfo representing the current peer. The new peer may use this information
    *         to create a RemotePeer representation of this peer. 
    *
    * @exception Throwable - negative acknowledgment. The join is vetoed (willingly or unwillingly)
    *            by this member.
    */
   RemotePeerInfo include(RemotePeerInfo newPeerInfo) throws Throwable;

   /**
    * Method invocation received by all group peers when a peer wants to leave the group.
    */
   void exclude(PeerIdentity originator);

   PeerIdentity ping(PeerIdentity originator);

}
