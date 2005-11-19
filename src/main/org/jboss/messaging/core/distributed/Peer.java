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

import java.util.Set;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Peer
{
   Serializable getGroupID();

   PeerIdentity getPeerIdentity();

   boolean hasJoined();

   /**
    * Returns a Set of PeerIdentity instances corresponding to peers that are part of the group.
    * It may return an empty set (in case the peer didn't join the group yet), but never null.
    */
   Set getView();

   /**
    * Connects the peer to the distributed destination. The underlying JChannel must be connected
    * at the time of the call.
    *
    * @exception DistributedException - a wrapper for exceptions thrown by the distributed layer.
    */
   void join() throws DistributedException;

   /**
    * Stops the peer and disconnects it from the distributed destination.
    *
    * @exception DistributedException - a wrapper for exceptions thrown by the distributed layer.
    */
   void leave() throws DistributedException;
}
