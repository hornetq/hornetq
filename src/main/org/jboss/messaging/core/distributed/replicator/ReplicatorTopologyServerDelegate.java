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

import org.jboss.messaging.core.distributed.util.ServerFacade;
import org.jgroups.Address;

import java.io.Serializable;
import java.util.Set;
import java.util.Map;


/**
 * Wraps togeter the methods to be invoked remotely by the replicator peers.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
interface ReplicatorTopologyServerDelegate extends ServerFacade
{
   /**
    * Remote method invoked by an output peer on all input peers' topologies when joining the
    * replicator.
    *
    * @exception Exception - negative acknowledgment. The join is vetoed (willingly or unwillingly)
    *            by this member.
    */
   public void outputPeerJoins(Serializable joiningPeerID, Address address) throws Exception;

   /**
    * Remote method invoked by a leaving output peer on all topologies.
    */
   public void outputPeerLeaves(Serializable leavingPeerID);


   /**
    * Returns a copy of the current view.
    * @return a Set of output peer IDs (Serializable instances).
    */
   public Set getView();

   /**
    * Returns a copy of the current view map.
    * @return a Map containing <output peer ID - JGroups Address>.
    */
   public Map getViewMap();

}
