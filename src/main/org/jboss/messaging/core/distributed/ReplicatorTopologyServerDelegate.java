/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.util.ServerDelegate;
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
interface ReplicatorTopologyServerDelegate extends ServerDelegate
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
