/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jgroups.Address;
import org.jboss.messaging.core.util.ServerDelegate;
import org.jboss.messaging.core.util.ServerDelegate;

import java.io.Serializable;

/**
 * Wraps togeter the methods to be invoked remotely by queue peers on other queue peers.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
interface QueueServerDelegate extends ServerDelegate
{
   /**
    * Remote method invoked by a queue peer on all other queue peers when joining the distributed
    * queue.
    *
    * @param joiningPeerAddress - the Address of the remote peer that is joining.
    * @param joiningPeerPipeID - the ID of the distributed pipe use by the remote peer that is
    *        joining to <i>receive</i> messages. It will share the same output endpoint to receive
    *        messages from different input endpoints.
    * @return a QueueJoinAcknowledgment instance.
    * @exception Exception - negative acknowledgment. The join is vetoed (willingly or unwillingly)
    *            by this member.
    */
   public QueueJoinAcknowledgment peerJoins(Address joiningPeerAddress,
                                            Serializable joiningPeerPipeID) throws Exception;

}
