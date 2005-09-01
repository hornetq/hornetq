/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jgroups.Address;
import org.jboss.messaging.core.distributed.util.ServerFacade;

import java.io.Serializable;

/**
 * Exposes methods to be invoked remotely by queue peers.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface QueueFacade extends ServerFacade
{

   /**
    * Remote method invoked by a queue peer on all other queue peers when joining the distributed
    * queue.
    *
    * @param address - the address of the new peer.
    * @param id - the new peer's peer id.
    * @param pipeID - the id of pipe the new peer listens on.
    *
    * @exception Exception - negative acknowledgment. The join is vetoed (willingly or unwillingly)
    *            by this member.
    */
   Acknowledgment join(Address address, Serializable id, Serializable pipeID) throws Throwable;

}
