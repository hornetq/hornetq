/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.core.util.ServerDelegate;
import org.jboss.messaging.core.Routable;


/**
 * Wraps togeter the methods to be invoked remotely by a replicator input.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
interface ReplicatorOutputServerDelegate extends ServerDelegate
{

   /**
    * The metohd to be called remotely by the replicator input in case of an unacknowledged
    * message re-delivery attempt. The redelivery attempts are unicast, not multicast.
    *
    * @return the acknowledgement as returned by the associated receiver.
    */
   public boolean handle(Routable r);

}
