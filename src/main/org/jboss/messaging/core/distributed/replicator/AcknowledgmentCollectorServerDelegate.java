/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed.replicator;

import org.jboss.messaging.core.distributed.util.ServerFacade;
import org.jboss.messaging.core.distributed.util.ServerFacade;

import java.io.Serializable;

/**
 * Methods to be invoked remotely by the ReplicatorOutput instances.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
interface AcknowledgmentCollectorServerDelegate extends ServerFacade
{
   public void acknowledge(Serializable messageID, Serializable outputPeerID,
                           Serializable receiverID, Boolean positive);
}
