/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.core.util.ServerDelegate;

import java.io.Serializable;

/**
 * Methods to be invoked remotely by the ReplicatorOutput instances.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
interface AcknowledgmentCollectorServerDelegate extends ServerDelegate
{
   public void acknowledge(Serializable messageID, Serializable outputPeerID,
                           Serializable receiverID, Boolean positive);
}
