/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.util.ServerDelegate;


/**
 * Answers to input peer pings.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
interface IdentityServerDelegate extends ServerDelegate
{

   /**
    * The metohd to be called remotely by the replicator input in to acquire the intial topology.
    * The identity of a replicator output is made of its peerID and its JGroups address.
    *
    * @return the replicator output's identity.
    */
   public PeerIdentity getIdentity();

}
