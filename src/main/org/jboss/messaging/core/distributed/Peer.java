/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.distributed;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Peer
{
   PeerIdentity getPeerIdentity();

   boolean hasJoined();

   /**
    * Join distributed entity.
    */
   void join() throws DistributedException;

   /**
    * Leave distributed entity.
    */
   void leave() throws DistributedException;
}
