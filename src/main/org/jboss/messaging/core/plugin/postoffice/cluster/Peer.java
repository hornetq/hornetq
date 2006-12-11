/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.plugin.postoffice.cluster;

import javax.management.NotificationBroadcaster;
import java.util.Set;


/**
 * Group management interface.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public interface Peer extends NotificationBroadcaster
{
   /**
    * Returns a set of nodeIDs (integers) representing the IDs of cluster's nodes.
    */
   Set getNodeIDView();


}
