/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.util.ServerDelegate;
import org.jboss.messaging.interfaces.Receiver;


/**
 * Wraps togeter the methods to be invoked remotely by the distributed pipe input endpoints.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
interface PipeOutputServerDelegate extends Receiver, ServerDelegate
{
}
