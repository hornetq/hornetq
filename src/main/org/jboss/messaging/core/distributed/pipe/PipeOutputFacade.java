/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.distributed.pipe;

import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.distributed.util.ServerFacade;



/**
 * Exposes methods to be invoked remotely by the pipe input.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface PipeOutputFacade extends ServerFacade
{
   Delivery handle(Routable r);
}
