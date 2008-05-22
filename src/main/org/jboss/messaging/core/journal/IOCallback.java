/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.journal;

import org.jboss.messaging.core.asyncio.AIOCallback;

/**
 * 
 * This class is just a direct extention of AIOCallback.
 * Just to avoid the direct dependency of org.jboss.messaging.core.asynciio.AIOCallback from the journal.
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface IOCallback extends AIOCallback
{
	
}
