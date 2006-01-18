/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.plugin.contract;

import org.jboss.system.Service;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface ServerPlugin extends Service
{
   /**
    * A server plugin will be always accessed via a hard reference, so it is essential that each
    * implementation exposes this method.
    */
   Object getInstance();
}
