/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import java.io.Serializable;

/**
 * A message is a routable instance that has a payload. The payload is opaque to the messaging
 * system.
 *
 * When implementing this interface, make sure you override equals() and hashCode() such that two
 * Message instances with equals IDs are equal.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Message extends Routable
{
   
   Serializable getPayload();

}
