/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.interfaces;

import java.io.Serializable;
import java.util.Set;

/**
 * A Message is a Routable that has a payload. The payload is what the sending clients submit to the
 * messaging system and expect to be delivered to the intended receiving clients. Each message has
 * an ID, which is not the same as the Routable ID. There can be two different Message copies in the
 * system carrying the same ID, while their Routable IDs are always different.
 *
 * When implementing this interface, make sure you override equals() and hashCode() such that two
 * Message instances with equals IDs are equal.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface Message extends Routable
{
   /**
    * Returns the message payload.
    */
   public Serializable getPayload();

}
