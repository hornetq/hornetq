/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.interfaces;

/**
 * A component that handles Messages. Handling could mean consumption or synchronous/asynchronous
 * forwarding to another Receiver(s).
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface Receiver
{
   /**
    * Hand over the message to the Receiver.
    *
    * <p>
    * When invoking this method, be prepared to deal with unchecked exceptions the Receiver may
    * throw. This is equivalent with a negative acknowledgement, but the sender may also decide
    * to remove this Receiver from its list and not attempt delivery to it anymore. 
    *
    * @param message the message to be handled by the Receiver.
    *
    * @return true if the Receiver acknowledges the Message and relieves the sender of the
    *         responsibity of dealing with the Message.
    */
   public boolean handle(Message message);
}
