/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import javax.jms.Message;
import javax.jms.JMSException;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface ConsumerDelegate
{

   /**
    * Receives the next message that arrives within the specified timeout interval. For a timeout
    * of 0, the call blocks indefinitely if there are no messages.
    */
   public Message receive(long timeout) throws JMSException;

}
