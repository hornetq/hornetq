/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client;

import javax.jms.Message;
import javax.jms.JMSException;
import javax.jms.MessageListener;

/**
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface ConsumerDelegate extends Lifecycle
{
   /**
    * Receive a message.
    *
    * @param timeout the timeout.
    * @return the message.
    * @throws JMSException for any error.
    */
   Message receive(long timeout) throws JMSException;

   /**
    * Set the message listener.
    * @param listener the new message listener.
    * @throws JMSException for any error.
    */
   void setMessageListener(MessageListener listener) throws JMSException;

}
