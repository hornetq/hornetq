/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client;

import org.jboss.messaging.jms.message.JBossMessage;

import javax.jms.Message;
import javax.jms.JMSException;

/**
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface ProducerDelegate extends Lifecycle
{
   /**
    * Send a message.
    *
    * @param message the message.
    * @throws JMSException for any error.
    */
   void send(Message message) throws JMSException;

   /**
    * Encapsulate a message
    *
    * @param message the message
    * @throws JMSException for any error
    */
   JBossMessage encapsulateMessage(Message message) throws JMSException;

}
