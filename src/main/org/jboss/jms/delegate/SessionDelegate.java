/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface SessionDelegate
{
   public ProducerDelegate createProducerDelegate(Destination destination) throws JMSException;

   public MessageConsumer createConsumer(Destination destination) throws JMSException;

   public Message createMessage() throws JMSException;

}
