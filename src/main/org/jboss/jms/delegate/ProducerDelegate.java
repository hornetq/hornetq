/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import javax.jms.Message;
import javax.jms.JMSException;

import org.jboss.jms.client.Closeable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface ProducerDelegate extends Closeable
{


   /**
    * The method's implementation expects to find all JMS headers (JMSDeliveryMode, etc.) correctly
    * set on the message.
    */
   public void send(Message m) throws JMSException;
      
}
