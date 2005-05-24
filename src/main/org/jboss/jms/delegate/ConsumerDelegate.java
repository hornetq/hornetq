/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import javax.jms.Message;
import javax.jms.JMSException;
import javax.jms.MessageListener;

import org.jboss.jms.client.Closeable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface ConsumerDelegate extends Closeable
{

   MessageListener getMessageListener() throws JMSException;
   void setMessageListener(MessageListener listener) throws JMSException;

   /**
    * @param timeout - a 0 timeout means wait forever and a negative value timeout means 
    *        "receiveNoWait".
    * @return
    * @throws JMSException
    */
   Message receive(long timeout) throws JMSException;
}
