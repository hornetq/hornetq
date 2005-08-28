/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import javax.jms.JMSException;
import javax.jms.Destination;
import javax.jms.Message;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.MetaDataRepository;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface ProducerDelegate extends Closeable, MetaDataRepository
{
   /**
    * Sends a message to the JMS provider.
    *
    * @param destination - the destination to send the message to. If null, the message will be sent
    *        to the producer's default destination.
    * @param message - the message to be sent.
    * @param deliveryMode - the delivery mode to use when sending this message. Must be one of
    *        DeliveryMode.PERSISTENT or DeliveryMode.NON_PERSISTENT. If -1, the message will be sent
    *        using the producer's default delivery mode.
    * @param priority - the priority to use when sending this message. A valid priority must be in
    *        the 0-9 range. If -1, the message will be sent using the producer's default priority.
    * @param timeToLive - the time to live to use when sending this message (in ms). Long.MIN_VALUE
    *        means the message will be sent using the producer's default timeToLive. 0 means live
    *        forever. For any other negative value, the message will be already expired when it is
    *        sent.
    *
    * @throws JMSException
    */
   public void send(Destination destination, Message message, int deliveryMode,
                    int priority, long timeToLive) throws JMSException;
      
}
