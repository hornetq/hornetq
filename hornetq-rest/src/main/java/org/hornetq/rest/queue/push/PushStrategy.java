package org.hornetq.rest.queue.push;

import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.jms.client.ConnectionFactoryOptions;
import org.hornetq.rest.queue.push.xml.PushRegistration;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public interface PushStrategy
{
   /**
    * Return false if unable to connect. Push consumer may be disabled if configured to do so when
    * unable to connect. Throw an exception if the message sent was unaccepted by the receiver.
    * Hornetq's retry and dead letter logic will take over from there.
    * @param message
    * @return {@code false} if unable to connect
    */
   boolean push(ClientMessage message);

   void setRegistration(PushRegistration reg);

   void start() throws Exception;

   void stop() throws Exception;

   void setJmsOptions(ConnectionFactoryOptions jmsOptions);
}
