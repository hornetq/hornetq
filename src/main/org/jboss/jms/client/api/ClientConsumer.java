/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.client.api;

import javax.jms.JMSException;

import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface ClientConsumer
{
   String getID();
   
   void changeRate(float newRate) throws JMSException;

   MessageHandler getMessageHandler() throws JMSException;

   void setMessageHandler(MessageHandler handler) throws JMSException;

   Destination getDestination() throws JMSException;

   boolean getNoLocal() throws JMSException;

   String getMessageSelector() throws JMSException;

   Message receive(long timeout) throws JMSException;
   
   void handleMessage(DeliverMessage message) throws Exception;
   
   void closing() throws JMSException;
   
   void close() throws JMSException;
   
   void recover(long lastDeliveryID) throws JMSException;
}
