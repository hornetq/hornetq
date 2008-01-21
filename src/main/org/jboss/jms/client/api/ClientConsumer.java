/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.client.api;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.message.JBossMessage;
import org.jboss.messaging.core.Destination;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface ClientConsumer extends Closeable
{
   String getID();
   
   void changeRate(float newRate) throws JMSException;

   MessageListener getMessageListener() throws JMSException;

   void setMessageListener(MessageListener listener) throws JMSException;

   Destination getDestination() throws JMSException;

   boolean getNoLocal() throws JMSException;

   String getMessageSelector() throws JMSException;

   Message receive(long timeout) throws JMSException;
   
   int getMaxDeliveries();
   
   boolean isShouldAck();
   
   void handleMessage(JBossMessage message) throws Exception;
   
   void addToFrontOfBuffer(JBossMessage message) throws JMSException;
   
   long getRedeliveryDelay();
}
