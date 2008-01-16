/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.client.api;

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.destination.JBossDestination;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface ClientProducer extends Closeable
{
   void setDisableMessageID(boolean value) throws JMSException;
   
   boolean isDisableMessageID() throws JMSException;
   
   void setDisableMessageTimestamp(boolean value) throws JMSException;
   
   boolean isDisableMessageTimestamp() throws JMSException;
   
   void setDeliveryMode(int deliveryMode) throws JMSException;
   
   int getDeliveryMode() throws JMSException;
   
   void setPriority(int defaultPriority) throws JMSException;
   
   int getPriority() throws JMSException;
   
   void setTimeToLive(long timeToLive) throws JMSException;
   
   long getTimeToLive() throws JMSException;
   
   JBossDestination getDestination() throws JMSException;
   
   void setDestination(JBossDestination dest) throws JMSException;

   /**
    * This method is only handled by the advice stack, the corresponding invocation is never sent
    * to the server.
    */
   void send(JBossDestination destination,
             Message message,
             int deliveryMode,
             int priority,
             long timeToLive) throws JMSException;

   void send(JBossDestination destination,
             Message message,
             int deliveryMode,
             int priority,
             long timeToLive, boolean keepOriginalID) throws JMSException;

}
