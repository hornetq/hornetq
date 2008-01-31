/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.client.api;

import javax.jms.JMSException;
import javax.transaction.xa.XAResource;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface ClientSession extends XAResource
{
   ClientConnection getConnection();

   String getID();

   ClientConsumer createClientConsumer(Destination destination, String selector,
                                       boolean noLocal, String subscriptionName) throws JMSException;
   
   ClientBrowser createClientBrowser(Destination queue, String messageSelector) throws JMSException;
   
   ClientProducer createClientProducer(JBossDestination destination) throws JMSException;

   JBossQueue createQueue(String queueName) throws JMSException;

   JBossTopic createTopic(String topicName) throws JMSException;

   void delivered() throws JMSException;
   
   void addTemporaryDestination(Destination destination) throws JMSException;

   void deleteTemporaryDestination(Destination destination) throws JMSException;

   void unsubscribe(String subscriptionName) throws JMSException;

   void send(Message message) throws JMSException;

   XAResource getXAResource();

   void commit() throws JMSException;

   void rollback() throws JMSException;

   boolean isXA() throws JMSException;
   
   void removeConsumer(ClientConsumer consumer) throws JMSException;
   
   void removeProducer(ClientProducer producer);
   
   void removeBrowser(ClientBrowser browser);
   
   boolean isClosed();
   
   void closing() throws JMSException;
   
   void close() throws JMSException;
   
   //TOD hide these  private api
   void delivered(long deliveryID, boolean expired);
   
   void flushAcks() throws JMSException;
   
}
