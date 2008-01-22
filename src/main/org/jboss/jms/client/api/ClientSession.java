/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.client.api;

import java.io.Serializable;
import java.util.List;

import javax.jms.JMSException;
import javax.transaction.xa.XAResource;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.client.impl.Ack;
import org.jboss.jms.client.impl.Cancel;
import org.jboss.jms.client.impl.DeliveryInfo;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossStreamMessage;
import org.jboss.jms.message.JBossTextMessage;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public interface ClientSession extends Closeable
{
   ClientConnection getConnection();

   String getID();

   ClientConsumer createClientConsumer(Destination destination, String selector,
         boolean noLocal, String subscriptionName,
         boolean isCC) throws JMSException;
   

   ClientBrowser createClientBrowser(Destination queue, String messageSelector) throws JMSException;
   
   ClientProducer createClientProducer(JBossDestination destination) throws JMSException;

   JBossQueue createQueue(String queueName) throws JMSException;

   JBossTopic createTopic(String topicName) throws JMSException;

   void acknowledgeDeliveries(List<Ack> acks) throws JMSException;

   boolean acknowledgeDelivery(Ack ack) throws JMSException;

   void cancelDeliveries(List<Cancel> cancels) throws JMSException;
 
   void cancelDelivery(Cancel cancel) throws JMSException;

   void addTemporaryDestination(Destination destination) throws JMSException;

   void deleteTemporaryDestination(Destination destination) throws JMSException;

   void unsubscribe(String subscriptionName) throws JMSException;

   void send(Message message) throws JMSException;

   int getDupsOKBatchSize() throws JMSException;

   public boolean isStrictTck() throws JMSException;

   JBossMessage createMessage() throws JMSException;

   JBossBytesMessage createBytesMessage() throws JMSException;

   JBossMapMessage createMapMessage() throws JMSException;

   JBossObjectMessage createObjectMessage() throws JMSException;

   JBossObjectMessage createObjectMessage(Serializable object) throws JMSException;

   JBossStreamMessage createStreamMessage() throws JMSException;

   JBossTextMessage createTextMessage() throws JMSException;

   JBossTextMessage createTextMessage(String text) throws JMSException;

   void preDeliver(DeliveryInfo deliveryInfo) throws JMSException;

   boolean postDeliver() throws JMSException;

   XAResource getXAResource();

   boolean isTransacted() throws JMSException;

   int getAcknowledgeMode() throws JMSException;

   void commit() throws JMSException;

   void rollback() throws JMSException;

   void recover() throws JMSException;

   void redeliver(List deliveryInfos) throws JMSException;

   void acknowledgeAll() throws JMSException;
   
   boolean isXA() throws JMSException;
   
   void setTreatAsNonTransactedWhenNotEnlisted(boolean treatAsNonTransactedWhenNotEnlisted);

   Object getCurrentTxId();

   void setCurrentTxId(Object currentTxId);
   
   /** This is a method used by children (Producer, Consumer and Browser) during close operations */
   void removeChild(String id) throws JMSException;
   
   boolean isClosed();
}
