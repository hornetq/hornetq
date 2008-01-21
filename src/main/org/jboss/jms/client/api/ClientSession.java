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
import javax.jms.MessageListener;
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
   
   /// Methods that will perform a server invocation ----------------------------------------------------------

   ClientConsumer createConsumerDelegate(Destination destination, String selector,
         boolean noLocal, String subscriptionName,
         boolean isCC) throws JMSException;
   

   ClientBrowser createBrowserDelegate(Destination queue, String messageSelector) throws JMSException;

   /**
    * Creates a queue identity given a Queue name. Does NOT create the physical queue. The physical
    * creation of queues is an administrative task and is not to be initiated by the JMS API, with
    * the exception of temporary queues.
    */
   JBossQueue createQueue(String queueName) throws JMSException;

   /**
    * Creates a topic identity given a Queue name. Does NOT create the physical topic. The physical
    * creation of topics is an administrative task and is not to be initiated by the JMS API, with
    * the exception of temporary topics.
    */
   JBossTopic createTopic(String topicName) throws JMSException;

   /**
    * Acknowledge a list of deliveries
    * @throws JMSException
    */
   void acknowledgeDeliveries(List<Ack> acks) throws JMSException;

   /**
    * Acknowledge a delivery
    * @throws JMSException
    */
   boolean acknowledgeDelivery(Ack ack) throws JMSException;

   /**
    * Cancel a list of deliveries.
    */
   void cancelDeliveries(List<Cancel> cancels) throws JMSException;
 
   /**
    * Cancel a delivery
    * @param cancelure 
    * @throws JMSException
    */
   void cancelDelivery(Cancel cancel) throws JMSException;

   /**
    * Add a temporary destination.
    */
   void addTemporaryDestination(Destination destination) throws JMSException;

   /**
    * Delete a temporary destination
    */
   void deleteTemporaryDestination(Destination destination) throws JMSException;

   /**
    * Unsubscribe the client from the durable subscription
    * specified by subscriptionName
    * 
    * @param subscriptionName the Name of the durable subscription to unsubscribe from
    * @throws JMSException if the unsubscribe fails
    */
   void unsubscribe(String subscriptionName) throws JMSException;

   /**
    * Send a message
    * @param message The message to send
    * @throws JMSException
    */
   void send(Message message) throws JMSException;

   int getDupsOKBatchSize();

   public boolean isStrictTck();


   /// Client methods -------------------------------------------------------------------------------

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

   MessageListener getMessageListener() throws JMSException;

   void setMessageListener(MessageListener listener) throws JMSException;

   void run() throws JMSException;

   XAResource getXAResource();

   void addAsfMessage(JBossMessage m, String consumerID, String queueName,
         int maxDeliveries, ClientSession connectionConsumerDelegate,
         boolean shouldAck) throws JMSException;

   boolean isTransacted() throws JMSException;

   int getAcknowledgeMode() throws JMSException;

   void commit() throws JMSException;

   void rollback() throws JMSException;

   void recover() throws JMSException;

   void redeliver(List deliveryInfos) throws JMSException;

   ClientProducer createProducerDelegate(JBossDestination destination) throws JMSException;

   void acknowledgeAll() throws JMSException;
   
   boolean isXA();
   
   boolean isTreatAsNonTransactedWhenNotEnlisted();
   
   public void setTreatAsNonTransactedWhenNotEnlisted(boolean treatAsNonTransactedWhenNotEnlisted);

   public Object getCurrentTxId();

   public void setCurrentTxId(Object currentTxId);
}
