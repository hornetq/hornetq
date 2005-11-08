/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.jms.delegate;


import java.io.Serializable;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.transaction.xa.XAResource;

import org.jboss.jms.MetaDataRepository;
import org.jboss.jms.client.Closeable;
import org.jboss.jms.tx.ResourceManager;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface SessionDelegate extends Closeable, MetaDataRepository
{
   public ProducerDelegate createProducerDelegate(Destination destination) throws JMSException;

   public ConsumerDelegate createConsumerDelegate(Destination destination, String selector,
                                                  boolean noLocal, String subscriptionName,
                                                  boolean connectionConsumer)
         throws JMSException;

   public Message createMessage() throws JMSException;
   
   public BytesMessage createBytesMessage() throws JMSException;
  
   public MapMessage createMapMessage() throws JMSException;

   public ObjectMessage createObjectMessage() throws JMSException;

   public ObjectMessage createObjectMessage(Serializable object) throws JMSException;

   public StreamMessage createStreamMessage() throws JMSException;

   public TextMessage createTextMessage() throws JMSException;
   
   public TextMessage createTextMessage(String text) throws JMSException;

   /**
    * Creates a queue identity given a Queue name. Does NOT create the physical queue. The physical
    * creation of queues is an administrative task and is not to be initiated by the JMS API, with
    * the exception of temporary queues.
    */
   public Queue createQueue(String queueName) throws JMSException;

   /**
    * Creates a topic identity given a Queue name. Does NOT create the physical topic. The physical
    * creation of topics is an administrative task and is not to be initiated by the JMS API, with
    * the exception of temporary topics.
    */
   public Topic createTopic(String topicName) throws JMSException;

   public void commit() throws JMSException;
   
   public void rollback() throws JMSException;
   
   public void recover() throws JMSException;

   public BrowserDelegate createBrowserDelegate(Destination queue, String messageSelector)
         throws JMSException;

   /**
    * Acknowledges a message back to the server (non transactionally).
    */
	public void acknowledge(String messageID, String receiverID)
         throws JMSException;
   
   public void preDeliver(String messageID, String receiverID)
      throws JMSException;
   
   public void postDeliver(String messageID, String receiverID)
      throws JMSException;
   

   /**
    * Acknowledges all messages received so far by this session.
    */
	public void acknowledgeSession() throws JMSException;

	public void cancelDeliveries(String receiverID) throws JMSException;
   
   /**
    * Add a temporary destination.
    */
   public void addTemporaryDestination(Destination destination) throws JMSException;
   
   /**
    * Delete a temporary destination
    */
   public void deleteTemporaryDestination(Destination destination) throws JMSException;
   
   /**
    * Unsubscribe the client from the durable subscription
    * specified by subscriptionName
    * 
    * @param subscriptionName the Name of the durable subscription to unsubscribe from
    * @throws JMSException if the unsubscribe fails
    */
   public void unsubscribe(String subscriptionName) throws JMSException;
   
   public XAResource getXAResource();
   
   public int getAcknowledgeMode() throws JMSException;
   
   public boolean getTransacted() throws JMSException;
   
   public void addAsfMessage(Message m, String receiverID, ConsumerDelegate cons);
   
   public MessageListener getMessageListener() throws JMSException;
   
   public void setMessageListener(MessageListener listener) throws JMSException;
   
   public void run();
   
   public void setAcknowledgeMode(int ackMode) throws JMSException;
   
   public void setTransacted(boolean transacted) throws JMSException;
   
   public void setXA(boolean XA);
   
   public boolean getXA();
   
   public void setXAResource(XAResource resource);
   
   public void setResourceManager(ResourceManager rm);
   
   public ResourceManager getResourceManager();
   
   public String getAsfReceiverID();
   
}
