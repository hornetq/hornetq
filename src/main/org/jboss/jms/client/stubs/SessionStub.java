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
package org.jboss.jms.client.stubs;

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

import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.remoting.InvokerLocator;

/**
 * 
 * The client stub class for SessionDelegate
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class SessionStub extends ClientStubBase implements SessionDelegate
{
   private static final long serialVersionUID = -8096852898620279131L;
   
   public SessionStub(String objectID, InvokerLocator locator)
   {
      super(objectID, locator);
   }
   
   public void acknowledge(String messageID, String receiverID) throws JMSException
   {
   }

   public void acknowledgeSession() throws JMSException
   {
   }

   public void addTemporaryDestination(Destination destination) throws JMSException
   {
   }

   public void cancelDeliveries(String receiverID) throws JMSException
   {
   }

   public void close() throws JMSException
   {
   }

   public void closing() throws JMSException
   {
   }

   public void commit() throws JMSException
   {
   }

   public BrowserDelegate createBrowserDelegate(Destination queue, String messageSelector) throws JMSException
   {
      return null;
   }

   public BytesMessage createBytesMessage() throws JMSException
   {
      return null;
   }

   public ConsumerDelegate createConsumerDelegate(Destination destination, String selector, boolean noLocal, String subscriptionName, boolean connectionConsumer) throws JMSException
   {
      return null;
   }

   public MapMessage createMapMessage() throws JMSException
   {
      return null;
   }

   public Message createMessage() throws JMSException
   {
      return null;
   }

   public ObjectMessage createObjectMessage() throws JMSException
   {
      return null;
   }

   public ObjectMessage createObjectMessage(Serializable object) throws JMSException
   {
      return null;
   }

   public ProducerDelegate createProducerDelegate(Destination destination) throws JMSException
   {
      return null;
   }

   public Queue createQueue(String queueName) throws JMSException
   {
      return null;
   }

   public StreamMessage createStreamMessage() throws JMSException
   {
      return null;
   }

   public TextMessage createTextMessage() throws JMSException
   {
      return null;
   }

   public TextMessage createTextMessage(String text) throws JMSException
   {
      return null;
   }

   public Topic createTopic(String topicName) throws JMSException
   {
      return null;
   }

   public void deleteTemporaryDestination(Destination destination) throws JMSException
   {      
   }

   public MessageListener getMessageListener() throws JMSException
   {
      return null;
   }

   public void postDeliver(String messageID, String receiverID) throws JMSException
   {
   }

   public void preDeliver(String messageID, String receiverID) throws JMSException
   {
   }

   public void recover() throws JMSException
   {
   }

   public void rollback() throws JMSException
   {
   }

   public void run()
   {
   }

   public void setMessageListener(MessageListener listener) throws JMSException
   {
   }

   public void unsubscribe(String subscriptionName) throws JMSException
   {
   }
   
   public XAResource getXAResource()
   {
      return null;
   }
   
   public int getAcknowledgeMode()
   {
      return 0;
   }
      
   public boolean getTransacted()
   {
      return false;
   }
   
   public void addAsfMessage(Message m, String consumerID, ConsumerDelegate cons)
   {
   }
  
}
