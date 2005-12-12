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
package org.jboss.jms.server.endpoint.delegate;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;

import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.server.endpoint.SessionEndpoint;

/**
 * Delegate class for SessionEndpoint
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class SessionEndpointDelegate extends EndpointDelegateBase implements SessionEndpoint
{
   protected SessionEndpoint del;
      
   public SessionEndpointDelegate(SessionEndpoint del)
   {
      this.del = del;
   }
   
   public Object getEndpoint()
   {
      return del;
   }

//   public void acknowledge(String messageID, String receiverID) throws JMSException
//   {
//      del.acknowledge(messageID, receiverID);
//   }
   
   public void acknowledge() throws JMSException
   {
      del.acknowledge();
   }

   public void addTemporaryDestination(Destination destination) throws JMSException
   {
      del.addTemporaryDestination(destination);
   }

   public void cancelDeliveries(String receiverID) throws JMSException
   {
      del.cancelDeliveries(receiverID);
   }

   public void close() throws JMSException
   {
      del.close();
   }

   public void closing() throws JMSException
   {
      del.closing();
   }

   public BrowserDelegate createBrowserDelegate(Destination queue, String messageSelector) throws JMSException
   {
      return del.createBrowserDelegate(queue, messageSelector);
   }

   public ConsumerDelegate createConsumerDelegate(Destination destination, String selector, boolean noLocal, String subscriptionName, boolean connectionConsumer) throws JMSException
   {
      return del.createConsumerDelegate(destination, selector, noLocal, subscriptionName, connectionConsumer);
   }

   public ProducerDelegate createProducerDelegate(Destination destination) throws JMSException
   {
      return del.createProducerDelegate(destination);
   }

   public Queue createQueue(String queueName) throws JMSException
   {
      return del.createQueue(queueName);
   }

   public Topic createTopic(String topicName) throws JMSException
   {
      return del.createTopic(topicName);
   }

   public void deleteTemporaryDestination(Destination destination) throws JMSException
   {
      del.deleteTemporaryDestination(destination);
   }

   public void unsubscribe(String subscriptionName) throws JMSException
   {
      del.unsubscribe(subscriptionName);
   }
   
}
