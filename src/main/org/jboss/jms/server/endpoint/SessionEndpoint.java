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
package org.jboss.jms.server.endpoint;


import java.util.List;

import javax.jms.JMSException;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.client.api.ClientBrowser;
import org.jboss.jms.client.impl.Ack;
import org.jboss.jms.client.impl.Cancel;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerResponse;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
   *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface SessionEndpoint extends Closeable
{
   CreateConsumerResponse createConsumerDelegate(Destination destination, String selector,
                                           boolean noLocal, String subscriptionName,
                                           boolean connectionConsumer) throws JMSException;
   
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
    * @param cancel
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
}

