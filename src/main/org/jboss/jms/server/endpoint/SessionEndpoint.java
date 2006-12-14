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
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.message.JBossMessage;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
   *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface SessionEndpoint extends Closeable
{
   ConsumerDelegate createConsumerDelegate(JBossDestination destination, String selector,
                                           boolean noLocal, String subscriptionName,
                                           boolean connectionConsumer,
                                           long failoverChannelID) throws JMSException;
   
   BrowserDelegate createBrowserDelegate(JBossDestination queue, String messageSelector)
      throws JMSException;

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
    * Acknowledge a batch of messages - used with client acknowledge or dups_ok acknowledge
    * @param ackInfos
    * @throws JMSException
    */
   void acknowledgeBatch(List deliveryIds) throws JMSException;
   
   /**
    * Acknowledge a message - used for auto acknowledge
    * @param deliveryId
    * @throws JMSException
    */
   void acknowledge(Ack ack) throws JMSException;
   
   /**
    * Add a temporary destination.
    */
   void addTemporaryDestination(JBossDestination destination) throws JMSException;
   
   /**
    * Delete a temporary destination
    */
   void deleteTemporaryDestination(JBossDestination destination) throws JMSException;
   
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
   void send(JBossMessage message) throws JMSException;
   
   /**
    * Cancel some deliveries.
    * This used at consumer close to cancel any undelivered messages left in the client buffer
    * or at session recovery to cancel any messages that couldn't be redelivered locally
    * @param ackInfos
    */
   void cancelDeliveries(List cancelInfos) throws JMSException;
      
   /**
    * Send delivery info to the server so the delivery lists can be repopulated
    * used at failover
    * @param ackInfos
    * @throws JMSException
    */
   void recoverDeliveries(List createInfos) throws JMSException;
}

