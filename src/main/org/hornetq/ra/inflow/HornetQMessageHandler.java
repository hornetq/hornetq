/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.ra.inflow;

import java.util.UUID;

import javax.jms.InvalidClientIDException;
import javax.jms.MessageListener;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.TransactionManager;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ClientSession.QueueQuery;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQMessage;

/**
 * The message handler
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @version $Revision: $
 */
public class HornetQMessageHandler implements MessageHandler
{
   /**
    * The logger
    */
   private static final Logger log = Logger.getLogger(HornetQMessageHandler.class);

   /**
    * Trace enabled
    */
   private static boolean trace = HornetQMessageHandler.log.isTraceEnabled();

   /**
    * The session
    */
   private final ClientSession session;

   private ClientConsumer consumer;

   /**
    * The endpoint
    */
   private MessageEndpoint endpoint;

   private final HornetQActivation activation;

   private boolean useLocalTx;
   
   private final int sessionNr;

   private final TransactionManager tm;

   public HornetQMessageHandler(final HornetQActivation activation, final TransactionManager tm, final ClientSession session, final int sessionNr)
   {
      this.activation = activation;
      this.session = session;
      this.sessionNr = sessionNr;
      this.tm = tm;
   }

   public void setup() throws Exception
   {
      if (HornetQMessageHandler.trace)
      {
         HornetQMessageHandler.log.trace("setup()");
      }

      HornetQActivationSpec spec = activation.getActivationSpec();
      String selector = spec.getMessageSelector();

      // Create the message consumer
      SimpleString selectorString = selector == null || selector.trim().equals("") ? null : new SimpleString(selector);
      if (activation.isTopic() && spec.isSubscriptionDurable())
      {
         String subscriptionName = spec.getSubscriptionName();
         String clientID = spec.getClientID();
         
         // Durable sub
         if (clientID == null)
         {
            throw new InvalidClientIDException("Cannot create durable subscription for " + subscriptionName + " - client ID has not been set");
         }

         SimpleString queueName = new SimpleString(HornetQDestination.createQueueNameForDurableSubscription(clientID, subscriptionName));

         QueueQuery subResponse = session.queueQuery(queueName);

         if (!subResponse.isExists())
         {
            session.createQueue(activation.getAddress(), queueName, selectorString, true);
         }
         else
         {
            // The check for already exists should be done only at the first session
            // As a deployed MDB could set up multiple instances in order to process messages in parallel.
            if (sessionNr == 0 && subResponse.getConsumerCount() > 0)
            {
               throw new javax.jms.IllegalStateException("Cannot create a subscriber on the durable subscription since it already has subscriber(s)");
            }

            SimpleString oldFilterString = subResponse.getFilterString();

            boolean selectorChanged = selector == null && oldFilterString != null ||
                                      oldFilterString == null && selector != null ||
                                      (oldFilterString != null && selector != null &&
                                      !oldFilterString.toString().equals(selector));

            SimpleString oldTopicName = subResponse.getAddress();

            boolean topicChanged = !oldTopicName.equals(activation.getAddress());

            if (selectorChanged || topicChanged)
            {
               // Delete the old durable sub
               session.deleteQueue(queueName);

               // Create the new one
               session.createQueue(activation.getAddress(), queueName, selectorString, true);
            }
         }
         consumer = session.createConsumer(queueName, null, false);
      }
      else
      {
         SimpleString queueName;
         if (activation.isTopic())
         {
            if (activation.getTopicTemporaryQueue() == null)
            {
               queueName = new SimpleString(UUID.randomUUID().toString());
               session.createQueue(activation.getAddress(), queueName, selectorString, false);
               activation.setTopicTemporaryQueue(queueName);
            }
            else
            {
               queueName = activation.getTopicTemporaryQueue(); 
            }
         }
         else
         {
            queueName = activation.getAddress();
         }
         consumer = session.createConsumer(queueName, selectorString);
      }

      // Create the endpoint, if we are transacted pass the sesion so it is enlisted, unless using Local TX
      MessageEndpointFactory endpointFactory = activation.getMessageEndpointFactory();
      useLocalTx = !activation.isDeliveryTransacted() && activation.getActivationSpec().isUseLocalTx();
      if (activation.isDeliveryTransacted() && !activation.getActivationSpec().isUseLocalTx())
      {
         endpoint = endpointFactory.createEndpoint(session);
      }
      else
      {
         endpoint = endpointFactory.createEndpoint(null);
      }
      consumer.setMessageHandler(this);
   }

   /**
    * Stop the handler
    */
   public void teardown()
   {
      if (HornetQMessageHandler.trace)
      {
         HornetQMessageHandler.log.trace("teardown()");
      }

      try
      {
         if (endpoint != null)
         {
            endpoint.release();
            endpoint = null;
         }
      }
      catch (Throwable t)
      {
         HornetQMessageHandler.log.debug("Error releasing endpoint " + endpoint, t);
      }
      
      try
      {
         consumer.close();
         if (activation.getTopicTemporaryQueue() != null)
         {
            // We need to delete temporary topics when the activation is stopped or messages will build up on the server
            SimpleString tmpQueue = activation.getTopicTemporaryQueue();
            QueueQuery subResponse = session.queueQuery(tmpQueue);
            if (subResponse.getConsumerCount() == 0)
            {
               session.deleteQueue(tmpQueue);
            }
         }
      }
      catch (Throwable t)
      {
         HornetQMessageHandler.log.debug("Error closing core-queue consumer", t);
      }

      try
      {
         if (session != null)
         {
            session.close();
         }
      }
      catch (Throwable t)
      {
         HornetQMessageHandler.log.debug("Error releasing session " + session, t);
      }
   }

   public void onMessage(final ClientMessage message)
   {
      if (HornetQMessageHandler.trace)
      {
         HornetQMessageHandler.log.trace("onMessage(" + message + ")");
      }

      HornetQMessage msg = HornetQMessage.createMessage(message, session);
      boolean beforeDelivery = false;

      try
      {
         if(activation.getActivationSpec().getTransactionTimeout() > 0 && tm != null)
         {
            tm.setTransactionTimeout(activation.getActivationSpec().getTransactionTimeout());
         }
         endpoint.beforeDelivery(HornetQActivation.ONMESSAGE);
         beforeDelivery = true;
         msg.doBeforeReceive();
         ((MessageListener)endpoint).onMessage(msg);
         message.acknowledge();
         try
         {
            endpoint.afterDelivery();
         }
         catch (ResourceException e)
         {
            HornetQMessageHandler.log.warn("Unable to call after delivery", e);
            return;
         }
         if (useLocalTx)
         {
            session.commit();
         }
      }
      catch (Throwable e)
      {
         HornetQMessageHandler.log.error("Failed to deliver message", e);
         // we need to call before/afterDelivery as a pair
         if (beforeDelivery)
         {
            try
            {
               endpoint.afterDelivery();
            }
            catch (ResourceException e1)
            {
               HornetQMessageHandler.log.warn("Unable to call after delivery", e);
            }
         }
         if (useLocalTx || !activation.isDeliveryTransacted())
         {
            try
            {
               session.rollback(true);
            }
            catch (HornetQException e1)
            {
               HornetQMessageHandler.log.warn("Unable to roll local transaction back");
            }
         }
      }

   }

}
