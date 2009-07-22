/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.ra.inflow;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossMessage;
import org.jboss.messaging.utils.SimpleString;

import javax.jms.InvalidClientIDException;
import javax.jms.MessageListener;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.util.UUID;

/**
 * The message handler
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @version $Revision: $
 */
public class JBMMessageHandler implements MessageHandler
{
   /**
    * The logger
    */
   private static final Logger log = Logger.getLogger(JBMMessageHandler.class);

   /**
    * Trace enabled
    */
   private static boolean trace = log.isTraceEnabled();

   /**
    * The session
    */
   private final ClientSession session;

   /**
    * The endpoint
    */
   private MessageEndpoint endpoint;

   private final JBMActivation activation;

   /**
    * The transaction demarcation strategy factory
    */
   private final DemarcationStrategyFactory strategyFactory = new DemarcationStrategyFactory();

   public JBMMessageHandler(final JBMActivation activation, final ClientSession session)
   {
      this.activation = activation;
      this.session = session;
   }

   public void setup() throws Exception
   {
      if (trace)
      {
         log.trace("setup()");
      }

      JBMActivationSpec spec = activation.getActivationSpec();
      String selector = spec.getMessageSelector();

      // Create the message consumer
      ClientConsumer consumer;
      SimpleString selectorString = selector == null || selector.trim().equals("") ? null : new SimpleString(selector);
      if (activation.isTopic() && spec.isSubscriptionDurable())
      {
         String subscriptionName = spec.getSubscriptionName();

         // Durable sub

         if (activation.getActivationSpec().getClientID() == null)
         {
            throw new InvalidClientIDException("Cannot create durable subscription - client ID has not been set");
         }

         SimpleString queueName = new SimpleString(JBossTopic.createQueueNameForDurableSubscription(activation.getActivationSpec()
                                                                                                              .getClientID(),
                                                                                                    subscriptionName));

         SessionQueueQueryResponseMessage subResponse = session.queueQuery(queueName);

         if (!subResponse.isExists())
         {
            session.createQueue(activation.getAddress(), queueName, selectorString, true);
         }
         else
         {
            // Already exists
            if (subResponse.getConsumerCount() > 0)
            {
               throw new javax.jms.IllegalStateException("Cannot create a subscriber on the durable subscription since it already has subscriber(s)");
            }

            SimpleString oldFilterString = subResponse.getFilterString();

            boolean selectorChanged = selector == null && oldFilterString != null ||
                                      oldFilterString == null &&
                                      selector != null ||
                                      oldFilterString != null &&
                                      selector != null &&
                                      !oldFilterString.equals(selector);

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
            queueName = new SimpleString(UUID.randomUUID().toString());
            session.createQueue(activation.getAddress(), queueName, selectorString, false);
         }
         else
         {
            queueName = activation.getAddress();
         }
         consumer = session.createConsumer(queueName, selectorString);
      }

      // Create the endpoint, if we are transacted pass the sesion so it is enlisted, unless using Local TX
      MessageEndpointFactory endpointFactory = activation.getMessageEndpointFactory();
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
      if (trace)
      {
         log.trace("teardown()");
      }

      try
      {
         if (endpoint != null)
         {
            endpoint.release();
         }
      }
      catch (Throwable t)
      {
         log.debug("Error releasing endpoint " + endpoint, t);
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
         log.debug("Error releasing session " + session, t);
      }
   }

   public void onMessage(final ClientMessage message)
   {
      if (trace)
      {
         log.trace("onMessage(" + message + ")");
      }

      TransactionDemarcationStrategy txnStrategy = strategyFactory.getStrategy();
      try
      {
         txnStrategy.start();
      }
      catch (Throwable throwable)
      {
         log.warn("Unable to create transaction: " + throwable.getMessage());
         txnStrategy = new NoTXTransactionDemarcationStrategy();
      }

      JBossMessage jbm = JBossMessage.createMessage(message, session);

      try
      {
         jbm.doBeforeReceive();
         message.acknowledge();
      }
      catch (Exception e)
      {
         log.error("Failed to prepare message for receipt", e);

         return;
      }

      try
      {
         ((MessageListener)endpoint).onMessage(jbm);
      }
      catch (Throwable t)
      {
         log.error("Unexpected error delivering message " + message, t);
         txnStrategy.error();
      }
      finally
      {
         txnStrategy.end();
      }
   }

   /**
    * Demarcation strategy factory
    */
   private class DemarcationStrategyFactory
   {
      /**
       * Get the transaction demarcation strategy
       *
       * @return The strategy
       */
      TransactionDemarcationStrategy getStrategy()
      {
         if (trace)
         {
            log.trace("getStrategy()");
         }

         if (activation.isDeliveryTransacted())
         {
            if (!activation.getActivationSpec().isUseLocalTx())
            {
               try
               {
                  return new XATransactionDemarcationStrategy();
                  }
                  catch (Throwable t)
                  {
                     log.error(this + " error creating transaction demarcation ", t);
                  }
            }
            else
            {
               return new LocalDemarcationStrategy();
            }

         }
         else
         {
            if (!activation.getActivationSpec().isUseLocalTx())
            {
               return new NoTXTransactionDemarcationStrategy();
            }
            else
            {
               return new LocalDemarcationStrategy();
            }
         }

         return null;
      }
   }

   /**
    * Transaction demarcation strategy
    */
   private interface TransactionDemarcationStrategy
   {
      /*
      * Start
      */
      void start() throws Throwable;

      /**
       * Error
       */
      void error();

      /**
       * End
       */
      void end();
   }

   /**
    * Local demarcation strategy
    */
   private class LocalDemarcationStrategy implements TransactionDemarcationStrategy
   {
      private boolean rolledBack = false;
      /*
      * Start
      */

      public void start()
      {
      }

      /**
       * Error
       */
      public void error()
      {
         if (trace)
         {
            log.trace("error()");
         }

         if (session != null)
         {
            try
            {
               session.rollback();
               rolledBack = true;
            }
            catch (MessagingException e)
            {
               log.error("Failed to rollback session transaction", e);
            }
         }
      }

      /**
       * End
       */
      public void end()
      {
         if (trace)
         {
            log.trace("end()");
         }

         if (!rolledBack)
         {
            if (session != null)
            {
               try
               {
                  session.commit();
               }
               catch (MessagingException e)
               {
                  log.error("Failed to commit session transaction", e);
               }
            }
         }
      }
   }

   /**
    * XA demarcation strategy
    */
   private class XATransactionDemarcationStrategy implements TransactionDemarcationStrategy
   {
      private final TransactionManager tm = activation.getTransactionManager();

      public void start() throws Throwable
      {
         final int timeout = activation.getActivationSpec().getTransactionTimeout();

         if (timeout > 0)
         {
            if (trace)
            {
               log.trace("Setting transactionTimeout for JMSSessionPool to " + timeout);
            }

            tm.setTransactionTimeout(timeout);
         }
         endpoint.beforeDelivery(JBMActivation.ONMESSAGE);
      }

      public void error()
      {
         try
         {
            try
            {
               tm.getTransaction().setRollbackOnly();
            }
            catch (SystemException e)
            {
               log.error("Unable to mark transaction as rollback only", e);
            }
            endpoint.afterDelivery();
         }
         catch (ResourceException e)
         {
            log.error("Error calling after delivery on endpoint", e);
         }
      }

      public void end()
      {
         try
         {
            endpoint.afterDelivery();
         }
         catch (ResourceException e)
         {
            log.error("Error calling after delivery on endpoint", e);
         }
      }
   }

   private class NoTXTransactionDemarcationStrategy implements TransactionDemarcationStrategy
   {
      public void start() throws Throwable
      {
      }

      public void error()
      {
      }

      public void end()
      {
      }
   }
}
