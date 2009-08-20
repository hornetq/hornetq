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
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.hornetq.jms.HornetQTopic;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.utils.SimpleString;

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
   private static boolean trace = log.isTraceEnabled();

   /**
    * The session
    */
   private final ClientSession session;

   /**
    * The endpoint
    */
   private MessageEndpoint endpoint;

   private final HornetQActivation activation;

   /**
    * The transaction demarcation strategy factory
    */
   private final DemarcationStrategyFactory strategyFactory = new DemarcationStrategyFactory();

   public HornetQMessageHandler(final HornetQActivation activation, final ClientSession session)
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

      HornetQActivationSpec spec = activation.getActivationSpec();
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

         SimpleString queueName = new SimpleString(HornetQTopic.createQueueNameForDurableSubscription(activation.getActivationSpec()
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

      HornetQMessage msg = HornetQMessage.createMessage(message, session);

      try
      {
         msg.doBeforeReceive();
         message.acknowledge();
      }
      catch (Exception e)
      {
         log.error("Failed to prepare message for receipt", e);

         return;
      }

      try
      {
         ((MessageListener) endpoint).onMessage(msg);
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
            catch (HornetQException e)
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
               catch (HornetQException e)
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

      private Transaction trans;

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

         tm.begin();

         try
         {
            trans = tm.getTransaction();

            if (trace)
            {
               log.trace(this + " using tx=" + trans);
            }

            if (!trans.enlistResource(session))
            {
               throw new JMSException("could not enlist resource");
            }
            if (trace)
            {
               log.trace(this + " XAResource '" + session + " enlisted.");
            }

         }
         catch (Throwable t)
         {
            try
            {
               tm.rollback();
            }
            catch (Throwable ignored)
            {
               log.trace(this + " ignored error rolling back after failed enlist", ignored);
            }
            throw t;
         }
      }

      public void error()
      {
         // Mark for tollback TX via TM
         try
         {
            if (trace)
            {
               log.trace(this + " using TM to mark TX for rollback tx=" + trans);
            }

            trans.setRollbackOnly();
         }
         catch (Throwable t)
         {
            log.error(this + " failed to set rollback only", t);
         }
      }

      public void end()
      {
         try
         {
            // Use the TM to commit the Tx (assert the correct association)
            Transaction currentTx = tm.getTransaction();
            if (!trans.equals(currentTx))
            {
               throw new IllegalStateException("Wrong tx association: expected " + trans + " was " + currentTx);
            }

            // Marked rollback
            if (trans.getStatus() == Status.STATUS_MARKED_ROLLBACK)
            {
               if (trace)
               {
                  log.trace(this + " rolling back JMS transaction tx=" + trans);
               }

               // Actually roll it back
               tm.rollback();

            }
            else if (trans.getStatus() == Status.STATUS_ACTIVE)
            {
               // Commit tx
               // This will happen if
               // a) everything goes well
               // b) app. exception was thrown
               if (trace)
               {
                  log.trace(this + " commiting the JMS transaction tx=" + trans);
               }

               tm.commit();

            }
            else
            {
               tm.suspend();
            }
         }
         catch (Throwable t)
         {
            log.error(this + " failed to commit/rollback", t);
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
