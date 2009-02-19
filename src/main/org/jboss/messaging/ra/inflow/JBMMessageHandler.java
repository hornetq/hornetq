/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2006, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkListener;
import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.jboss.messaging.core.logging.Logger;

/**
 * The message handler
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMMessageHandler implements MessageListener, Work, WorkListener
{
     /** The logger */
   private static final Logger log = Logger.getLogger(JBMMessageHandler.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The message handler pool */
   private JBMMessageHandlerPool pool;

   /** Is in use */
   private AtomicBoolean inUse;

   /** Done latch */
   private CountDownLatch done;

   /** The transacted flag */
   private boolean transacted;

   /** The acknowledge mode */
   private int acknowledge;

   /** The session */
   private Session session;

   /** Any XA session */
   private XASession xaSession;

   /** The message consumer */
   private MessageConsumer messageConsumer;

   /** The endpoint */
   private MessageEndpoint endpoint;

   /** The transaction demarcation strategy */
   private TransactionDemarcationStrategy txnStrategy;

   /**
    * Constructor
    * @param pool The message handler pool
    */
   public JBMMessageHandler(JBMMessageHandlerPool pool)
   {
      if (trace)
         log.trace("constructor(" + pool + ")");

      this.pool = pool;
   }

   /**
    * Setup the session
    */
   public void setup() throws Exception
   {
      if (trace)
         log.trace("setup()");

      inUse = new AtomicBoolean(false);
      done = new CountDownLatch(1);
      
      JBMActivation activation = pool.getActivation();
      JBMActivationSpec spec = activation.getActivationSpec();
      String selector = spec.getMessageSelector();

      Connection connection = activation.getConnection();

      // Create the session
      if (activation.isDeliveryTransacted())
      {
         xaSession = ((XAConnection)connection).createXASession();
         session = xaSession.getSession();
      } 
      else
      {
         transacted = spec.isSessionTransacted();
         acknowledge = spec.getAcknowledgeModeInt();
         session = connection.createSession(transacted, acknowledge);
      }

      // Create the message consumer
      if (activation.isTopic() && spec.isSubscriptionDurable())
      {
         Topic topic = (Topic) activation.getDestination();
         String subscriptionName = spec.getSubscriptionName();

         if (selector == null || selector.trim().equals(""))
         {
            messageConsumer = (MessageConsumer)session.createDurableSubscriber(topic, subscriptionName);
         }
         else
         {
            messageConsumer = (MessageConsumer)session.createDurableSubscriber(topic, subscriptionName, selector, false);
         }
      }
      else
      {
         if (selector == null || selector.trim().equals(""))
         {
            messageConsumer = session.createConsumer(activation.getDestination());
         }
         else
         {
            messageConsumer = session.createConsumer(activation.getDestination(), selector);
         }
      }

      // Create the endpoint
      MessageEndpointFactory endpointFactory = activation.getMessageEndpointFactory();
      XAResource xaResource = null;
      
      if (activation.isDeliveryTransacted() && xaSession != null)
         xaResource = xaSession.getXAResource();
      
      endpoint = endpointFactory.createEndpoint(xaResource);

      // Set the message listener
      messageConsumer.setMessageListener(this);
   }

   /**
    * Stop the handler
    */
   public void teardown()
   {
      if (trace)
         log.trace("teardown()");
      
      try
      {
         if (endpoint != null)
            endpoint.release();
      } 
      catch (Throwable t)
      {
         log.debug("Error releasing endpoint " + endpoint, t);
      }

      try
      {
         if (xaSession != null)
            xaSession.close();
      }
      catch (Throwable t)
      {
         log.debug("Error releasing xaSession " + xaSession, t);
      }

      try
      {
         if (session != null)
            session.close();
      }
      catch (Throwable t)
      {
         log.debug("Error releasing session " + session, t);
      }
   }

   /**
    * Is in use
    * @return True if in use; otherwise false
    */
   public boolean isInUse()
   {
      if (trace)
         log.trace("isInUse()");
      
      return inUse.get();
   }

   /**
    * On message
    * @param message The message
    */
   public void onMessage(Message message)
   {
      if (trace)
         log.trace("onMessage(" + message + ")");

      inUse.set(true);
      
      try
      {
         endpoint.beforeDelivery(JBMActivation.ONMESSAGE);

         try
         {
            MessageListener listener = (MessageListener) endpoint;
            listener.onMessage(message);
         } 
         finally
         {
            endpoint.afterDelivery();
         }
      }
      catch (Throwable t)
      {
         log.error("Unexpected error delivering message " + message, t);

         if (txnStrategy != null)
            txnStrategy.error();
      }

      inUse.set(false);
      done.countDown();
   }

   /**
    * Run
    */
   public void run()
   {
      if (trace)
         log.trace("run()");

      try
      {
         setup();

         txnStrategy = createTransactionDemarcation();
      } 
      catch (Throwable t)
      {
         log.error("Error creating transaction demarcation. Cannot continue.");
         return;
      }

      try
      {
         // Wait for onMessage
         while (done.getCount() > 0)
         {
            try
            {
               done.await();
            }
            catch (InterruptedException ignore)
            {
            }
         }
      }
      catch (Throwable t)
      {
         if (txnStrategy != null)
            txnStrategy.error();

      }
      finally
      {
         if (txnStrategy != null)
            txnStrategy.end();

         txnStrategy = null;
      }
   }

   /**
    * Release
    */
   public void release()
   {
      if (trace)
         log.trace("release()");
   }

   /**
    * Work accepted
    * @param e The work event
    */
   public void workAccepted(WorkEvent e)
   {
      if (trace)
         log.trace("workAccepted()");
   }

   /**
    * Work completed
    * @param e The work event
    */
   public void workCompleted(WorkEvent e)
   {
      if (trace)
         log.trace("workCompleted()");

      teardown();
      pool.removeHandler(this);
   }

   /**
    * Work rejected
    * @param e The work event
    */
   public void workRejected(WorkEvent e)
   {
      if (trace)
         log.trace("workRejected()");

      teardown();
      pool.removeHandler(this);
   }

   /**
    * Work started
    * @param e The work event
    */
   public void workStarted(WorkEvent e)
   {
      if (trace)
         log.trace("workStarted()");
   }

   /**
    * Create the transaction demarcation strategy
    * @return The strategy
    */
   private TransactionDemarcationStrategy createTransactionDemarcation()
   {
      if (trace)
         log.trace("createTransactionDemarcation()");

      return new DemarcationStrategyFactory().getStrategy();
   }

   /**
    * Demarcation strategy factory
    */
   private class DemarcationStrategyFactory
   {
      /**
       * Get the transaction demarcation strategy
       * @return The strategy
       */
      TransactionDemarcationStrategy getStrategy()
      {
         if (trace)
            log.trace("getStrategy()");

         final JBMActivationSpec spec = pool.getActivation().getActivationSpec();
         final JBMActivation activation = pool.getActivation();

         if (activation.isDeliveryTransacted())
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

         return null;
      }
   }

   /**
    * Transaction demarcation strategy
    */
   private interface TransactionDemarcationStrategy
   {
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
      /**
       * Error
       */
      public void error()
      {
         if (trace)
            log.trace("error()");

         final JBMActivationSpec spec = pool.getActivation().getActivationSpec();

         if (spec.isSessionTransacted())
         {
            if (session != null)
            {
               try
               {
                  /*
                   * Looks strange, but this basically means
                   * 
                   * If the underlying connection was non-XA and the transaction
                   * attribute is REQUIRED we rollback. Also, if the underlying
                   * connection was non-XA and the transaction attribute is
                   * NOT_SUPPORT and the non standard redelivery behavior is
                   * enabled we rollback to force redelivery.
                   * 
                   */
                  if (pool.getActivation().isDeliveryTransacted() || spec.getRedeliverUnspecified())
                  {
                     session.rollback();
                  }
               } catch (JMSException e)
               {
                  log.error("Failed to rollback session transaction", e);
               }
            }
         }
      }

      /**
       * End
       */
      public void end()
      {
         if (trace)
            log.trace("error()");

         final JBMActivationSpec spec = pool.getActivation().getActivationSpec();

         if (spec.isSessionTransacted())
         {
            if (session != null)
            {
               try
               {
                  session.commit();
               } catch (JMSException e)
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
      private Transaction trans = null;
      private TransactionManager tm = pool.getActivation().getTransactionManager();

      public XATransactionDemarcationStrategy() throws Throwable
      {
         final int timeout = pool.getActivation().getActivationSpec().getTransactionTimeout();

         if (timeout > 0)
         {
            if (trace)
               log.trace("Setting transactionTimeout for JMSSessionPool to " + timeout);

            tm.setTransactionTimeout(timeout);
         }

         tm.begin();

         try
         {
            trans = tm.getTransaction();

            if (trace)
               log.trace(this + " using tx=" + trans);

            if (xaSession != null)
            {
               XAResource res = xaSession.getXAResource();

               if (!trans.enlistResource(res))
               {
                  throw new JMSException("could not enlist resource");
               }
               if (trace)
                  log.trace(this + " XAResource '" + res + " enlisted.");
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
               log.trace(this + " using TM to mark TX for rollback tx=" + trans);

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
            if (trans.equals(currentTx) == false)
               throw new IllegalStateException("Wrong tx association: expected " + trans + " was " + currentTx);

            // Marked rollback
            if (trans.getStatus() == Status.STATUS_MARKED_ROLLBACK)
            {
               if (trace)
                  log.trace(this + " rolling back JMS transaction tx=" + trans);

               // Actually roll it back
               tm.rollback();

               // NO XASession? then manually rollback.
               // This is not so good but
               // it's the best we can do if we have no XASession.
               if (xaSession == null && pool.getActivation().isDeliveryTransacted())
               {
                  session.rollback();
               }
            }
            else if (trans.getStatus() == Status.STATUS_ACTIVE)
            {
               // Commit tx
               // This will happen if
               // a) everything goes well
               // b) app. exception was thrown
               if (trace)
                  log.trace(this + " commiting the JMS transaction tx=" + trans);

               tm.commit();

               // NO XASession? then manually commit. This is not so good but
               // it's the best we can do if we have no XASession.
               if (xaSession == null && pool.getActivation().isDeliveryTransacted())
               {
                  session.commit();
               }

            } 
            else
            {
               tm.suspend();

               if (xaSession == null && pool.getActivation().isDeliveryTransacted())
               {
                  session.rollback();
               }
            }
         } catch (Throwable t)
         {
            log.error(this + " failed to commit/rollback", t);
         }
      }
   }
}
