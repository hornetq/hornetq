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

import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.ra.JBMResourceAdapter;
import org.jboss.messaging.ra.Util;
import org.jboss.messaging.core.logging.Logger;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;

import org.jboss.tm.TransactionManagerLocator;

/**
 * The activation.
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMActivation implements ExceptionListener
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMActivation.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();
   
   /** The onMessage method */
   public static final Method ONMESSAGE; 
   
   /** The resource adapter */
   protected JBMResourceAdapter ra;
   
   /** The activation spec */
   protected JBMActivationSpec spec;

   /** The message endpoint factory */
   protected MessageEndpointFactory endpointFactory;
   
   /** Whether delivery is active */
   protected AtomicBoolean deliveryActive = new AtomicBoolean(false);

   /** Whether we are in the failure recovery loop */
   private AtomicBoolean inFailure = new AtomicBoolean(false);

   /** The destination */
   protected Destination destination;

   /** The destination type */
   protected boolean isTopic = false;
   
   /** The connection */
   protected Connection connection;
   
   /** Is the delivery transacted */
   protected boolean isDeliveryTransacted;
   
   /** The TransactionManager */
   protected TransactionManager tm;
   
   static
   {
      try
      {
         ONMESSAGE = MessageListener.class.getMethod("onMessage", new Class[] { Message.class });
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   /**
    * Constructor
    * @param ra The resource adapter
    * @param endpointFactory The endpoint factory
    * @param spec The activation spec
    * @exception ResourceException Thrown if an error occurs
    */
   public JBMActivation(JBMResourceAdapter ra, MessageEndpointFactory endpointFactory, JBMActivationSpec spec) throws ResourceException
   {
      if (trace)
         log.trace("constructor(" + ra + ", " + endpointFactory + ", " + spec + ")");

      this.ra = ra;
      this.endpointFactory = endpointFactory;
      this.spec = spec;
      try
      {
         this.isDeliveryTransacted = endpointFactory.isDeliveryTransacted(ONMESSAGE);
      }
      catch (Exception e)
      {
         throw new ResourceException(e);
      }
   }

   /**
    * Get the activation spec
    * @return The value
    */
   public JBMActivationSpec getActivationSpec()
   {
      if (trace)
         log.trace("getActivationSpec()");

      return spec;
   }

   /**
    * Get the message endpoint factory
    * @return The value
    */
   public MessageEndpointFactory getMessageEndpointFactory()
   {
      if (trace)
         log.trace("getMessageEndpointFactory()");

      return endpointFactory;
   }

   /**
    * Get whether delivery is transacted
    * @return The value
    */
   public boolean isDeliveryTransacted()
   {
      if (trace)
         log.trace("isDeliveryTransacted()");

      return isDeliveryTransacted;
   }

   /**
    * Get the work manager
    * @return The value
    */
   public WorkManager getWorkManager()
   {
      if (trace)
         log.trace("getWorkManager()");

      return ra.getWorkManager();
   }
   
   /**
    * Get the transaction manager
    * @return The value
    */
   public TransactionManager getTransactionManager()
   {
      if (trace)
         log.trace("getTransactionManager()");

      if (tm == null)
         tm = TransactionManagerLocator.locateTransactionManager();

      return tm;
   }

   /**
    * Get the connection
    * @return The value
    */
   public Connection getConnection()
   {
      if (trace)
         log.trace("getConnection()");

      return connection;
   }

   /**
    * Get the destination 
    * @return The value
    */
   public Destination getDestination()
   {
      if (trace)
         log.trace("getDestination()");

      return destination;
   }

   /**
    * Is the destination a topic
    * @return The value
    */
   public boolean isTopic()
   {
      if (trace)
         log.trace("isTopic()");

      return isTopic;
   }
   
   /**
    * Start the activation
    * @throws ResourceException Thrown if an error occurs
    */
   public void start() throws ResourceException
   {
      if (trace)
         log.trace("start()");

      ra.getWorkManager().scheduleWork(new SetupActivation());
      deliveryActive.set(true);
   }

   /**
    * Stop the activation
    */
   public void stop()
   {
      if (trace)
         log.trace("stop()");

      deliveryActive.set(false);
      teardown();
   }

   /**
    * Handles any failure by trying to reconnect
    * @param failure The reason for the failure
    */
   public void handleFailure(Throwable failure)
   {
      log.warn("Failure in jms activation " + spec, failure);
      int reconnectCount = 0;
      
      // Only enter the failure loop once
      if (inFailure.getAndSet(true))
         return;

      try
      {
         while (deliveryActive.get() && reconnectCount < spec.getReconnectAttempts())
         {
            teardown();

            try
            {
               if (spec.getReconnectIntervalMillis() > 0)
                  Thread.sleep(spec.getReconnectIntervalMillis());
            }
            catch (InterruptedException e)
            {
               log.debug("Interrupted trying to reconnect " + spec, e);
               break;
            }

            log.info("Attempting to reconnect " + spec);
            try
            {
               setup();
               log.info("Reconnected with messaging provider.");            
               break;
            }
            catch (Throwable t)
            {
               log.error("Unable to reconnect " + spec, t);
            }
            ++reconnectCount;
         }
      }
      finally
      {
         // Leaving failure recovery loop
         inFailure.set(false);
      }
   }

   /**
    * On exception
    * @param exception The reason for the failure
    */
   public void onException(JMSException exception)
   {
      if (trace)
         log.trace("onException(" + exception + ")");

      handleFailure(exception);
   }


   /**
    * Setup the activation
    * @throws Exception Thrown if an error occurs
    */
   protected void setup() throws Exception
   {
      log.debug("Setting up " + spec);
      
      Context ctx = new InitialContext();
      log.debug("Using context " + ctx.getEnvironment() + " for " + spec);
      try
      {
         setupDestination(ctx);
         setupConnection(ctx);
      }
      finally
      {
         if (ctx != null)
            ctx.close();
      }
      setupSessionPool();
      
      log.debug("Setup complete " + this);
   }
   
   /**
    * Teardown the activation
    */
   protected void teardown()
   {
      log.debug("Tearing down " + spec);

      teardownSessionPool();
      teardownConnection();
      teardownDestination();

      log.debug("Tearing down complete " + this);
   }
   
   /**
    * Setup the destination
    * @param ctx The naming context
    * @throws Exception Thrown if an error occurs
    */
   protected void setupDestination(Context ctx) throws Exception
   {
      if (trace)
         log.trace("setupDestination(" + ctx + ")");

      String destinationName = spec.getDestination();

      String destinationTypeString = spec.getDestinationType();
      if (destinationTypeString != null && !destinationTypeString.trim().equals(""))
      {
         log.debug("Destination type defined as " + destinationTypeString);

         Class<?> destinationType;
         if (Topic.class.getName().equals(destinationTypeString))
         {
            destinationType = Topic.class;
            isTopic = true;
         }
         else
         {
            destinationType = Queue.class;
         }

         log.debug("Retrieving destination " + destinationName + " of type " + destinationType.getName());
         destination = (Destination) Util.lookup(ctx, destinationName, destinationType);
      }
      else
      {
         log.debug("Destination type not defined");
         log.debug("Retrieving destination " + destinationName + " of type " + Destination.class.getName());

         destination = (Destination) Util.lookup(ctx, destinationName, Destination.class);
         if (destination instanceof Topic)
         {
            isTopic = true;
         }
      }

      log.debug("Got destination " + destination + " from " + destinationName);
   }
   
   /**
    * Teardown the destination
    */
   protected void teardownDestination()
   {
      if (trace)
         log.trace("teardownDestination()");

      destination = null;
   }
   
   /**
    * Setup the Connection
    * @param ctx the naming context
    * @throws Exception for any error
    */
   protected void setupConnection(Context ctx) throws Exception
   {
      log.debug("Setup connection " + this);

      String user = spec.getUser();
      String pass = spec.getPassword();
      String clientID = spec.getClientId();

      if (isTopic)
         connection = setupTopicConnection(ctx, user, pass, clientID);
      else
         connection = setupQueueConnection(ctx, user, pass, clientID);
      
      log.debug("Established connection " + this);
   }
   
   /**
    * Setup a queue connection
    * @param ctx The naming context
    * @param user The user
    * @param pass The password
    * @param clientID The client id
    * @return The connection
    * @throws Exception Thrown if an error occurs
    */
   protected QueueConnection setupQueueConnection(Context ctx, String user, String pass, String clientID) throws Exception
   {
      if (trace)
         log.trace("setupQueueConnection(" + ctx + ", " + user + ", ****, " + clientID + ")");

      QueueConnection result = null;

      JBossConnectionFactory jcf = ra.getJBossConnectionFactory();

      if (isDeliveryTransacted)
      {
         if (user != null)
            result = jcf.createXAQueueConnection(user, pass);
         else
            result = jcf.createXAQueueConnection();
      }
      else
      {
         if (user != null)
            result = jcf.createQueueConnection(user, pass);
         else
            result = jcf.createQueueConnection();
      }
      try
      {
         if (clientID != null)
            result.setClientID(clientID);

         result.setExceptionListener(this);

         log.debug("Using queue connection " + result);

         return result;
      }
      catch (Throwable t)
      {
         try
         {
            if (result != null)
               result.close();
         }
         catch (Exception e)
         {
            log.trace("Ignored error closing connection", e);
         }
         if (t instanceof Exception)
            throw (Exception) t;
         throw new RuntimeException("Error configuring connection", t);
      }
   }
   
   /**
    * Setup a topic connection
    * @param ctx The naming context
    * @param user The user
    * @param pass The password
    * @param clientID The client id
    * @return The connection
    * @throws Exception Thrown if an error occurs
    */
   protected TopicConnection setupTopicConnection(Context ctx, String user, String pass, String clientID) throws Exception
   {
      if (trace)
         log.trace("setupTopicConnection(" + ctx + ", " + user + ", ****, " + clientID + ")");

      TopicConnection result = null;

      JBossConnectionFactory jcf = ra.getJBossConnectionFactory();

      if (isDeliveryTransacted)
      {
         if (user != null)
            result = jcf.createXATopicConnection(user, pass);
         else
            result = jcf.createXATopicConnection();
      }
      else
      {
         if (user != null)
            result = jcf.createTopicConnection(user, pass);
         else
            result = jcf.createTopicConnection();
      }
      try
      {
         if (clientID != null)
            result.setClientID(clientID);

         result.setExceptionListener(this);

         log.debug("Using topic connection " + result);

         return result;
      }
      catch (Throwable t)
      {
         try
         {
            if (result != null)
               result.close();
         }
         catch (Exception e)
         {
            log.trace("Ignored error closing connection", e);
         }
         if (t instanceof Exception)
            throw (Exception) t;
         throw new RuntimeException("Error configuring connection", t);
      }
   }
   
   /**
    * Teardown the connection
    */
   protected void teardownConnection()
   {
      if (trace)
         log.trace("teardownConnection()");

      try
      {
         if (connection != null)
         {
            log.debug("Closing the " + connection);
            connection.close();
         }
      }
      catch (Throwable t)
      {
         log.debug("Error closing the connection " + connection, t);
      }
      connection = null;
   }
   
   /**
    * Setup the server session pool
    * @throws Exception for any error
    */
   protected void setupSessionPool() throws Exception
   {
      log.debug("Starting delivery " + connection);
      connection.start();
      log.debug("Started delivery " + connection);
   }
   
   /**
    * Teardown the server session pool
    */
   protected void teardownSessionPool()
   {
      try
      {
         if (connection != null)
         {
            log.debug("Stopping delivery " + connection);
            connection.stop();
         }
      }
      catch (Throwable t)
      {
         log.debug("Error stopping delivery " + connection, t);
      }
   }

   /**
    * Handles the setup
    */
   private class SetupActivation implements Work
   {
      public void run()
      {
         try
         {
            setup();
         }
         catch (Throwable t)
         {
            handleFailure(t);
         }
      }

      public void release()
      {
      }
   }

   /**
    * Get a string representation
    * @return The value
    */
   public String toString()
   {
      StringBuffer buffer = new StringBuffer();
      buffer.append(JBMActivation.class.getName()).append('(');
      buffer.append("spec=").append(spec.getClass().getName());
      buffer.append(" mepf=").append(endpointFactory.getClass().getName());
      buffer.append(" active=").append(deliveryActive.get());
      if (destination != null)
         buffer.append(" destination=").append(destination);
      if (connection != null)
         buffer.append(" connection=").append(connection);
      buffer.append(" transacted=").append(isDeliveryTransacted);
      buffer.append(')');
      return buffer.toString();
   }
}
