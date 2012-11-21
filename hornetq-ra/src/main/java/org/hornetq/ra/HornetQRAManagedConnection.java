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

package org.hornetq.ra;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;
import javax.jms.XASession;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.IllegalStateException;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionMetaData;
import javax.resource.spi.SecurityException;
import javax.security.auth.Subject;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.hornetq.jms.client.HornetQConnection;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQXAConnection;

/**
 * The managed connection
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 */
public class HornetQRAManagedConnection implements ManagedConnection, ExceptionListener
{
   /** Trace enabled */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /** The managed connection factory */
   private final HornetQRAManagedConnectionFactory mcf;

   /** The connection request information */
   private final HornetQRAConnectionRequestInfo cri;

   /** The resource adapter */
   private final HornetQResourceAdapter ra;

   /** The user name */
   private final String userName;

   /** The password */
   private final String password;

   /** Has the connection been destroyed */
   private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

   /** Event listeners */
   private final List<ConnectionEventListener> eventListeners;

   /** Handles */
   private final Set<HornetQRASession> handles;

   /** Lock */
   private ReentrantLock lock = new ReentrantLock();

   // Physical connection stuff
   private HornetQConnectionFactory connectionFactory;

   private HornetQXAConnection connection;

   // The ManagedConnection will paly with a XA and a NonXASession to couple with
   // cases where a commit is called on a non-XAed (or non-enlisted) case.
   private Session nonXAsession;

   private XASession xaSession;

   private XAResource xaResource;

   private final TransactionManager tm;

   private boolean inManagedTx;

   /**
    * Constructor
    * @param mcf The managed connection factory
    * @param cri The connection request information
    * @param userName The user name
    * @param password The password
    */
   public HornetQRAManagedConnection(final HornetQRAManagedConnectionFactory mcf,
                                     final HornetQRAConnectionRequestInfo cri,
                                     final HornetQResourceAdapter ra,
                                     final String userName,
                                     final String password) throws ResourceException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor(" + mcf + ", " + cri + ", " + userName + ", ****)");
      }

      this.mcf = mcf;
      this.cri = cri;
      this.tm = ra.getTM();
      this.ra = ra;
      this.userName = userName;
      this.password = password;
      eventListeners = Collections.synchronizedList(new ArrayList<ConnectionEventListener>());
      handles = Collections.synchronizedSet(new HashSet<HornetQRASession>());

      connection = null;
      nonXAsession = null;
      xaSession = null;
      xaResource = null;

      try
      {
         setup();
      }
      catch (Throwable t)
      {
         try
         {
            destroy();
         }
         catch (Throwable ignored)
         {
         }
         throw new ResourceException("Error during setup", t);
      }
   }

   /**
    * Get a connection
    * @param subject The security subject
    * @param cxRequestInfo The request info
    * @return The connection
    * @exception ResourceException Thrown if an error occurs
    */
   public synchronized Object getConnection(final Subject subject, final ConnectionRequestInfo cxRequestInfo) throws ResourceException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("getConnection(" + subject + ", " + cxRequestInfo + ")");
      }

      // Check user first
      HornetQRACredential credential = HornetQRACredential.getCredential(mcf, subject, cxRequestInfo);

      // Null users are allowed!
      if (userName != null && !userName.equals(credential.getUserName()))
      {
         throw new SecurityException("Password credentials not the same, reauthentication not allowed");
      }

      if (userName == null && credential.getUserName() != null)
      {
         throw new SecurityException("Password credentials not the same, reauthentication not allowed");
      }

      if (isDestroyed.get())
      {
         throw new IllegalStateException("The managed connection is already destroyed");
      }

      HornetQRASession session = new HornetQRASession(this, (HornetQRAConnectionRequestInfo)cxRequestInfo);
      handles.add(session);
      return session;
   }

   /**
    * Destroy all handles.
    * @exception ResourceException Failed to close one or more handles.
    */
   private void destroyHandles() throws ResourceException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("destroyHandles()");
      }

      try
      {
         if (connection != null)
         {
            connection.stop();
         }
      }
      catch (Throwable t)
      {
         HornetQRALogger.LOGGER.trace("Ignored error stopping connection", t);
      }

      for (HornetQRASession session : handles)
      {
         session.destroy();
      }

      handles.clear();
   }

   /**
    * Destroy the physical connection.
    * @exception ResourceException Could not property close the session and connection.
    */
   public void destroy() throws ResourceException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("destroy()");
      }

      if (isDestroyed.get() ||  connection == null)
      {
         return;
      }

      isDestroyed.set(true);

      try
      {
         connection.setExceptionListener(null);
      }
      catch (JMSException e)
      {
         HornetQRALogger.LOGGER.debug("Error unsetting the exception listener " + this, e);
      }

      destroyHandles();

      try
      {
         try
         {
            if (nonXAsession != null)
            {
               nonXAsession.close();
            }

            if (xaSession != null)
            {
               xaSession.close();
            }
         }
         catch (JMSException e)
         {
            HornetQRALogger.LOGGER.debug("Error closing session " + this, e);
         }

         if (connection != null)
         {
            connection.close();
         }

         // we must close the HornetQConnectionFactory because it contains a ServerLocator
         if (connectionFactory != null)
         {
            connectionFactory.close();
         }
      }
      catch (Throwable e)
      {
         throw new ResourceException("Could not properly close the session and connection", e);
      }
   }

   /**
    * Cleanup
    * @exception ResourceException Thrown if an error occurs
    */
   public void cleanup() throws ResourceException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("cleanup()");
      }

      if (isDestroyed.get())
      {
         throw new IllegalStateException("ManagedConnection already destroyed");
      }

      destroyHandles();

      inManagedTx = false;

      inManagedTx = false;

      // I'm recreating the lock object when we return to the pool
      // because it looks too nasty to expect the connection handle
      // to unlock properly in certain race conditions
      // where the dissociation of the managed connection is "random".
      lock = new ReentrantLock();
   }

   /**
    * Move a handler from one mc to this one.
    * @param obj An object of type HornetQSession.
    * @throws ResourceException Failed to associate connection.
    * @throws IllegalStateException ManagedConnection in an illegal state.
    */
   public void associateConnection(final Object obj) throws ResourceException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("associateConnection(" + obj + ")");
      }

      if (!isDestroyed.get() && obj instanceof HornetQRASession)
      {
         HornetQRASession h = (HornetQRASession)obj;
         h.setManagedConnection(this);
         handles.add(h);
      }
      else
      {
         throw new IllegalStateException("ManagedConnection in an illegal state");
      }
   }

   public void checkTransactionActive() throws JMSException
   {
      // don't bother looking at the transaction if there's an active XID
      if (!inManagedTx && tm != null)
      {
         try
         {
            Transaction tx = tm.getTransaction();
            if (tx != null)
            {
               int status = tx.getStatus();
               // Only allow states that will actually succeed
               if (status != Status.STATUS_ACTIVE && status != Status.STATUS_PREPARING &&
                   status != Status.STATUS_PREPARED &&
                   status != Status.STATUS_COMMITTING)
               {
                  throw new javax.jms.IllegalStateException("Transaction " + tx + " not active");
               }
            }
         }
         catch (SystemException e)
         {
            JMSException jmsE = new javax.jms.IllegalStateException("Unexpected exception on the Transaction ManagerTransaction");
            jmsE.initCause(e);
            throw jmsE;
         }
      }
   }


   /**
    * Aqquire a lock on the managed connection
    */
   protected void lock()
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("lock()");
      }

      lock.lock();
   }

   /**
    * Aqquire a lock on the managed connection within the specified period
    * @exception JMSException Thrown if an error occurs
    */
   protected void tryLock() throws JMSException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("tryLock()");
      }

      Integer tryLock = mcf.getUseTryLock();
      if (tryLock == null || tryLock.intValue() <= 0)
      {
         lock();
         return;
      }
      try
      {
         if (lock.tryLock(tryLock.intValue(), TimeUnit.SECONDS) == false)
         {
            throw new ResourceAllocationException("Unable to obtain lock in " + tryLock + " seconds: " + this);
         }
      }
      catch (InterruptedException e)
      {
         throw new ResourceAllocationException("Interrupted attempting lock: " + this);
      }
   }

   /**
    * Unlock the managed connection
    */
   protected void unlock()
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("unlock()");
      }

      lock.unlock();
   }

   /**
    * Add a connection event listener.
    * @param l The connection event listener to be added.
    */
   public void addConnectionEventListener(final ConnectionEventListener l)
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("addConnectionEventListener(" + l + ")");
      }

      eventListeners.add(l);
   }

   /**
    * Remove a connection event listener.
    * @param l The connection event listener to be removed.
    */
   public void removeConnectionEventListener(final ConnectionEventListener l)
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("removeConnectionEventListener(" + l + ")");
      }

      eventListeners.remove(l);
   }

   /**
    * Get the XAResource for the connection.
    * @return The XAResource for the connection.
    * @exception ResourceException XA transaction not supported
    */
   public XAResource getXAResource() throws ResourceException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("getXAResource()");
      }

      //
      // Spec says a mc must allways return the same XA resource,
      // so we cache it.
      //
      if (xaResource == null)
      {
            xaResource = new HornetQRAXAResource(this, xaSession.getXAResource());
      }

      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("XAResource=" + xaResource);
      }

      return xaResource;
   }

   /**
    * Get the location transaction for the connection.
    * @return The local transaction for the connection.
    * @exception ResourceException Thrown if operation fails.
    */
   public LocalTransaction getLocalTransaction() throws ResourceException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("getLocalTransaction()");
      }

      LocalTransaction tx = new HornetQRALocalTransaction(this);

      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("LocalTransaction=" + tx);
      }

      return tx;
   }

   /**
    * Get the meta data for the connection.
    * @return The meta data for the connection.
    * @exception ResourceException Thrown if the operation fails.
    * @exception IllegalStateException Thrown if the managed connection already is destroyed.
    */
   public ManagedConnectionMetaData getMetaData() throws ResourceException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("getMetaData()");
      }

      if (isDestroyed.get())
      {
         throw new IllegalStateException("The managed connection is already destroyed");
      }

      return new HornetQRAMetaData(this);
   }

   /**
    * Set the log writer -- NOT SUPPORTED
    * @param out The log writer
    * @exception ResourceException If operation fails
    */
   public void setLogWriter(final PrintWriter out) throws ResourceException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("setLogWriter(" + out + ")");
      }
   }

   /**
    * Get the log writer -- NOT SUPPORTED
    * @return Always null
    * @exception ResourceException If operation fails
    */
   public PrintWriter getLogWriter() throws ResourceException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("getLogWriter()");
      }

      return null;
   }

   /**
    * Notifies user of a JMS exception.
    * @param exception The JMS exception
    */
   public void onException(final JMSException exception)
   {
      if(HornetQConnection.EXCEPTION_FAILOVER.equals(exception.getErrorCode()))
      {
         return;
      }
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("onException(" + exception + ")");
      }

      if (isDestroyed.get())
      {
         if (HornetQRAManagedConnection.trace)
         {
            HornetQRALogger.LOGGER.trace("Ignoring error on already destroyed connection " + this, exception);
         }
         return;
      }

      HornetQRALogger.LOGGER.handlingJMSFailure(exception);

      try
      {
         connection.setExceptionListener(null);
      }
      catch (JMSException e)
      {
         HornetQRALogger.LOGGER.debug("Unable to unset exception listener", e);
      }

      ConnectionEvent event = new ConnectionEvent(this, ConnectionEvent.CONNECTION_ERROR_OCCURRED, exception);
      sendEvent(event);
   }

   /**
    * Get the session for this connection.
    * @return The session
    * @throws JMSException
    */
   protected Session getSession() throws JMSException
   {
      if (xaResource != null && inManagedTx)
      {
         if (HornetQRAManagedConnection.trace)
         {
            HornetQRALogger.LOGGER.trace("getSession() -> XA session " + xaSession.getSession());
         }

         return xaSession.getSession();
      }
      else
      {
         if (HornetQRAManagedConnection.trace)
         {
            HornetQRALogger.LOGGER.trace("getSession() -> non XA session " + nonXAsession);
         }

         return nonXAsession;
      }
   }

   /**
    * Send an event.
    * @param event The event to send.
    */
   protected void sendEvent(final ConnectionEvent event)
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("sendEvent(" + event + ")");
      }

      int type = event.getId();

      // convert to an array to avoid concurrent modification exceptions
      ConnectionEventListener[] list = eventListeners.toArray(new ConnectionEventListener[eventListeners.size()]);

      for (ConnectionEventListener l : list)
      {
         switch (type)
         {
            case ConnectionEvent.CONNECTION_CLOSED:
               l.connectionClosed(event);
               break;

            case ConnectionEvent.LOCAL_TRANSACTION_STARTED:
               l.localTransactionStarted(event);
               break;

            case ConnectionEvent.LOCAL_TRANSACTION_COMMITTED:
               l.localTransactionCommitted(event);
               break;

            case ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK:
               l.localTransactionRolledback(event);
               break;

            case ConnectionEvent.CONNECTION_ERROR_OCCURRED:
               l.connectionErrorOccurred(event);
               break;

            default:
               throw new IllegalArgumentException("Illegal eventType: " + type);
         }
      }
   }

   /**
    * Remove a handle from the handle map.
    * @param handle The handle to remove.
    */
   protected void removeHandle(final HornetQRASession handle)
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("removeHandle(" + handle + ")");
      }

      handles.remove(handle);
   }

   /**
    * Get the request info for this connection.
    * @return The connection request info for this connection.
    */
   protected HornetQRAConnectionRequestInfo getCRI()
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("getCRI()");
      }

      return cri;
   }

   /**
    * Get the connection factory for this connection.
    * @return The connection factory for this connection.
    */
   protected HornetQRAManagedConnectionFactory getManagedConnectionFactory()
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("getManagedConnectionFactory()");
      }

      return mcf;
   }

   /**
    * Start the connection
    * @exception JMSException Thrown if the connection cant be started
    */
   void start() throws JMSException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("start()");
      }

      if (connection != null)
      {
         connection.start();
      }
   }

   /**
    * Stop the connection
    * @exception JMSException Thrown if the connection cant be stopped
    */
   void stop() throws JMSException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("stop()");
      }

      if (connection != null)
      {
         connection.stop();
      }
   }

   /**
    * Get the user name
    * @return The user name
    */
   protected String getUserName()
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("getUserName()");
      }

      return userName;
   }

   /**
    * Setup the connection.
    * @exception ResourceException Thrown if a connection couldnt be created
    */
   private void setup() throws ResourceException
   {
      if (HornetQRAManagedConnection.trace)
      {
         HornetQRALogger.LOGGER.trace("setup()");
      }

      try
      {
         connectionFactory = ra.createHornetQConnectionFactory(mcf.getProperties());
         boolean transacted = cri.isTransacted();
         int acknowledgeMode =  Session.AUTO_ACKNOWLEDGE;
         if (cri.getType() == HornetQRAConnectionFactory.TOPIC_CONNECTION)
         {
            if (userName != null && password != null)
            {
               connection = (HornetQXAConnection)connectionFactory.createXATopicConnection(userName, password);
            }
            else
            {
               connection = (HornetQXAConnection)connectionFactory.createXATopicConnection();
            }

            connection.setExceptionListener(this);

            xaSession = connection.createXATopicSession();
            nonXAsession = connection.createNonXATopicSession(transacted, acknowledgeMode);

         }
         else if (cri.getType() == HornetQRAConnectionFactory.QUEUE_CONNECTION)
         {
            if (userName != null && password != null)
            {
               connection = (HornetQXAConnection)connectionFactory.createXAQueueConnection(userName, password);
            }
            else
            {
               connection = (HornetQXAConnection)connectionFactory.createXAQueueConnection();
            }

            connection.setExceptionListener(this);

            xaSession = connection.createXAQueueSession();
            nonXAsession = connection.createNonXAQueueSession(transacted, acknowledgeMode);

         }
         else
         {
            if (userName != null && password != null)
            {
               connection = (HornetQXAConnection)connectionFactory.createXAConnection(userName, password);
            }
            else
            {
               connection = (HornetQXAConnection)connectionFactory.createXAConnection();
            }

            connection.setExceptionListener(this);

            xaSession = connection.createXASession();
            nonXAsession = connection.createNonXASession(transacted, acknowledgeMode);
         }

      }
      catch (JMSException je)
      {
         throw new ResourceException(je.getMessage(), je);
      }
   }

   protected void setInManagedTx(boolean inManagedTx)
   {
      this.inManagedTx = inManagedTx;
   }

}
