/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.ra;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.XAConnection;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicConnection;
import javax.jms.XATopicSession;
import javax.resource.NotSupportedException;
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
import javax.transaction.xa.XAResource;

import org.jboss.messaging.core.logging.Logger;

/**
 * The managed connection
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision:  $
 */
public class JBMManagedConnection implements ManagedConnection, ExceptionListener
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMManagedConnection.class);
   
   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();
   
   /** The managed connection factory */
   private JBMManagedConnectionFactory mcf;

   /** The connection request information */
   private JBMConnectionRequestInfo cri;

   /** The user name */
   private String userName;

   /** The password */
   private String password;

   /** Has the connection been destroyed */
   private AtomicBoolean isDestroyed = new AtomicBoolean(false);

   /** Event listeners */
   private List<ConnectionEventListener> eventListeners;
   
   /** Handles */
   private Set<JBMSession> handles;

   /** Lock */
   private ReentrantLock lock = new ReentrantLock();
   
   // Physical JMS connection stuff
   private Connection connection;
   private XAConnection xaConnection;
   private Session session;
   private TopicSession topicSession;
   private QueueSession queueSession;
   private XASession xaSession;
   private XATopicSession xaTopicSession;
   private XAQueueSession xaQueueSession;
   private XAResource xaResource;

   /**
    * Constructor
    * @param mcf The managed connection factory
    * @param cri The connection request information
    * @param userName The user name
    * @param password The password
    */
   public JBMManagedConnection(JBMManagedConnectionFactory mcf, 
                               JBMConnectionRequestInfo cri,
                               String userName, 
                               String password)
      throws ResourceException
   {
      if (trace)
         log.trace("constructor(" + mcf + ", " + cri + ", " + userName + ", ****)");

      this.mcf = mcf;
      this.cri = cri;
      this.userName = userName;
      this.password = password;
      this.eventListeners = Collections.synchronizedList(new ArrayList<ConnectionEventListener>());
      this.handles = Collections.synchronizedSet(new HashSet<JBMSession>());

      this.connection = null;
      this.xaConnection = null;
      this.session = null;
      this.topicSession = null;
      this.queueSession = null;
      this.xaSession = null;
      this.xaTopicSession = null;
      this.xaQueueSession = null;
      this.xaResource = null;

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
   public synchronized Object getConnection(Subject subject, ConnectionRequestInfo cxRequestInfo)
      throws ResourceException
   {
      if (trace)
         log.trace("getConnection(" + subject + ", " + cxRequestInfo + ")");

      // Check user first
      JBMCredential credential = JBMCredential.getCredential(mcf, subject, cxRequestInfo);

      // Null users are allowed!
      if (userName != null && !userName.equals(credential.getUserName()))
         throw new SecurityException("Password credentials not the same, reauthentication not allowed");

      if (userName == null && credential.getUserName() != null)
         throw new SecurityException("Password credentials not the same, reauthentication not allowed");

      if (isDestroyed.get())
         throw new IllegalStateException("The managed connection is already destroyed");

      JBMSession session = new JBMSession(this, (JBMConnectionRequestInfo)cxRequestInfo);
      handles.add(session);
      return session;
   }

   /**
    * Destroy all handles.
    * @exception ResourceException Failed to close one or more handles.
    */
   private void destroyHandles() throws ResourceException
   {
      if (trace)
         log.trace("destroyHandles()");

      try
      {
         if (xaConnection != null)
            xaConnection.stop();  

         if (connection != null)
            connection.stop();  
      }
      catch (Throwable t)
      {
         log.trace("Ignored error stopping connection", t);
      }
      
      for (JBMSession session: handles)
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
      if (trace)
         log.trace("destroy()");

      if (isDestroyed.get() || (xaConnection == null && connection == null))
         return;

      isDestroyed.set(true);

      try
      {
         if (xaConnection != null)
         {
            xaConnection.setExceptionListener(null);
         }
         else
         {
            connection.setExceptionListener(null);
         }
      }
      catch (JMSException e)
      {
         log.debug("Error unsetting the exception listener " + this, e);
      }
      
      destroyHandles();
      
      try
      {
         try
         {
            if (topicSession != null)
               topicSession.close();

            if (xaTopicSession != null)
               xaTopicSession.close();

            if (queueSession != null)
               queueSession.close();

            if (xaQueueSession != null)
               xaQueueSession.close();

            if (session != null)
               session.close();

            if (xaSession != null)
               xaSession.close();
         }
         catch (JMSException e)
         {
            log.debug("Error closing session " + this, e);
         }

         if (connection != null)
            connection.close();

         if (xaConnection != null)
            xaConnection.close();
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
      if (trace)
         log.trace("cleanup()");

      if (isDestroyed.get())
         throw new IllegalStateException("ManagedConnection already destroyed");

      destroyHandles();

      // I'm recreating the lock object when we return to the pool
      // because it looks too nasty to expect the connection handle
      // to unlock properly in certain race conditions
      // where the dissociation of the managed connection is "random".
      lock = new ReentrantLock();
   }

   /**
    * Move a handler from one mc to this one.
    * @param obj An object of type JBMSession.
    * @throws ResourceException Failed to associate connection.
    * @throws IllegalStateException ManagedConnection in an illegal state.
    */
   public void associateConnection(final Object obj) throws ResourceException
   {
      if (trace)
         log.trace("associateConnection(" + obj + ")");

      if (!isDestroyed.get() && obj instanceof JBMSession)
      {
         JBMSession h = (JBMSession)obj;
         h.setManagedConnection(this);
         handles.add(h);
      }
      else
         throw new IllegalStateException("ManagedConnection in an illegal state");
   }

   /**
    * Aqquire a lock on the managed connection
    */
   protected void lock()
   {
      if (trace)
         log.trace("lock()");

      lock.lock();
   }

   /**
    * Aqquire a lock on the managed connection within the specified period
    * @exception JMSException Thrown if an error occurs
    */
   protected void tryLock() throws JMSException
   {
      if (trace)
         log.trace("tryLock()");

      Integer tryLock = mcf.getUseTryLock();
      if (tryLock == null || tryLock.intValue() <= 0)
      {
         lock();
         return;
      }
      try
      {
         if (lock.tryLock(tryLock.intValue(), TimeUnit.SECONDS) == false)
            throw new ResourceAllocationException("Unable to obtain lock in " + tryLock + " seconds: " + this);
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
      if (trace)
         log.trace("unlock()");

      lock.unlock();
   }

   /**
    * Add a connection event listener.
    * @param l The connection event listener to be added.
    */
   public void addConnectionEventListener(ConnectionEventListener l)
   {
      if (trace)
         log.trace("addConnectionEventListener(" + l + ")");
      
      eventListeners.add(l);
   }

   /**
    * Remove a connection event listener.
    * @param l The connection event listener to be removed.
    */
   public void removeConnectionEventListener(ConnectionEventListener l)
   {
      if (trace)
         log.trace("removeConnectionEventListener(" + l + ")");

      eventListeners.remove(l);
   }

   /**
    * Get the XAResource for the connection.
    * @return The XAResource for the connection.
    * @exception ResourceException XA transaction not supported
    */
   public XAResource getXAResource() throws ResourceException
   {
      if (trace)
         log.trace("getXAResource()");
      
      if (xaConnection == null)
         throw new NotSupportedException("Non XA transaction not supported");

      //
      // Spec says a mc must allways return the same XA resource,
      // so we cache it.
      //
      if (xaResource == null)
      {
         if (xaTopicSession != null)
            xaResource = xaTopicSession.getXAResource();
         else if (xaQueueSession != null)
            xaResource = xaQueueSession.getXAResource();
         else
            xaResource = xaSession.getXAResource();
      }

      if (trace)
         log.trace("XAResource=" + xaResource);

      xaResource = new JBMXAResource(this, xaResource);
      return xaResource;
   }

   /**
    * Get the location transaction for the connection.
    * @return The local transaction for the connection.
    * @exception ResourceException Thrown if operation fails.
    */
   public LocalTransaction getLocalTransaction() throws ResourceException
   {
      if (trace)
         log.trace("getLocalTransaction()");
      
      LocalTransaction tx = new JBMLocalTransaction(this);

      if (trace)
         log.trace("LocalTransaction=" + tx);

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
      if (trace)
         log.trace("getMetaData()");
      
      if (isDestroyed.get())
         throw new IllegalStateException("The managed connection is already destroyed");

      return new JBMMetaData(this);
   }

   /**
    * Set the log writer -- NOT SUPPORTED
    * @param out The log writer
    * @exception ResourceException If operation fails
    */
   public void setLogWriter(PrintWriter out) throws ResourceException
   {
      if (trace)
         log.trace("setLogWriter(" + out + ")");
   }

   /**
    * Get the log writer -- NOT SUPPORTED
    * @return Always null
    * @exception ResourceException If operation fails
    */
   public PrintWriter getLogWriter() throws ResourceException
   {
      if (trace)
         log.trace("getLogWriter()");

      return null;
   }

   /**
    * Notifies user of a JMS exception.
    * @param exception The JMS exception
    */
   public void onException(JMSException exception)
   {
      if (trace)
         log.trace("onException(" + exception + ")");

      if (isDestroyed.get())
      {
         if (trace)
            log.trace("Ignoring error on already destroyed connection " + this, exception);
         return;
      }

      log.warn("Handling JMS exception failure: " + this, exception);

      try
      {
         if (xaConnection != null)
         {
            xaConnection.setExceptionListener(null);
         }
         else
         {
            connection.setExceptionListener(null);
         }
      }
      catch (JMSException e)
      {
         log.debug("Unable to unset exception listener", e);
      }
      
      ConnectionEvent event = new ConnectionEvent(this, ConnectionEvent.CONNECTION_ERROR_OCCURRED, exception);
      sendEvent(event);
   }
   
   /**
    * Is managed connection running in XA mode
    * @return True if XA; otherwise false
    */
   protected boolean isXA()
   {
      if (trace)
         log.trace("isXA()");

      return xaConnection != null;
   }

   /**
    * Get the XA session for this connection.
    * @return The XA session
    */
   protected XASession getXASession()
   {
      if (trace)
         log.trace("getXASession()");

      if (isXA())
      {
         if (xaTopicSession != null)
            return xaTopicSession;
         else if (xaQueueSession != null)
            return xaQueueSession;
         else
            return xaSession;
      }
      else
         return null;
   }

   /**
    * Get the session for this connection.
    * @return The session
    */
   protected Session getSession()
   {
      if (trace)
         log.trace("getSession()");

      if (topicSession != null)
         return topicSession;
      else if (queueSession != null)
         return queueSession;
      else
         return session;
   }

   /**
    * Send an event.
    * @param event The event to send.
    */
   protected void sendEvent(ConnectionEvent event)
   {
      if (trace)
         log.trace("sendEvent(" + event + ")");

      int type = event.getId();

      // convert to an array to avoid concurrent modification exceptions
      ConnectionEventListener[] list =
         (ConnectionEventListener[])eventListeners.toArray(new ConnectionEventListener[eventListeners.size()]);

      for (ConnectionEventListener l : list)
      {
         switch (type) {
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
   protected void removeHandle(JBMSession handle)
   {
      if (trace)
         log.trace("removeHandle(" + handle + ")");

      handles.remove(handle);
   }

   /**
    * Get the request info for this connection.
    * @return The connection request info for this connection.
    */
   protected JBMConnectionRequestInfo getCRI()
   {
      if (trace)
         log.trace("getCRI()");

      return cri;
   }

   /**
    * Get the connection factory for this connection.
    * @return The connection factory for this connection.
    */
   protected JBMManagedConnectionFactory getManagedConnectionFactory()
   {
      if (trace)
         log.trace("getManagedConnectionFactory()");

      return mcf;
   }

   /**
    * Start the connection
    * @exception JMSException Thrown if the connection cant be started
    */
   void start() throws JMSException
   {
      if (trace)
         log.trace("start()");

      if (connection != null)
         connection.start();

      if (xaConnection != null)
         xaConnection.start();
   }

   /**
    * Stop the connection
    * @exception JMSException Thrown if the connection cant be stopped
    */
   void stop() throws JMSException
   {
      if (trace)
         log.trace("stop()");

      if (xaConnection != null)
         xaConnection.stop();

      if (connection != null)
         connection.stop();
   }
   
   /**
    * Get the user name
    * @return The user name
    */
   protected String getUserName()
   {
      if (trace)
         log.trace("getUserName()");

      return userName;
   }

   /**
    * Setup the connection.
    * @exception ResourceException Thrown if a connection couldnt be created
    */
   private void setup() throws ResourceException
   {
      if (trace)
         log.trace("setup()");

      try
      {
         boolean transacted = cri.isTransacted();
         int acknowledgeMode = Session.AUTO_ACKNOWLEDGE;

         if (cri.getType() == JBMConnectionFactory.TOPIC_CONNECTION)
         {
            if (cri.isUseXA())
            {
               if (userName != null && password != null)
               {
                  xaConnection = mcf.getJBossConnectionFactory().createXATopicConnection(userName, password);
               }
               else
               {
                  xaConnection = mcf.getJBossConnectionFactory().createXATopicConnection();
               }

               xaConnection.setExceptionListener(this);

               xaTopicSession = ((XATopicConnection)xaConnection).createXATopicSession();
               topicSession = xaTopicSession.getTopicSession();
            }
            else
            {
               if (userName != null && password != null)
               {
                  connection = mcf.getJBossConnectionFactory().createTopicConnection(userName, password);
               }
               else
               {
                  connection = mcf.getJBossConnectionFactory().createTopicConnection();
               }

               connection.setExceptionListener(this);

               topicSession = ((TopicConnection)connection).createTopicSession(transacted, acknowledgeMode);
            }
         }
         else if (cri.getType() == JBMConnectionFactory.QUEUE_CONNECTION)
         {
            if (cri.isUseXA())
            {
               if (userName != null && password != null)
               {
                  xaConnection = mcf.getJBossConnectionFactory().createXAQueueConnection(userName, password);
               }
               else
               {
                  xaConnection = mcf.getJBossConnectionFactory().createXAQueueConnection();
               }

               xaConnection.setExceptionListener(this);

               xaQueueSession = ((XAQueueConnection)xaConnection).createXAQueueSession();
               queueSession = xaQueueSession.getQueueSession();
            }
            else
            {
               if (userName != null && password != null)
               {
                  connection = mcf.getJBossConnectionFactory().createQueueConnection(userName, password);
               }
               else
               {
                  connection = mcf.getJBossConnectionFactory().createQueueConnection();
               }

               connection.setExceptionListener(this);

               queueSession = ((QueueConnection)connection).createQueueSession(transacted, acknowledgeMode);
            }
         }
         else
         {
            if (cri.isUseXA())
            {
               if (userName != null && password != null)
               {
                  xaConnection = mcf.getJBossConnectionFactory().createXAConnection(userName, password);
               }
               else
               {
                  xaConnection = mcf.getJBossConnectionFactory().createXAConnection();
               }

               xaConnection.setExceptionListener(this);

               xaSession = xaConnection.createXASession();
               session = xaSession.getSession();
            }
            else
            {
               if (userName != null && password != null)
               {
                  connection = mcf.getJBossConnectionFactory().createConnection(userName, password);
               }
               else
               {
                  connection = mcf.getJBossConnectionFactory().createConnection();
               }

               connection.setExceptionListener(this);

               session = connection.createSession(transacted, acknowledgeMode);
            }
         }
      }
      catch (JMSException je)
      {
         throw new ResourceException(je.getMessage(), je);
      }
   }
}
