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
package org.jboss.jms.client.container;

import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.JBossConnectionMetaData;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.message.MessageIdGeneratorFactory;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.ConnectionListener;

/**
 * Handles operations related to the connection
 *
 * This aspect is PER_INSTANCE.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionAspect implements ConnectionListener
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ConnectionAspect.class);
   private static boolean trace = log.isTraceEnabled();
   
   // Attributes ----------------------------------------------------
   
   
   protected JBossConnectionMetaData connMetaData;
   
   protected ConnectionState state;
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   // Interceptor implementation ------------------------------------
   
   public Object handleGetClientID(Invocation invocation) throws Throwable
   {
      ConnectionState currentState = getConnectionState(invocation);
      
      currentState.setJustCreated(false);
      
      if (currentState.getClientID() == null)
      {
         //Get from the server
         currentState.setClientID((String)invocation.invokeNext());
      }
      return currentState.getClientID();
   }
   
   public Object handleSetClientID(Invocation invocation) throws Throwable
   {
      ConnectionState currentState = getConnectionState(invocation);
      
      if (currentState.getClientID() != null)
      {
         throw new IllegalStateException("Client id has already been set");
      }
      if (!currentState.isJustCreated())
      {
         throw new IllegalStateException("setClientID can only be called directly after the connection is created");
      }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      currentState.setClientID((String)mi.getArguments()[0]);
      
      currentState.setJustCreated(false);
      
      // this gets invoked on the server too
      return invocation.invokeNext();
   }
   
   public Object handleGetExceptionListener(Invocation invocation) throws Throwable
   {
      ConnectionState currentState = getConnectionState(invocation);
      currentState.setJustCreated(false);
      
      return currentState.getExceptionListener();
   }
   
   public Object handleSetExceptionListener(Invocation invocation) throws Throwable
   {
      ConnectionState currentState = getConnectionState(invocation);
      currentState.setJustCreated(false);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      currentState.setExceptionListener((ExceptionListener)mi.getArguments()[0]);
      
      Client client = getConnectionState(invocation).getRemotingConnection().getInvokingClient();
      
      if (client == null)
      {
         throw new java.lang.IllegalStateException("Cannot find remoting client");
      }
      
      if (currentState.getExceptionListener() != null)
      {
         client.addConnectionListener(this);
         currentState.setListenerAdded(true);
      }
      else
      {
         if (currentState.isListenerAdded())
         {
            client.removeConnectionListener(this);
         }
      }
      return null;
   }
   
   public Object handleGetConnectionMetaData(Invocation invocation) throws Throwable
   {
      ConnectionState currentState = getConnectionState(invocation);
      currentState.setJustCreated(false);
      
      if (connMetaData == null)
      {
         ClientConnectionDelegate delegate = (ClientConnectionDelegate)invocation.getTargetObject();
         connMetaData = new JBossConnectionMetaData(((ConnectionState)delegate.getState()).getVersionToUse());
      }
      
      return connMetaData;
   }
   
   public Object handleStart(Invocation invocation) throws Throwable
   {
      ConnectionState currentState = getConnectionState(invocation);
      currentState.setStarted(true);
      return invocation.invokeNext();
   }
   
   public Object handleStop(Invocation invocation) throws Throwable
   {
      ConnectionState currentState = getConnectionState(invocation);
      currentState.setStarted(false);
      return invocation.invokeNext();
   }
   
   public Object handleCreateSessionDelegate(Invocation invocation) throws Throwable
   {
      ConnectionState currentState = getConnectionState(invocation);
      currentState.setJustCreated(false);
      return invocation.invokeNext();
   }
   
   public Object handleClose(Invocation invocation) throws Throwable
   {
      Object ret = invocation.invokeNext();
      
      // Remove any exception listener
      ConnectionState currentState = getConnectionState(invocation);
      
      Client client = getConnectionState(invocation).getRemotingConnection().getInvokingClient();
      
      if (currentState.isListenerAdded())
      {
         client.removeConnectionListener(this);
      }
            
      ConnectionState state = getConnectionState(invocation);
      
      // Finished with the connection - we need to shutdown callback server
      state.getRemotingConnection().stop();
       
      // Remove reference to message id generator
      MessageIdGeneratorFactory.instance.checkInGenerator(state.getServerID());
      
      return ret;
   }
   
   
   // ConnectionListener implementation -----------------------------------------------------------
   
   public void handleConnectionException(Throwable t, Client c)
   {
      log.error("Caught exception from connection", t);
      
      if (state.getExceptionListener()!= null)
      {
         JMSException j = null;
         if (t instanceof Error)
         {
            final String msg = "Caught Error on underlying connection";
            log.error(msg, t);
            j = new JMSException(msg + ": " + t.getMessage());
         }
         else if (t instanceof Exception)
         {
            Exception e =(Exception)t;
            j = new JMSException("Throwable received from underlying connection");
            j.setLinkedException(e);
         }
         else
         {
            //Some other Throwable subclass
            final String msg = "Caught Throwable on underlying connection";
            log.error(msg, t);
            j = new JMSException(msg + ": " + t.getMessage());
         }
         synchronized (state.getExceptionListener())
         {
            state.getExceptionListener().onException(j);
         }
      }
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private ConnectionState getConnectionState(Invocation invocation)
   {
      if (state==null)
      {
         ClientConnectionDelegate currentDelegate =
            ((ClientConnectionDelegate)invocation.getTargetObject());
         
         state = (ConnectionState)currentDelegate.getState();
      }
      return state;
   }
   
   
   // Inner classes -------------------------------------------------
}
