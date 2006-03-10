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

import javax.jms.ConnectionMetaData;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.JBossConnectionMetaData;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.ConnectionState;
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
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionAspect implements ConnectionListener
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ConnectionAspect.class);

   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
   
   protected String clientID;
   
   protected ExceptionListener exceptionListener;
   
   protected ConnectionMetaData connMetaData;
   
   boolean justCreated = true;
   
   protected boolean listenerAdded;
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public Object handleGetClientID(Invocation invocation) throws Throwable
   {
      justCreated = false;
      
      if (clientID == null)          
      {
         //Get from the server
         clientID = (String)invocation.invokeNext();
      }
      return clientID;
   }
   
   public Object handleSetClientID(Invocation invocation) throws Throwable
   {
      if (clientID != null)
      {
         throw new IllegalStateException("Client id has already been set");
      }
      if (!justCreated)
      {
         throw new IllegalStateException("setClientID can only be called directly after the connection is created");
      }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      clientID = (String)mi.getArguments()[0];
      
      justCreated = false;
      
      //This gets invoked on the server too
      return invocation.invokeNext();
   }
   
   public Object handleGetExceptionListener(Invocation invocation) throws Throwable
   {
      justCreated = false;
      
      return exceptionListener;
   }
   
   public Object handleSetExceptionListener(Invocation invocation) throws Throwable
   {
      justCreated = false;
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      exceptionListener = (ExceptionListener)mi.getArguments()[0];            
      
      Client client = getState(invocation).getRemotingConnection().getInvokingClient();
      
      if (client == null)
      {
         throw new java.lang.IllegalStateException("Cannot find remoting client");
      }
      
      if (exceptionListener != null)
      {
         client.addConnectionListener(this);
         listenerAdded = true;
      }
      else
      {            
         if (listenerAdded)
         {
            client.removeConnectionListener(this);
         }
      }
      return null;
   }
   
   public Object handleGetConnectionMetaData(Invocation invocation) throws Throwable
   {
      justCreated = false;

      if (connMetaData == null)
      {
         ClientConnectionDelegate delegate = (ClientConnectionDelegate)invocation.getTargetObject();
         connMetaData = new JBossConnectionMetaData(((ConnectionState)delegate.getState()).getVersionToUse());
      }

      return connMetaData;
   }
   
   public Object handleCreateSessionDelegate(Invocation invocation) throws Throwable
   {
      justCreated = false;
      
      return invocation.invokeNext();
   }
   
   public Object handleClose(Invocation invocation) throws Throwable
   {
      Object ret = invocation.invokeNext();
      
      //Finished with the connection - we need to shutdown callback server
      getState(invocation).getRemotingConnection().close();
      
      return ret;
   }
   
   // ConnectionListener implementation -----------------------------------------------------------
   
   public void handleConnectionException(Throwable t, Client c)
   {
      if (trace)
      {
         log.trace("Caught exception from connection", t);
      }
      if (exceptionListener != null)
      {
         
         if (t instanceof Error)
         {
            log.error("Caught error on connection", t);
         }
         else
         {
            Exception e =(Exception)t;
            JMSException j = new JMSException("Throwable received from underlying connection");
            j.setLinkedException(e);
            synchronized (exceptionListener)
            {
               exceptionListener.onException(j);
            }
         }
         
      }
      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   private ConnectionState getState(Invocation inv)
   {
      return (ConnectionState)((DelegateSupport)inv.getTargetObject()).getState();
   }
   
   // Inner classes -------------------------------------------------
}
