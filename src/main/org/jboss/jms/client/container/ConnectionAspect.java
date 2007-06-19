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

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.FailoverListener;
import org.jboss.jms.client.JBossConnectionMetaData;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.message.MessageIdGeneratorFactory;
import org.jboss.jms.tx.ResourceManagerFactory;

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
public class ConnectionAspect
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected JBossConnectionMetaData connMetaData;
   
   protected ConnectionState state;

   // The identity of the delegate this interceptor is associated with
   private Integer id;

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
      ConnectionState state = getConnectionState(invocation);
      state.setJustCreated(false);
      
      return state.getRemotingConnection().getConnectionListener().getJMSExceptionListener();
   }
   
   public Object handleSetExceptionListener(Invocation invocation) throws Throwable
   {
      ConnectionState state = getConnectionState(invocation);
      state.setJustCreated(false);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      ExceptionListener exceptionListener = (ExceptionListener)mi.getArguments()[0];
      state.getRemotingConnection().getConnectionListener().
         addJMSExceptionListener(exceptionListener);

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
      currentState.setJustCreated(false);
      return invocation.invokeNext();
   }
   
   public Object handleStop(Invocation invocation) throws Throwable
   {
      ConnectionState currentState = getConnectionState(invocation);
      currentState.setStarted(false);
      currentState.setJustCreated(false);
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
   	try
   	{
   		Object ret = invocation.invokeNext();
   		
         return ret;
   	}
   	finally
   	{
	      //Always cleanup in a finally - we need to cleanup if the server call to close fails too
   		
	      ConnectionState state = getConnectionState(invocation);
	
	      JMSRemotingConnection remotingConnection = state.getRemotingConnection();
	
	      // remove the consolidated remoting connection listener
	
	      ConsolidatedRemotingConnectionListener l = remotingConnection.removeConnectionListener();
	      if (l != null)
	      {
	         l.clear();
	      }
	
	      // Finished with the connection - we need to shutdown callback server
	      remotingConnection.stop();
	       
	      // Remove reference to message ID generator
	      MessageIdGeneratorFactory.instance.checkInGenerator(state.getServerID());
	      
	      // And to resource manager
	      ResourceManagerFactory.instance.checkInResourceManager(state.getServerID());
   	}
   }

   public Object  handleRegisterFailoverListener(Invocation invocation) throws Throwable
   {
      ConnectionState state = getConnectionState(invocation);

      MethodInvocation mi = (MethodInvocation)invocation;
      FailoverListener listener = (FailoverListener)mi.getArguments()[0];

      state.getFailoverCommandCenter().registerFailoverListener(listener);

      return null;
   }

   public Object handleUnregisterFailoverListener(Invocation invocation) throws Throwable
   {
      ConnectionState state = getConnectionState(invocation);

      MethodInvocation mi = (MethodInvocation)invocation;
      FailoverListener listener = (FailoverListener)mi.getArguments()[0];

      boolean result = state.getFailoverCommandCenter().unregisterFailoverListener(listener);

      return new Boolean(result);
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("ConnectionAspect[");

      if (id == null)
      {
         sb.append("UNINITIALIZED]");
      }
      else
      {
         sb.append(id).append("]");
      }
      return sb.toString();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private ConnectionState getConnectionState(Invocation invocation)
   {
      if (state == null)
      {
         ClientConnectionDelegate currentDelegate =
            ((ClientConnectionDelegate)invocation.getTargetObject());
         
         state = (ConnectionState)currentDelegate.getState();
         id = new Integer(state.getDelegate().getID());

      }
      return state;
   }
   
   
   // Inner classes -------------------------------------------------
}
