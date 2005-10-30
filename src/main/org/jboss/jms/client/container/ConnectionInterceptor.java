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

import java.io.Serializable;

import javax.jms.ConnectionMetaData;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.JBossConnectionMetaData;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.ConnectionListener;

/**
 * Handles operations related to the connection
 * 
 * Important! There is one instance of this interceptor per instance of Connection
 * and ConnectionFactory
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionInterceptor implements Interceptor, Serializable, ConnectionListener
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -3245645348483459328L;

   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ConnectionInterceptor.class);

   // Attributes ----------------------------------------------------
   
   protected String clientID;
   
   protected ExceptionListener exceptionListener;
   
   protected ConnectionMetaData connMetaData = new JBossConnectionMetaData();
   
   boolean justCreated = true;
   
   protected ResourceManager rm;
   
   protected boolean listenerAdded;
   
   // Constructors --------------------------------------------------
   
   public ConnectionInterceptor()
   {
      if (log.isTraceEnabled()) { log.trace("Creating new ConnectionInterceptor"); }
   }

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "ConnectionInterceptor";  
   }
   
   public Object invoke(Invocation invocation) throws Throwable
   {
      if (!(invocation instanceof MethodInvocation))
      {
         return invocation.invokeNext();
      }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      String methodName = mi.getMethod().getName();      
            
      if (log.isTraceEnabled()) { log.trace("handling " + methodName); }
      
      if ("createConnectionDelegate".equals(methodName))
      {
         ConnectionDelegate connectionDelegate = (ConnectionDelegate)invocation.invokeNext();
         ResourceManager rm = new ResourceManager(connectionDelegate);

         // TODO: Why not just this.rm = rm?
         connectionDelegate.setResourceManager(rm);
         
//         Pinger thePinger = new Pinger(connectionDelegate);
//         thePinger.start();
//         connectionDelegate.setPinger(thePinger);
         
         return connectionDelegate;
      }
        
      if ("getClientID".equals(methodName))
      {           
         justCreated = false;
         if (clientID == null)          
         {
            //Get from server
            clientID = (String)invocation.invokeNext();
         }
         return clientID;
      }
      else if ("setClientID".equals(methodName))
      {            
         if (clientID != null)
         {
            throw new IllegalStateException("Client id has already been set");
         }
         if (!justCreated)
         {
            throw new IllegalStateException("setClientID can only be called directly after the connection is created");
         }
         clientID = (String)mi.getArguments()[0];
         
         justCreated = false;
         
         //This gets invoked on the server too
         return invocation.invokeNext();
      }
      else if ("getExceptionListener".equals(methodName))
      {            
         justCreated = false;
         return exceptionListener;
      }
      else if ("setExceptionListener".equals(methodName))
      {
         justCreated = false;
         exceptionListener = (ExceptionListener)mi.getArguments()[0];
         
         Client client = (Client)invocation.getMetaData(RemotingClientInterceptor.REMOTING, RemotingClientInterceptor.CLIENT);                  

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
      else if ("getConnectionMetaData".equals(methodName))
      {
         justCreated = false;
         return connMetaData;
      }
      else if ("getResourceManager".equals(methodName))
      {
         return rm;
      }
      else if ("setResourceManager".equals(methodName))
      {
         this.rm = (ResourceManager)mi.getArguments()[0];
         if (log.isTraceEnabled()) { log.trace("setting the resource manager, ending the invocation"); }
         return null;
      }
      else if ("createSessionDelegate".equals(methodName))
      {
         justCreated = false;
         return invocation.invokeNext();
      }
      else
      {
         return invocation.invokeNext();
      }
   }
   
   // ConnectionListener implementation
   
   public void handlerConnectionException(Throwable t, Client c)
   {
      if (log.isTraceEnabled())
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

   // Inner classes -------------------------------------------------
}
