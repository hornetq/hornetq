/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import java.io.Serializable;

import javax.jms.ConnectionMetaData;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.JBossConnectionMetaData;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.logging.Logger;

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
public class ConnectionInterceptor implements Interceptor, Serializable
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
   

   // Constructors --------------------------------------------------
   
   public ConnectionInterceptor()
   {
      if (log.isTraceEnabled())
      {
         log.trace("Creating new ConnectionInterceptor");
      }
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
            
      if (log.isTraceEnabled())
      {
         log.trace("ConnectionInterceptor, methodName=" + methodName);
      }
      
      if ("createConnectionDelegate".equals(methodName))
      {
         ConnectionDelegate connectionDelegate = (ConnectionDelegate)invocation.invokeNext();
         ResourceManager rm = new ResourceManager(connectionDelegate);
         connectionDelegate.setResourceManager(rm);
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
