/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.lang.reflect.Method;

import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;

/**
 * Handles client id
 * There is one instance of this interceptor per connection
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
   
   private String clientID;
   
   private boolean justCreated = true;
   
   private ExceptionListener exceptionListener;

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
      if (invocation instanceof MethodInvocation)
      {
         MethodInvocation mi = (MethodInvocation)invocation;
         Method m = mi.getMethod();
         String name = m.getName();
         
         if (log.isTraceEnabled())
         {
            log.trace("ConnectionInterceptor, methodName=" + name);
         }
         
         if (!"setClientID".equals(name))
         {
            justCreated = false;
         }
         
         if ("getClientID".equals(name))
         {
            return getClientID(mi);
         }
         else if ("setClientID".equals(name))
         {
            String clientID = getClientID(mi);
            if (clientID != null)
            {
               throw new IllegalStateException("Client id has already been set");
            }
            if (!justCreated)
            {
               throw new IllegalStateException("setClientID can only be called directly after the connection is created");
            }
            this.clientID = (String)mi.getArguments()[0];
            //This gets invoked on the server too
            return invocation.invokeNext();
         }
         else if ("getExceptionListener".equals(name))
         {            
            return exceptionListener;
         }
         else if ("setExceptionListener".equals(name))
         {
            exceptionListener = (ExceptionListener)mi.getArguments()[0];
            
            return null;
         }
         else
         {            
            return invocation.invokeNext();
         }
      }
      throw new IllegalStateException("Shouldn't get here");
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   private String getClientID(Invocation invocation)
   {
      if (clientID != null)
      {
         return clientID;
      }
      
      clientID = (String)invocation.getMetaData(JMSAdvisor.JMS, JMSAdvisor.CLIENT_ID);
      
      return clientID;
   }

   // Inner classes -------------------------------------------------
}
