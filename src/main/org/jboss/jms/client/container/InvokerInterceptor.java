/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.Client;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.InvocationResponse;

import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class InvokerInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------

   public static final InvokerInterceptor singleton = new InvokerInterceptor();

   public static final String REMOTING = "REMOTING";
   public static final String INVOKER_LOCATOR = "INVOKER_LOCATOR";
   public static final String SUBSYSTEM = "SUBSYSTEM";
   public static final String CLIENT = "CLIENT";


   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   // InvokerInterceptor implementation -----------------------------

   public String getName() { return "InvokerInterceptor"; }

   public Object invoke(Invocation invocation) throws Throwable
   {
      // look for a Client, it's possible that it has been created already
      Client client = (Client)invocation.getMetaData(REMOTING, CLIENT);

      if (client == null)
      {
         InvokerLocator locator = (InvokerLocator)invocation.getMetaData(REMOTING, INVOKER_LOCATOR);
         if (locator == null)
         {
            throw new RuntimeException("No InvokerLocator supplied.  Can't invoke remotely!");
         }
         String subsystem = (String)invocation.getMetaData(REMOTING, SUBSYSTEM);
         if (subsystem == null)
         {
            throw new RuntimeException("No subsystem supplied.  Can't invoke remotely!");
         }
         client = new Client(locator, subsystem);
      }
      InvocationResponse response = (InvocationResponse)client.invoke(invocation, null);
      invocation.setResponseContextInfo(response.getContextInfo());
      return response.getResponse();
   }

   // Public --------------------------------------------------------

   Object readResolve() throws ObjectStreamException
   {
      return singleton;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

