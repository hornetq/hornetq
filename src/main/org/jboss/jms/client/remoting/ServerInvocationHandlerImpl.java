/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.remoting;

import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvokerCallbackHandler;

import javax.management.MBeanServer;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerInvocationHandlerImpl implements ServerInvocationHandler
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   // ServerInvocationHandler implementation ------------------------

   public void setMBeanServer(MBeanServer server)
   {
   }

   public void setInvoker(ServerInvoker invoker)
   {
   }

   public Object invoke(InvocationRequest invocation) throws Throwable
   {
      return null;
   }

   public void addListener(InvokerCallbackHandler callbackHandler)
   {
   }

   public void removeListener(InvokerCallbackHandler callbackHandler)
   {
   }
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
