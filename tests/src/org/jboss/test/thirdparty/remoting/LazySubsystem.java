/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting;

import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.test.thirdparty.remoting.util.TestableSubsystem;
import org.jboss.logging.Logger;

import javax.management.MBeanServer;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class LazySubsystem implements ServerInvocationHandler, TestableSubsystem
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(LazySubsystem.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public LazySubsystem()
   {
   }

   // ServerInvocationHandler implementation -------------------------------------------------------

   public void setMBeanServer(MBeanServer server)
   {
   }

   public void setInvoker(ServerInvoker invoker)
   {
   }

   public Object invoke(InvocationRequest invocation) throws Throwable
   {
      long sleepTime = ((Long)invocation.getParameter()).longValue();

      log.debug("sleeping for " + (sleepTime / 1000) + " seconds ...");

      Thread.sleep(sleepTime);

      log.debug("woke up");
      
      return null;
   }

   public void addListener(InvokerCallbackHandler callbackHandler)
   {
   }

   public void removeListener(InvokerCallbackHandler callbackHandler)
   {
   }

   // TestableSubsystem implementation ----------------------------------------------------------

   public InvocationRequest getNextInvocation(long timeout) throws InterruptedException
   {
      return null;
   }

   public boolean isFailed()
   {
      return false;
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}

