/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.perf.framework;

import javax.management.MBeanServer;

import org.jboss.logging.Logger;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

/**
 * 
 * A ExecutorInvocationHandler.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ExecutorInvocationHandler implements ServerInvocationHandler
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ExecutorInvocationHandler.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected JobStore store;

   // Constructors --------------------------------------------------

   public ExecutorInvocationHandler()
   {
      store = new SimpleJobStore();
   }

   // ServerInvocationHandler implementation -----------------------

   public Object invoke(InvocationRequest invocation) throws Throwable
   {
      ServerRequest request = (ServerRequest)invocation.getParameter();

      log.debug("received " + request);

      return request.execute(store);
   }

   public void addListener(InvokerCallbackHandler callbackHandler)
   {
      log.warn("addListener(" + callbackHandler + ") ignored!");
   }

   public void removeListener(InvokerCallbackHandler callbackHandler)
   {
      log.warn("removeListener(" + callbackHandler + ") ignored!");
   }

   public void setMBeanServer(MBeanServer server)
   {
      log.warn("setMBeanServer(" + server + ") ignored!");
   }

   public void setInvoker(ServerInvoker invoker)
   {
      log.warn("setInvoker(" + invoker + ") ignored!");
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------



}