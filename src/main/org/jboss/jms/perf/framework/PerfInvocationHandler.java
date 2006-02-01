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
 * A PerfInvocationHandler.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class PerfInvocationHandler implements ServerInvocationHandler
{
   private static final Logger log = Logger.getLogger(PerfInvocationHandler.class);
   
   protected JobStore store;
   
   public PerfInvocationHandler()
   {
      store = new SimpleJobStore();
   }

   public Object invoke(InvocationRequest invocation) throws Throwable
   {
      log.trace("Received request");
      ServerRequest request = (ServerRequest)invocation.getParameter();
      return request.execute(store);         
   }

   public void addListener(InvokerCallbackHandler callbackHandler)
   {
      
   }

   public void removeListener(InvokerCallbackHandler callbackHandler)
   {
   }

   public void setMBeanServer(MBeanServer server)
   {      
   }
   
   public void setInvoker(ServerInvoker invoker)
   {      
   }
}