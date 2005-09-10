/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.test.messaging.jms.perf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.management.MBeanServer;

import org.jboss.logging.Logger;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class PerfInvocationHandler implements ServerInvocationHandler, JobStore
{
   private static final Logger log = Logger.getLogger(PerfInvocationHandler.class);
   
   protected List tests = new ArrayList();


   public void addJob(Job test)
   {
      tests.add(test);
   }
   
   public void removeJobs()
   {
      tests.clear();
   }
   
   public List getJobs()
   {
      return Collections.unmodifiableList(tests);
   }
   
   public Object invoke(InvocationRequest invocation) throws Throwable
   {
      log.info("Received request");
      ServerRequest request = (ServerRequest)invocation.getParameter();
      return request.execute(this);         
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