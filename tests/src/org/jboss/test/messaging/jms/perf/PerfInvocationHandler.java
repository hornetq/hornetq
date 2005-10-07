/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.test.messaging.jms.perf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
   
   protected Map jobs  = new HashMap();

   public void addJob(Job job)
   {
      jobs.put(job.getName(), job);
   }
   
   public void removeJobs()
   {
      jobs.clear();
   }
   
   public Collection getJobs()
   {
      return Collections.unmodifiableCollection(jobs.values());
   }
   
   public Job getJob(String jobName)
   {
      return (Job)jobs.get(jobName);
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