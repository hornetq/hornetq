/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.util.Iterator;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class StopRequest implements ServerRequest
{
   private static final long serialVersionUID = 3776076737763152410L;

   public Object execute(JobStore store)
   {
      Iterator iter = store.getJobs().iterator();
      
      while (iter.hasNext())
      {
         Job job = (Job)iter.next();
         
         job.stop();
      }
      
      return null;
   }
}
