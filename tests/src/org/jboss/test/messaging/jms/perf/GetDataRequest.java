/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class GetDataRequest implements ServerRequest
{
   private static final long serialVersionUID = -5794495523075748864L;

   public Object execute(JobStore store)
   {
      List data = new ArrayList();
      Iterator iter = store.getJobs().iterator();
      while (iter.hasNext())
      {
         Job job = (Job)iter.next();
         data.add(job.getData());         
      }
      return data;
   }

}
