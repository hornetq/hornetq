/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.jms.perf.framework;

import java.util.Map;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import org.jboss.logging.Logger;

/**
 * 
 * A SimpleJobStore.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version 1.1
 *
 * SimpleJobStore.java,v 1.1 2006/02/01 17:38:30 timfox Exp
 */
public class SimpleJobStore implements JobStore
{
   private static final Logger log = Logger.getLogger(BaseJob.class);

   private Map jobs;

   public SimpleJobStore()
   {
      jobs = new ConcurrentReaderHashMap();
   }

   public void addJob(Job job)
   {
      if (log.isTraceEnabled()) { log.trace("adding job " + job); }
      jobs.put(job.getID(), job);
   }

   public Job getJob(String jobId)
   {
      return (Job)jobs.get(jobId);
   }

   public void removeJob(String jobId)
   {
      jobs.remove(jobId);
   }

}
