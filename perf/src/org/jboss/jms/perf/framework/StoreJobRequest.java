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

/**
 * 
 * A StoreJobRequest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version 1.1
 *
 * StoreJobRequest.java,v 1.1 2006/02/01 17:38:30 timfox Exp
 */
public class StoreJobRequest implements ServerRequest
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -4083875589896827910L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private Job job;

   // Constructors --------------------------------------------------

   public StoreJobRequest(Job job)
   {
      this.job = job;
   }

   // ServerRequest implementation ----------------------------------

   public Object execute(JobStore store) throws Exception
   {
      store.addJob(job);

      job.initialize();

      return null;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "StoreJobRequest[" + job + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
