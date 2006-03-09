/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.data;

import org.jboss.jms.perf.framework.Job;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class SimpleJobList implements JobList
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private List jobs;

   // Constructors --------------------------------------------------

   public SimpleJobList()
   {
      jobs = new ArrayList();
   }

   // JobList implementation ----------------------------------------

   public void addJob(Job job)
   {
      jobs.add(job);
   }

   public int size()
   {
      return jobs.size();
   }

   public Iterator iterator()
   {
      return jobs.iterator();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
