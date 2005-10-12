/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SlaveInfo implements Serializable
{
   protected String name;
   
   protected String locator;
   
   protected List jobs;
   
   public SlaveInfo(String name, String locator)
   {
      this.name = name;
      this.locator = locator;
      this.jobs = new ArrayList();
   }
   
   public void addJob(Job job)
   {
      jobs.add(job);
   }
   
   public List getJobs()
   {
      return jobs;
   }
   
   
}
