/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 
 * A Execution.
 * 
 * Represents the record of an execution of a PerformanceTest on a particular Date,
 * against a particular provider e.g. JBossMQ, JBossMessaging etc.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class Execution implements Serializable
{
   private static final long serialVersionUID = 8086268408804672852L;
   
   protected Date date;
   
   protected String providerName;
   
   protected List datapoints;
   
   protected PerformanceTest performanceTest;      
   
   public Execution(PerformanceTest pt, Date date, String providerName)
   {
      this.performanceTest = pt;
      
      this.date = date;
      
      this.providerName = providerName;
      
      this.datapoints = new ArrayList();     
      
      pt.addExecution(this);
   }
   
   public PerformanceTest getPerformanceTest()
   {
      return performanceTest;
   }
   
   public void addDatapoint(Datapoint datapoint)
   {
      datapoints.add(datapoint);
   }
   
   public Date getDate()
   {
      return date;
   }

   public void setDate(Date date)
   {
      this.date = date;
   }

   public String getProviderName()
   {
      return providerName;
   }

   public void setProviderName(String providerName)
   {
      this.providerName = providerName;
   }

   public List getDatapoints()
   {
      return datapoints;
   }
   
   
}
