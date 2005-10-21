/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.jms.perf.PerfRunner;


public class Execution implements Serializable
{
   private static final long serialVersionUID = 8086268408804672852L;
   
   private static final Logger log = Logger.getLogger(Execution.class);   
   

   public Execution(Benchmark bm, Date date, String provider)
   {
      this.benchmark = bm;
      this.date = date;
      this.provider = provider;
      this.measurements = new ArrayList();
      bm.addExecution(this);
   }
   
   public Benchmark getBenchmark()
   {
      return benchmark;
   }
   
   
   protected Date date;
   
   protected String provider;
   
   protected List measurements;
   
   protected Benchmark benchmark;      

   public void addMeasurement(Measurement measure)
   {
     // log.info("QAdding measurement:" + measure);
      measurements.add(measure);
   }
   
   /**
    * Get the date.
    * 
    * @return the date.
    */
   public Date getDate()
   {
      return date;
   }

   /**
    * Set the date.
    * 
    * @param date The date to set.
    */
   public void setDate(Date date)
   {
      this.date = date;
   }

   /**
    * Get the provider.
    * 
    * @return the provider.
    */
   public String getProvider()
   {
      return provider;
   }

   /**
    * Set the provider.
    * 
    * @param provider The provider to set.
    */
   public void setProvider(String provider)
   {
      this.provider = provider;
   }

   /**
    * Get the measurements.
    * 
    * @return the measurements.
    */
   public List getMeasurements()
   {
    //  log.info("Getting measurements:" + measurements.size());
      return measurements;
   }
   
   
}
