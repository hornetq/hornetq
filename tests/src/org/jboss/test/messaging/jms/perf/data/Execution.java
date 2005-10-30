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
