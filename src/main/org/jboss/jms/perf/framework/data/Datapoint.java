/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.logging.Logger;

/**
 * 
 * A Datapoint.
 * 
 * Represents a measured data point - a data point can have many dimensions
 * 
 * A data-point may, for example, be a point on a graph of send rate vs receive rate
 * in this case it has two dimensions (send rate and receive rate)
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class Datapoint implements Serializable
{      
   private static final long serialVersionUID = 7208068351755451288L;

   private static final Logger log = Logger.getLogger(Datapoint.class);
   
   protected String name;
   
   protected Map measurements;
   
   public Datapoint(String name)
   {
      this.name = name;
      
      measurements = new HashMap();
   }

   public String getName()
   {
      return name;
   }

   public void setName(String name)
   {
      this.name = name;
   }

   public List getMeasurements()
   {
      return new ArrayList(measurements.values());
   }

//   public void setMeasurements(List measurements)
//   {
//      this.measurements = measurements;
//   }
   
   public void addMeasurement(Measurement measurement)
   {
      measurements.put(measurement.getDimensionName(), measurement);
   }
   
   
   public Measurement getMeasurement(String dimensionName)
   {
      return (Measurement)measurements.get(dimensionName);
   }
        
}
