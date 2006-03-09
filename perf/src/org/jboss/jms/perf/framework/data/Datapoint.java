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
 * Represents a measured data point - a data point can have many dimensions
 * 
 * A data-point may, for example, be a point on a graph of send rate vs receive rate in this case it
 * has two dimensions (send rate and receive rate)
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class Datapoint implements Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 7208068351755451288L;

   private static final Logger log = Logger.getLogger(Datapoint.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private int index;
   // Map<dimensionName - value>
   private Map measurements;

   // Constructors --------------------------------------------------

   /**
    * @param index - indicates order in which this datapoint was taken, relative to other datapoints
    *                Datapoint 0 was measured before datapoint 1.
    */
   public Datapoint(int index)
   {
      this.index = index;
      measurements = new HashMap();
   }

   // Public --------------------------------------------------------

   public int getIndex()
   {
      return index;
   }

   public void addMeasurement(String dimensionName, Serializable value)
   {
      measurements.put(dimensionName, value);
   }

   public Serializable getMeasurement(String dimensionName)
   {
      return (Serializable)measurements.get(dimensionName);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
