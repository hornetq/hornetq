/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.data;

import java.io.Serializable;

import org.jboss.logging.Logger;

public class Measurement implements Serializable
{      
   private static final long serialVersionUID = 7208068351755451288L;

   private static final Logger log = Logger.getLogger(Measurement.class);
   
   protected String dimensionName;
   
   protected Object value;
   
   public Measurement(String dimensionName, Object value)
   {
      this.dimensionName = dimensionName;
      
      this.value = value;
   }

   public String getDimensionName()
   {
      return dimensionName;
   }

   public void setDimensionName(String dimensionName)
   {
      this.dimensionName = dimensionName;
   }

   public Object getValue()
   {
      return value;
   }

   public void setValue(Object value)
   {
      this.value = value;
   }
   
   
}

