/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.jboss.logging.Logger;

/**
 * 
 * A Measurement.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class Measurement implements Serializable
{      
   private static final long serialVersionUID = 7208068351755451288L;

   private static final Logger log = Logger.getLogger(Measurement.class);
   
   protected String name;
   
   protected Double value;
   
   protected Map variables;
   
   public Measurement(String name, double value)
   {
      this.name = name;
      this.value = new Double(value);
      log.info("Measurement, name=" + name + ", value=" + value);
      variables = new HashMap();
   }
   
   public void setVariableValue(String variableName, double value)
   {
      variables.put(variableName, new Double(value));
   }
   
   public double getVariableValue(String variableName)
   {
      return ((Double)variables.get(variableName)).doubleValue();
   }
   
   public Map getVariables()
   {
      return variables;
   }

   /**
    * Get the name.
    * 
    * @return the name.
    */
   public String getName()
   {
      return name;
   }

   /**
    * Set the name.
    * 
    * @param name The name to set.
    */
   public void setName(String name)
   {
      this.name = name;
   }

   /**
    * Get the value.
    * 
    * @return the value.
    */
   public Double getValue()
   {
      return value;
   }

   /**
    * Set the value.
    * 
    * @param value The value to set.
    */
   public void setValue(Double value)
   {
      this.value = value;
   }
             
}
