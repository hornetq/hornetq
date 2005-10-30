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
import java.util.HashMap;
import java.util.Map;

public class Measurement implements Serializable
{      
   private static final long serialVersionUID = 7208068351755451288L;

   protected String name;
   
   protected Double value;
   
   protected Map variables;
   
   public Measurement(String name, double value)
   {
      this.name = name;
      this.value = new Double(value);
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
