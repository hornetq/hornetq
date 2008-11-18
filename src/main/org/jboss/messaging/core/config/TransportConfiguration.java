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
package org.jboss.messaging.core.config;

import java.io.Serializable;
import java.util.Map;

/**
 * A TransportConfiguration
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TransportConfiguration implements Serializable
{
   private static final long serialVersionUID = -3994528421527392679L;

   private final String factoryClassName;
   
   private final Map<String, Object> params;
   
   public TransportConfiguration(final String className, final Map<String, Object> params)
   {
      this.factoryClassName = className;
      
      this.params = params;
   }
   
   public TransportConfiguration(final String className)
   {
      this.factoryClassName = className;
      
      this.params = null;
   }
   
   public String getFactoryClassName()
   {
      return factoryClassName;
   }
   
   public Map<String, Object> getParams()
   {
      return params;
   }
   
   private int hash = -1;
   
   public int hashCode()
   {
      return factoryClassName.hashCode();
   }
   
   public boolean equals(final Object other)
   {
      if (other instanceof TransportConfiguration == false)
      {
         return false;
      }
      
      TransportConfiguration kother = (TransportConfiguration)other;

      if (factoryClassName.equals(kother.factoryClassName))
      {
         if (params == null)
         {
            return kother.params == null;
         }
         else
         {
            if (kother.params == null)
            {
               return false;
            }
            else if (params.size() == kother.params.size())
            {
               for (Map.Entry<String, Object> entry : params.entrySet())
               {
                  Object thisVal = entry.getValue();

                  Object otherVal = kother.params.get(entry.getKey());

                  if (otherVal == null || !otherVal.equals(thisVal))
                  {
                     return false;
                  }
               }
               return true;
            }
            else
            {
               return false;
            }
         }
      }
      else
      {
         return false;
      }
   }
   
}
