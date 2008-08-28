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
package org.jboss.messaging.util;

import java.util.Map;

import org.jboss.messaging.core.logging.Logger;

/**
 * A ConfigurationHelper
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ConfigurationHelper
{
   public static final Logger log = Logger.getLogger(ConfigurationHelper.class);
   
   public static String getStringProperty(final String propName, final String def,
                                          final Map<String, Object> props)
   {
      if (props == null)
      {
         return def;
      }
      
      Object prop = props.get(propName);
      
      if (prop == null)
      {
         return def;
      }
      else
      {
         if (prop instanceof String == false)
         {
            log.warn("Property " + propName + " must be a String");
            
            return def;
         }
         else
         {
            return (String)prop;
         }
      }      
   }
   
   public static int getIntProperty(final String propName, final int def,
                                    final Map<String, Object> props)
   {
      if (props == null)
      {
         return def;
      }
      Object prop = props.get(propName);

      if (prop == null)
      {
         return def;
      }
      else
      {
         if (prop instanceof Integer == false)
         {
            log.warn("Property " + propName + " must be an Integer");

            return def;
         }
         else
         {
            return (Integer)prop;
         }
      }      
   }
   
   public static long getLongProperty(final String propName, final long def,
                                     final Map<String, Object> props)
   {
      if (props == null)
      {
         return def;
      }
      
      Object prop = props.get(propName);

      if (prop == null)
      {
         return def;
      }
      else
      {
         if (prop instanceof Long == false)
         {
            log.warn("Property " + propName + " must be an Long");

            return def;
         }
         else
         {
            return (Long)prop;
         }
      }      
   }
   
   public static boolean getBooleanProperty(final String propName, final boolean def,
            final Map<String, Object> props)
   {
      if (props == null)
      {
         return def;
      }
      
      Object prop = props.get(propName);

      if (prop == null)
      {
         return def;
      }
      else
      {
         if (prop instanceof Boolean == false)
         {
            log.warn("Property " + propName + " must be a Boolean");

            return def;
         }
         else
         {
            return (Boolean)prop;
         }
      }      
   }
}
