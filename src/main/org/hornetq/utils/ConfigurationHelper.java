/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.utils;

import java.util.Map;

import org.hornetq.core.logging.Logger;

/**
 * A ConfigurationHelper
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ConfigurationHelper
{
   public static final Logger log = Logger.getLogger(ConfigurationHelper.class);

   public static String getStringProperty(final String propName, final String def, final Map<String, Object> props)
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
            return prop.toString();
         }
         else
         {
            return (String)prop;
         }
      }
   }

   public static int getIntProperty(final String propName, final int def, final Map<String, Object> props)
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
         // The resource adapter will aways send Strings, hence the conversion here
         if (prop instanceof String)
         {
            return Integer.valueOf((String)prop);
         }
         else if (prop instanceof Integer == false)
         {
            log.warn("Property " + propName + " must be an Integer, it is " + prop.getClass().getName());

            return def;
         }
         else
         {
            return (Integer)prop;
         }
      }
   }

   public static long getLongProperty(final String propName, final long def, final Map<String, Object> props)
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
         // The resource adapter will aways send Strings, hence the conversion here
         if (prop instanceof String)
         {
            return Long.valueOf((String)prop);
         }
         else if (prop instanceof Long == false)
         {
            log.warn("Property " + propName + " must be an Long, it is " + prop.getClass().getName());

            return def;
         }
         else
         {
            return (Long)prop;
         }
      }
   }

   public static boolean getBooleanProperty(final String propName, final boolean def, final Map<String, Object> props)
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
         // The resource adapter will aways send Strings, hence the conversion here
         if (prop instanceof String)
         {
            return Boolean.valueOf((String)prop);
         }
         else if (prop instanceof Boolean == false)
         {
            log.warn("Property " + propName + " must be a Boolean, it is " + prop.getClass().getName());

            return def;
         }
         else
         {
            return (Boolean)prop;
         }
      }
   }
}
