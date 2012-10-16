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

import org.hornetq.core.config.impl.Validators;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * A XMLConfigurationUtil
 *
 * @author jmesnil
 *
 *
 */
public class XMLConfigurationUtil
{

   public static Double getDouble(final Element e,
                                  final String name,
                                  final double def,
                                  final Validators.Validator validator)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         double val = XMLUtil.parseDouble(nl.item(0));
         validator.validate(name, val);
         return val;
      }
      else
      {
         validator.validate(name, def);
         return def;
      }
   }

   public static String getString(final Element e,
                                  final String name,
                                  final String def,
                                  final Validators.Validator validator)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         String val = nl.item(0).getTextContent().trim();
         validator.validate(name, val);
         return val;
      }
      else
      {
         validator.validate(name, def);
         return def;
      }
   }

   public static Long getLong(final Element e, final String name, final long def, final Validators.Validator validator)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         long val = XMLUtil.parseLong(nl.item(0));
         validator.validate(name, val);
         return val;
      }
      else
      {
         validator.validate(name, def);
         return def;
      }
   }

   public static Integer getInteger(final Element e,
                                    final String name,
                                    final int def,
                                    final Validators.Validator validator)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         int val = XMLUtil.parseInt(nl.item(0));
         validator.validate(name, val);
         return val;
      }
      else
      {
         validator.validate(name, def);
         return def;
      }
   }

   public static Boolean getBoolean(final Element e, final String name, final boolean def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return org.hornetq.utils.XMLUtil.parseBoolean(nl.item(0));
      }
      else
      {
         return def;
      }
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
