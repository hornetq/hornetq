/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

   public static Double getDouble(final Element e, final String name, final double def, final Validators.Validator validator)
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

   public static String getString(final Element e, final String name, final String def, final Validators.Validator validator)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         String val =  nl.item(0).getTextContent().trim();
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

   public static Integer getInteger(final Element e, final String name, final int def, final Validators.Validator validator)
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
