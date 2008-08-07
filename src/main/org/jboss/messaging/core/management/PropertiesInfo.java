/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.management;

import static javax.management.openmbean.SimpleType.STRING;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

/**
 * Info for a Message property.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class PropertiesInfo
{
   // Constants -----------------------------------------------------

   public static final TabularType TABULAR_TYPE;
   private static CompositeType ROW_TYPE;

   static
   {
      try
      {
         ROW_TYPE = new CompositeType("Property", "Property", new String[] {
               "key", "value" }, new String[] { "Key of the property",
               "Value of the property" }, new OpenType[] { STRING, STRING });
         TABULAR_TYPE = new TabularType("PropertyInfo",
               "Properties of the message", ROW_TYPE, new String[] { "key" });
      } catch (OpenDataException e)
      {
         e.printStackTrace();
         throw new IllegalStateException(e);
      }
   }

   // Attributes ----------------------------------------------------

   private final Map<String, String> properties = new HashMap<String, String>();

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public Map<String, String> entries()
   {
      return properties;
   }

   public void put(final String key, final String value)
   {
      properties.put(key, value);
   }

   public TabularData toTabularData()
   {
      try
      {
         TabularDataSupport data = new TabularDataSupport(TABULAR_TYPE);
         for (Entry<String, String> entry : properties.entrySet())
         {
            data
                  .put(new CompositeDataSupport(ROW_TYPE, new String[] { "key",
                        "value" }, new Object[] { entry.getKey(),
                        entry.getValue() }));
         }
         return data;
      } catch (OpenDataException e)
      {
         e.printStackTrace();
         return null;
      }
   }
}