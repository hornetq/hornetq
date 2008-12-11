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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.jboss.messaging.core.config.TransportConfiguration;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class TransportConfigurationInfo
{
   // Constants -----------------------------------------------------

   public static final CompositeType TYPE;

   private static final String MESSAGE_TYPE_NAME = "TransportConfigurationInfo";

   private static final String MESSAGE_TABULAR_TYPE_NAME = "TransportConfigurationTabularInfo";

   private static final String[] ITEM_NAMES = new String[] { "name", "factoryClassName", "parameters" };

   private static final String[] ITEM_DESCRIPTIONS = new String[] { "Name of the connector",
                                                                   "Name of the Transport factory class",
                                                                   "Parameters" };

   private static final OpenType[] TYPES;

   private static final TabularType TABULAR_TYPE;

   static
   {
      try
      {
         TYPES = new OpenType[] { STRING, STRING, PropertiesInfo.TABULAR_TYPE };
         TYPE = new CompositeType(MESSAGE_TYPE_NAME,
                                  "Information for a TransportConfigurationInfo",
                                  ITEM_NAMES,
                                  ITEM_DESCRIPTIONS,
                                  TYPES);
         TABULAR_TYPE = new TabularType(MESSAGE_TABULAR_TYPE_NAME,
                                        "Information for tabular TransportConfigurationInfo",
                                        TYPE,
                                        new String[] { "name", "factoryClassName" });
      }
      catch (OpenDataException e)
      {
         e.printStackTrace();
         throw new IllegalStateException(e);
      }
   }

   // Attributes ----------------------------------------------------

   private final TransportConfiguration config;

   // Static --------------------------------------------------------

   public static TabularData toTabularData(final Collection<TransportConfiguration> configs) throws OpenDataException
   {
      TabularData data = new TabularDataSupport(TABULAR_TYPE);
      for (TransportConfiguration config : configs)
      {
         data.put(new TransportConfigurationInfo(config).toCompositeData());
      }
      return data;
   }

   public static TransportConfiguration[] from(TabularData msgs)
   {
      Collection values = msgs.values();
      List<TransportConfiguration> infos = new ArrayList<TransportConfiguration>();
      for (Object object : values)
      {
         CompositeData compositeData = (CompositeData)object;
         String name = (String)compositeData.get("name");
         String factoryClassName = (String)compositeData.get("factoryClassName");

         TabularData connectorData = (TabularData)compositeData.get("parameters");
         PropertiesInfo params = PropertiesInfo.from(connectorData);

         Map<String, Object> p = new HashMap<String, Object>(params.entries());
         infos.add(new TransportConfiguration(factoryClassName, p, name));
      }

      return (TransportConfiguration[])infos.toArray(new TransportConfiguration[infos.size()]);
   }

   // Constructors --------------------------------------------------

   public TransportConfigurationInfo(final TransportConfiguration config)
   {
      this.config = config;
   }

   // Public --------------------------------------------------------

   public CompositeData toCompositeData()
   {
      try
      {
         return new CompositeDataSupport(TYPE,
                                         ITEM_NAMES,
                                         new Object[] { config.getName(),
                                                       config.getFactoryClassName(),
                                                       PropertiesInfo.toTabularData(config.getParams()) });
      }
      catch (OpenDataException e)
      {
         e.printStackTrace();
         return null;
      }
   }

   @Override
   public String toString()
   {
      return "BroadcastGroupConfigurationInfo[config=" + config.toString() + "]";
   }
}
