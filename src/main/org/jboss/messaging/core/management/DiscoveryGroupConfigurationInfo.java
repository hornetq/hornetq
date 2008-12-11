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

import static javax.management.openmbean.SimpleType.INTEGER;
import static javax.management.openmbean.SimpleType.LONG;
import static javax.management.openmbean.SimpleType.STRING;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class DiscoveryGroupConfigurationInfo
{
   // Constants -----------------------------------------------------

   public static final CompositeType TYPE;

   private static final String MESSAGE_TYPE_NAME = "DiscoveryGroupConfigurationInfo";

   private static final String MESSAGE_TABULAR_TYPE_NAME = "DiscoveryGroupConfigurationTabularInfo";

   private static final String[] ITEM_NAMES = new String[] { "name", "groupAddress", "groupPort", "refreshTimeout" };

   private static final String[] ITEM_DESCRIPTIONS = new String[] { "Name of the discovery group",
                                                                   "Address of the discovery group",
                                                                   "Port of the discovery group",
                                                                   "Timeout to refresh" };

   private static final OpenType[] TYPES;

   private static final TabularType TABULAR_TYPE;

   static
   {
      try
      {
         TYPES = new OpenType[] { STRING, STRING, INTEGER, LONG };
         TYPE = new CompositeType(MESSAGE_TYPE_NAME,
                                  "Information for a DiscoveryGroupConfiguration",
                                  ITEM_NAMES,
                                  ITEM_DESCRIPTIONS,
                                  TYPES);
         TABULAR_TYPE = new TabularType(MESSAGE_TABULAR_TYPE_NAME,
                                        "Information for tabular DiscoveryGroupConfiguration",
                                        TYPE,
                                        new String[] { "name" });
      }
      catch (OpenDataException e)
      {
         e.printStackTrace();
         throw new IllegalStateException(e);
      }
   }

   // Attributes ----------------------------------------------------

   private final DiscoveryGroupConfiguration config;

   // Static --------------------------------------------------------

   public static TabularData toTabularData(final Collection<DiscoveryGroupConfiguration> configs) throws OpenDataException
   {
      TabularData data = new TabularDataSupport(TABULAR_TYPE);
      for (DiscoveryGroupConfiguration config : configs)
      {
         data.put(new DiscoveryGroupConfigurationInfo(config).toCompositeData());
      }
      return data;
   }

   public static DiscoveryGroupConfiguration[] from(TabularData msgs)
   {
      Collection values = msgs.values();
      List<DiscoveryGroupConfiguration> infos = new ArrayList<DiscoveryGroupConfiguration>();
      for (Object object : values)
      {
         CompositeData compositeData = (CompositeData)object;
         String name = (String)compositeData.get("name");
         String groupAddress = (String)compositeData.get("groupAddress");
         int groupPort = (Integer)compositeData.get("groupPort");
         long refreshTimeout = (Long)compositeData.get("refreshTimeout");

         infos.add(new DiscoveryGroupConfiguration(name, groupAddress, groupPort, refreshTimeout));
      }

      return (DiscoveryGroupConfiguration[])infos.toArray(new DiscoveryGroupConfiguration[infos.size()]);
   }

   // Constructors --------------------------------------------------

   public DiscoveryGroupConfigurationInfo(final DiscoveryGroupConfiguration config)
   {
      this.config = config;
   }

   // Public --------------------------------------------------------

   public CompositeData toCompositeData()
   {
      try
      {
         return new CompositeDataSupport(TYPE, ITEM_NAMES, new Object[] { config.getName(),
                                                                         config.getGroupAddress(),
                                                                         config.getGroupPort(),
                                                                         config.getRefreshTimeout() });
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
      return "DiscoveryGroupConfiguration[config=" + config.toString() + "]";
   }
}
