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

import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.util.Pair;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class BroadcastGroupConfigurationInfo
{
   // Constants -----------------------------------------------------

   public static final CompositeType TYPE;

   private static final String MESSAGE_TYPE_NAME = "BroadcastGroupConfigurationInfo";

   private static final String MESSAGE_TABULAR_TYPE_NAME = "BroadcastGroupConfigurationTabularInfo";

   private static final String[] ITEM_NAMES = new String[] { "name",
                                                            "localBindAddress",
                                                            "localBindPort",
                                                            "groupAddress",
                                                            "groupPort",
                                                            "broadcastPeriod",
                                                            "connectorInfos" };

   private static final String[] ITEM_DESCRIPTIONS = new String[] { "Name of the broadcast group",
                                                                   "Address of local bind",
                                                                   "port of local bind",
                                                                   "Address of the broadcast group",
                                                                   "Port of the broadcast group",
                                                                   "Period of broadcast",
                                                                   "Name of the connectors" };

   private static final OpenType[] TYPES;

   private static final TabularType TABULAR_TYPE;

   static
   {
      try
      {
         TYPES = new OpenType[] { STRING, STRING, INTEGER, STRING, INTEGER, LONG, PairsInfo.TABULAR_TYPE };
         TYPE = new CompositeType(MESSAGE_TYPE_NAME,
                                  "Information for a BroadcastGroupConfigurationInfo",
                                  ITEM_NAMES,
                                  ITEM_DESCRIPTIONS,
                                  TYPES);
         TABULAR_TYPE = new TabularType(MESSAGE_TABULAR_TYPE_NAME,
                                        "Information for tabular BroadcastGroupConfigurationInfo",
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

   private final BroadcastGroupConfiguration config;

   // Static --------------------------------------------------------

   public static TabularData toTabularData(final Collection<BroadcastGroupConfiguration> configs) throws OpenDataException
   {
      TabularData data = new TabularDataSupport(TABULAR_TYPE);
      for (BroadcastGroupConfiguration config : configs)
      {
         data.put(new BroadcastGroupConfigurationInfo(config).toCompositeData());
      }
      return data;
   }

   public static BroadcastGroupConfiguration[] from(TabularData msgs)
   {
      Collection values = msgs.values();
      List<BroadcastGroupConfiguration> infos = new ArrayList<BroadcastGroupConfiguration>();
      for (Object object : values)
      {
         CompositeData compositeData = (CompositeData)object;
         String name = (String)compositeData.get("name");
         String localBindAddress = (String)compositeData.get("localBindAddress");
         int localBindPort = (Integer)compositeData.get("localBindPort");
         String groupAddress = (String)compositeData.get("groupAddress");
         int groupPort = (Integer)compositeData.get("groupPort");
         long broadcastPeriod = (Long)compositeData.get("broadcastPeriod");
         
         TabularData connectorData = (TabularData)compositeData.get("connectorInfos");
         List<Pair<String, String>> connectorInfos = PairsInfo.from(connectorData);         
         if (connectorInfos.size() == 0)
         {
            connectorInfos = null;
         }
         
         infos.add(new BroadcastGroupConfiguration(name,
                                                   localBindAddress,
                                                   localBindPort,
                                                   groupAddress,
                                                   groupPort,
                                                   broadcastPeriod,
                                                   connectorInfos));
      }

      return (BroadcastGroupConfiguration[])infos.toArray(new BroadcastGroupConfiguration[infos.size()]);
   }

   // Constructors --------------------------------------------------

   public BroadcastGroupConfigurationInfo(final BroadcastGroupConfiguration config)
   {
      this.config = config;
   }

   // Public --------------------------------------------------------

   public CompositeData toCompositeData()
   {
      try
      {         
         return new CompositeDataSupport(TYPE, ITEM_NAMES, new Object[] { config.getName(),
                                                                         config.getLocalBindAddress(),
                                                                         config.getLocalBindPort(),
                                                                         config.getGroupAddress(),
                                                                         config.getGroupPort(),
                                                                         config.getBroadcastPeriod(),
                                                                         PairsInfo.toTabularData(config.getConnectorInfos()) });
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
