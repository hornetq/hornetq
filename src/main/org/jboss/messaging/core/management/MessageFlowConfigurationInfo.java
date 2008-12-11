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

import static javax.management.openmbean.SimpleType.BOOLEAN;
import static javax.management.openmbean.SimpleType.DOUBLE;
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

import org.jboss.messaging.core.config.cluster.MessageFlowConfiguration;
import org.jboss.messaging.util.Pair;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MessageFlowConfigurationInfo
{
   // Constants -----------------------------------------------------

   public static final CompositeType TYPE;

   private static final String MESSAGE_TYPE_NAME = "MessageFlowConfigurationInfo";

   private static final String MESSAGE_TABULAR_TYPE_NAME = "MessageFlowConfigurationTabularInfo";

   private static final String[] ITEM_NAMES = new String[] { "name",
                                                            "address",
                                                            "filter",
                                                            "fanout",
                                                            "maxBatchSize",
                                                            "maxBatchTime",
                                                            "transformerClassName",
                                                            "retryInterval",
                                                            "retryIntervalMultiplier",
                                                            "maxRetriesBeforeFailover",
                                                            "maxRetriesAfterFailover",
                                                            "useDuplicateDetection",
                                                            "discoveryGroupName",
                                                            "staticConnectorNamePairs" };

   private static final String[] ITEM_DESCRIPTIONS = new String[] { "Name",
                                                                   "Address",
                                                                   "Filter",
                                                                   "Does the flow fanout messages?",
                                                                   "Maximum size of a batch",
                                                                   "Maximum time before sending a batch",
                                                                   "Name of the Transformer class",
                                                                   "Interval to retry",
                                                                   "Multiplier of the interval to retry",
                                                                   "Maximum number of retries before a failover",
                                                                   "Maximum number of retries after a failover",
                                                                   "Use duplicate detection?",
                                                                   "Name of the discovery group",
                                                                   "Static pairs of connectors" };

   private static final OpenType[] TYPES;

   private static final TabularType TABULAR_TYPE;

   static
   {
      try
      {
         TYPES = new OpenType[] { STRING,
                                 STRING,
                                 STRING,
                                 BOOLEAN,
                                 INTEGER,
                                 LONG,
                                 STRING,
                                 LONG,
                                 DOUBLE,
                                 INTEGER,
                                 INTEGER,
                                 BOOLEAN,
                                 STRING,
                                 PairsInfo.TABULAR_TYPE };
         TYPE = new CompositeType(MESSAGE_TYPE_NAME,
                                  "Information for a MessageFlowConfiguration",
                                  ITEM_NAMES,
                                  ITEM_DESCRIPTIONS,
                                  TYPES);
         TABULAR_TYPE = new TabularType(MESSAGE_TABULAR_TYPE_NAME,
                                        "Information for tabular MessageFlowConfiguration",
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

   private MessageFlowConfiguration config;

   // Static --------------------------------------------------------

   public static TabularData toTabularData(final Collection<MessageFlowConfiguration> configs) throws OpenDataException
   {
      TabularData data = new TabularDataSupport(TABULAR_TYPE);

      for (MessageFlowConfiguration config : configs)
      {
         data.put(new MessageFlowConfigurationInfo(config).toCompositeData());
      }
      return data;
   }

   public static MessageFlowConfiguration[] from(TabularData msgs)
   {
      Collection values = msgs.values();
      List<MessageFlowConfiguration> infos = new ArrayList<MessageFlowConfiguration>();
      for (Object object : values)
      {
         CompositeData compositeData = (CompositeData)object;
         String name = (String)compositeData.get("name");
         String address = (String)compositeData.get("address");
         String filter = (String)compositeData.get("filter");
         boolean fanout = (Boolean)compositeData.get("fanout");
         int maxBatchSize = (Integer)compositeData.get("maxBatchSize");
         long maxBatchTime = (Long)compositeData.get("maxBatchTime");
         String transformerClassName = (String)compositeData.get("transformerClassName");
         long retryInterval = (Long)compositeData.get("retryInterval");
         double retryIntervalMultiplier = (Double)compositeData.get("retryIntervalMultiplier");
         int maxRetriesBeforeFailover = (Integer)compositeData.get("maxRetriesBeforeFailover");
         int maxRetriesAfterFailover = (Integer)compositeData.get("maxRetriesAfterFailover");
         boolean useDuplicateDetection = (Boolean)compositeData.get("useDuplicateDetection");
         String discoveryGroupName = (String)compositeData.get("discoveryGroupName");

         TabularData connectorData = (TabularData)compositeData.get("staticConnectorNamePairs");
         List<Pair<String, String>> connectorInfos = PairsInfo.from(connectorData);
         if (connectorInfos.size() == 0)
         {
            connectorInfos = null;
         }

         if (connectorInfos == null)
         {
            infos.add(new MessageFlowConfiguration(name,
                                                   address,
                                                   filter,
                                                   fanout,
                                                   maxBatchSize,
                                                   maxBatchTime,
                                                   transformerClassName,
                                                   retryInterval,
                                                   retryIntervalMultiplier,
                                                   maxRetriesBeforeFailover,
                                                   maxRetriesAfterFailover,
                                                   useDuplicateDetection,
                                                   discoveryGroupName));
         }
         else
         {
            infos.add(new MessageFlowConfiguration(name,
                                                   address,
                                                   filter,
                                                   fanout,
                                                   maxBatchSize,
                                                   maxBatchTime,
                                                   transformerClassName,
                                                   retryInterval,
                                                   retryIntervalMultiplier,
                                                   maxRetriesBeforeFailover,
                                                   maxRetriesAfterFailover,
                                                   useDuplicateDetection,
                                                   connectorInfos));
         }
      }

      return (MessageFlowConfiguration[])infos.toArray(new MessageFlowConfiguration[infos.size()]);
   }

   // Constructors --------------------------------------------------

   public MessageFlowConfigurationInfo(MessageFlowConfiguration config)
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
                                                       config.getAddress(),
                                                       config.getFilterString(),
                                                       config.isFanout(),
                                                       config.getMaxBatchSize(),
                                                       config.getMaxBatchTime(),
                                                       config.getTransformerClassName(),
                                                       config.getRetryInterval(),
                                                       config.getRetryIntervalMultiplier(),
                                                       config.getMaxRetriesBeforeFailover(),
                                                       config.getMaxRetriesAfterFailover(),
                                                       config.isUseDuplicateDetection(),
                                                       config.getDiscoveryGroupName(),
                                                       PairsInfo.toTabularData(config.getConnectorNamePairs()) });
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
      return "MessageInfo[config=" + config.toString() + "]";
   }
}
