/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.jms.server.config.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.utils.BufferHelper;
import org.hornetq.utils.DataConstants;

/**
 * A TransportConfigurationEncodingSupport
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class TransportConfigurationEncodingSupport
{
   public static List<Pair<TransportConfiguration, TransportConfiguration>> decodeConfigs(HornetQBuffer buffer)
   {
      int size = buffer.readInt();
      List<Pair<TransportConfiguration, TransportConfiguration>> configs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>(size);

      for (int i = 0; i < size; i++)
      {
         TransportConfiguration live = decode(buffer);
         boolean hasBackup = buffer.readBoolean();
         TransportConfiguration backup = null;
         if (hasBackup)
         {
            backup = decode(buffer);
         }
         configs.add(new Pair<TransportConfiguration, TransportConfiguration>(live, backup));
      }

      return configs;
   }

   public static TransportConfiguration decode(HornetQBuffer buffer)
   {
      String name = BufferHelper.readNullableSimpleStringAsString(buffer);
      String factoryClassName = buffer.readSimpleString().toString();
      int paramSize = buffer.readInt();
      Map<String, Object> params = new HashMap<String, Object>();
      for (int i = 0; i < paramSize; i++)
      {
         String key = buffer.readSimpleString().toString();
         String value = buffer.readSimpleString().toString();
         params.put(key, value);
      }
      TransportConfiguration config = new TransportConfiguration(factoryClassName, params, name);
      return config;
   }

   public static void encodeConfigs(HornetQBuffer buffer,
                                    List<Pair<TransportConfiguration, TransportConfiguration>> configs)
   {
      buffer.writeInt(configs == null ? 0 : configs.size());
      if (configs != null)
      {
         for (Pair<TransportConfiguration, TransportConfiguration> pair : configs)
         {
            encode(buffer, pair.getA());
            boolean backup = (pair.getB() != null);
            buffer.writeBoolean(backup);
            if (backup)
            {
               encode(buffer, pair.getB());
            }
         }
      }
   }

   public static void encode(HornetQBuffer buffer, TransportConfiguration config)
   {
      BufferHelper.writeAsNullableSimpleString(buffer, config.getName());
      BufferHelper.writeAsSimpleString(buffer, config.getFactoryClassName());
      buffer.writeInt(config.getParams().size());
      for (Entry<String, Object> param : config.getParams().entrySet())
      {
         BufferHelper.writeAsSimpleString(buffer, param.getKey());
         BufferHelper.writeAsSimpleString(buffer, param.getValue().toString());
      }
   }

   public static int getEncodeSize(TransportConfiguration config)
   {
      int size = BufferHelper.sizeOfNullableSimpleString(config.getName()) +
                 BufferHelper.sizeOfSimpleString(config.getFactoryClassName());

      size += DataConstants.SIZE_INT; // number of params
      for (Entry<String, Object> param : config.getParams().entrySet())
      {
         size += BufferHelper.sizeOfSimpleString(param.getKey());
         size += BufferHelper.sizeOfSimpleString(param.getValue().toString());
      }
      return size;
   }

   public static int getEncodeSize(List<Pair<TransportConfiguration, TransportConfiguration>> configs)
   {
      int size = DataConstants.SIZE_INT; // number of configs;
      if (configs != null)
      {
         for (Pair<TransportConfiguration, TransportConfiguration> pair : configs)
         {
            size += getEncodeSize(pair.getA());
            size += DataConstants.SIZE_BOOLEAN; // whether there is a backup config
            if (pair.getB() != null)
            {
               size += getEncodeSize(pair.getB());
            }
         }
      }
      return size;
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
