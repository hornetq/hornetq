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

package org.hornetq.tests.integration.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.jms.server.config.impl.TransportConfigurationEncodingSupport;
import org.hornetq.tests.util.RandomUtil;

import junit.framework.TestCase;

/**
 * A TransportConfigurationEncodingSupportTest
 *
 * @author jmesnil
 *
 *
 */
public class TransportConfigurationEncodingSupportTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testTransportConfiguration() throws Exception
   {
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PORT_PROP_NAME, 5665);
      params.put(TransportConstants.HOST_PROP_NAME, RandomUtil.randomString());
      TransportConfiguration config = new TransportConfiguration(NettyConnectorFactory.class.getName(), params);

      HornetQBuffer buffer = HornetQBuffers.fixedBuffer(TransportConfigurationEncodingSupport.getEncodeSize(config));
      TransportConfigurationEncodingSupport.encode(buffer, config);

      assertEquals(buffer.capacity(), buffer.writerIndex());
      buffer.readerIndex(0);

      TransportConfiguration decoded = TransportConfigurationEncodingSupport.decode(buffer);
      assertNotNull(decoded);

      assertEquals(config.getName(), decoded.getName());
      assertEquals(config.getFactoryClassName(), decoded.getFactoryClassName());
      assertEquals(config.getParams().size(), decoded.getParams().size());
      for (String key : config.getParams().keySet())
      {
         assertEquals(config.getParams().get(key).toString(), decoded.getParams().get(key).toString());
      }
   }

   public void testTransportConfigurations() throws Exception
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      Map<String, Object> liveParams = new HashMap<String, Object>();
      liveParams.put(TransportConstants.PORT_PROP_NAME, 5665);
      TransportConfiguration live1 = new TransportConfiguration(NettyConnectorFactory.class.getName(), liveParams);
      Map<String, Object> backupParams = new HashMap<String, Object>();
      backupParams.put(TransportConstants.PORT_PROP_NAME, 5775);
      TransportConfiguration backup1 = new TransportConfiguration(NettyConnectorFactory.class.getName(), backupParams);
      Map<String, Object> liveParams2 = new HashMap<String, Object>();
      liveParams2.put(TransportConstants.PORT_PROP_NAME, 6665);
      TransportConfiguration live2 = new TransportConfiguration(NettyConnectorFactory.class.getName(), liveParams2);

      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(live1, backup1));
      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(live2, null));

      HornetQBuffer buffer = HornetQBuffers.fixedBuffer(TransportConfigurationEncodingSupport.getEncodeSize(connectorConfigs));
      TransportConfigurationEncodingSupport.encodeConfigs(buffer, connectorConfigs);

      assertEquals(buffer.capacity(), buffer.writerIndex());
      buffer.readerIndex(0);

      List<Pair<TransportConfiguration, TransportConfiguration>> decodedConfigs = TransportConfigurationEncodingSupport.decodeConfigs(buffer);
      assertNotNull(decodedConfigs);
      assertEquals(2, decodedConfigs.size());

      assertEquivalent(connectorConfigs.get(0).a, decodedConfigs.get(0).a);
      assertEquivalent(connectorConfigs.get(0).b, decodedConfigs.get(0).b);
      assertEquivalent(connectorConfigs.get(1).a, decodedConfigs.get(1).a);
      assertNull(decodedConfigs.get(1).b);
   }

   // decoded TransportConfiguration have parameter values as String instead of primitive type
   private static void assertEquivalent(TransportConfiguration expected, TransportConfiguration actual)
   {
      assertEquals(expected.getFactoryClassName(), actual.getFactoryClassName());
      assertEquals(expected.getName(), actual.getName());
      assertEquals(expected.getParams().size(), actual.getParams().size());
      for (Map.Entry<String, Object> entry : expected.getParams().entrySet())
      {
         String key = entry.getKey();
         assertEquals(expected.getParams().get(key).toString(), actual.getParams().get(key).toString());
      }
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
