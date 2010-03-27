/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
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
import org.hornetq.integration.transports.netty.NettyConnectorFactory;
import org.hornetq.integration.transports.netty.TransportConstants;
import org.hornetq.jms.persistence.config.PersistedConnectionFactory;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.hornetq.tests.util.RandomUtil;

/**
 * A JMSConnectionFactoryConfigurationStorageTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JMSConnectionFactoryConfigurationStorageTest extends StorageManagerTestBase
{

   private Map<String, PersistedConnectionFactory> mapExpectedCFs;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      mapExpectedCFs = new HashMap<String, PersistedConnectionFactory>();
   }

   @Override
   protected void tearDown() throws Exception
   {
      mapExpectedCFs = null;

      super.tearDown();
   }

   protected void addSetting(PersistedConnectionFactory setting) throws Exception
   {
      mapExpectedCFs.put(setting.getName(), setting);
      jmsJournal.storeConnectionFactory(setting);
   }

   public void testSettings() throws Exception
   {

      createJMSStorage();

      String str[] = new String[5];
      for (int i = 0; i < 5; i++)
      {
         str[i] = "str" + i;
      }

      ConnectionFactoryConfiguration config = new ConnectionFactoryConfigurationImpl("some-name", str);

      addSetting(new PersistedConnectionFactory(config));

      jmsJournal.stop();

      createJMSStorage();

      List<PersistedConnectionFactory> cfs = jmsJournal.recoverConnectionFactories();

      assertEquals(1, cfs.size());

      assertEquals("some-name", cfs.get(0).getName());

      assertEquals(5, cfs.get(0).getConfig().getBindings().length);

      for (int i = 0; i < 5; i++)
      {
         assertEquals("str" + i, cfs.get(0).getConfig().getBindings()[i]);
      }
   }

   public void testSizeOfCF() throws Exception
   {

      String str[] = new String[5];
      for (int i = 0; i < 5; i++)
      {
         str[i] = "str" + i;
      }

      ConnectionFactoryConfiguration config = new ConnectionFactoryConfigurationImpl("some-name", str);

      int size = config.getEncodeSize();

      HornetQBuffer buffer = HornetQBuffers.fixedBuffer(size);

      config.encode(buffer);

      assertEquals(size, buffer.writerIndex());
      
      PersistedConnectionFactory persistedCF = new PersistedConnectionFactory(config);
      
      size = persistedCF.getEncodeSize();

      buffer = HornetQBuffers.fixedBuffer(size);
      
      persistedCF.encode(buffer);
      
      assertEquals(size, buffer.writerIndex());
      
   }
   
   public void testSettingsWithConnectorConfigs() throws Exception
   {

      createJMSStorage();

      String str[] = new String[5];
      for (int i = 0; i < 5; i++)
      {
         str[i] = "str" + i;
      }

      ConnectionFactoryConfiguration config = new ConnectionFactoryConfigurationImpl("some-name", str);
      List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration,TransportConfiguration>>();
      Map<String, Object> liveParams = new HashMap<String, Object>();
      liveParams.put(TransportConstants.PORT_PROP_NAME, 5665);
      TransportConfiguration live1 = new TransportConfiguration(NettyConnectorFactory.class.getName(), liveParams );
      Map<String, Object> backupParams = new HashMap<String, Object>();
      backupParams.put(TransportConstants.PORT_PROP_NAME, 5775);
      TransportConfiguration backup1 = new TransportConfiguration(NettyConnectorFactory.class.getName(), backupParams);
      Map<String, Object> liveParams2 = new HashMap<String, Object>();
      liveParams2.put(TransportConstants.PORT_PROP_NAME, 6665);
      TransportConfiguration live2 = new TransportConfiguration(NettyConnectorFactory.class.getName(), liveParams2);
      
      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(live1, backup1));
      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(live2, null));

      config.setConnectorConfigs(connectorConfigs );
      List<Pair<String, String>> connectors = new ArrayList<Pair<String,String>>();
      connectors.add(new Pair<String, String>(RandomUtil.randomString(), null));
      config.setConnectorNames(connectors);
      config.setCallTimeout(RandomUtil.randomPositiveLong());
      
      addSetting(new PersistedConnectionFactory(config));

      jmsJournal.stop();

      createJMSStorage();

      List<PersistedConnectionFactory> cfs = jmsJournal.recoverConnectionFactories();

      assertEquals(1, cfs.size());

      assertEquals("some-name", cfs.get(0).getName());

      assertEquals(config.getCallTimeout(), cfs.get(0).getConfig().getCallTimeout());

      assertEquals(5, cfs.get(0).getConfig().getBindings().length);

      for (int i = 0; i < 5; i++)
      {
         assertEquals("str" + i, cfs.get(0).getConfig().getBindings()[i]);
      }
   }

   /**
    * @param journal
    * @throws Exception
    */
   private void checkSettings() throws Exception
   {
      List<PersistedConnectionFactory> listSetting = jmsJournal.recoverConnectionFactories();

      assertEquals(mapExpectedCFs.size(), listSetting.size());

      for (PersistedConnectionFactory el : listSetting)
      {
         PersistedConnectionFactory el2 = mapExpectedCFs.get(el.getName());

         assertEquals(el, el2);
      }
   }

}
