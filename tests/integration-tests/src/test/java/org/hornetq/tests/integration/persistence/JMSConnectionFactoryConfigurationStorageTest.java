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
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.jms.persistence.config.PersistedConnectionFactory;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.api.core.Pair;

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
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      mapExpectedCFs = new HashMap<String, PersistedConnectionFactory>();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      mapExpectedCFs = null;

      super.tearDown();
   }

   protected void addSetting(PersistedConnectionFactory setting) throws Exception
   {
      mapExpectedCFs.put(setting.getName(), setting);
      jmsJournal.storeConnectionFactory(setting);
   }

   @Test
   public void testSettings() throws Exception
   {

      createJMSStorage();

      List<String> transportConfigs = new ArrayList<String>();

      for (int i = 0 ; i < 5; i++)
      {
         transportConfigs.add("c1-" + i);
         transportConfigs.add("c2-" + i);
      }


      ConnectionFactoryConfiguration config = new ConnectionFactoryConfigurationImpl("some-name", false,  transportConfigs);

      addSetting(new PersistedConnectionFactory(config));

      jmsJournal.stop();

      createJMSStorage();

      List<PersistedConnectionFactory> cfs = jmsJournal.recoverConnectionFactories();

      assertEquals(1, cfs.size());

      assertEquals("some-name", cfs.get(0).getName());

      PersistedConnectionFactory cf1 = cfs.get(0);

      assertEquals(10, cf1.getConfig().getConnectorNames().size());

      List<String> configs = cf1.getConfig().getConnectorNames();
      for (int i = 0, j = 0; i < 10; i+=2, j++)
      {
         assertEquals(configs.get(i), "c1-" + j);
         assertEquals(configs.get(i+1), "c2-" + j);
      }
   }

   @Test
   public void testSizeOfCF() throws Exception
   {

      String str[] = new String[5];
      for (int i = 0; i < 5; i++)
      {
         str[i] = "str" + i;
      }

      ConnectionFactoryConfiguration config = new ConnectionFactoryConfigurationImpl("some-name", false, new ArrayList<String>(),  "");

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

   @Test
   public void testSettingsWithConnectorConfigs() throws Exception
   {

      createJMSStorage();

      String str[] = new String[5];
      for (int i = 0; i < 5; i++)
      {
         str[i] = "str" + i;
      }

      List<String> connectorConfigs = new ArrayList<String>();
      Map<String, Object> liveParams = new HashMap<String, Object>();
      liveParams.put(TransportConstants.PORT_PROP_NAME, 5665);
      Map<String, Object> backupParams = new HashMap<String, Object>();
      backupParams.put(TransportConstants.PORT_PROP_NAME, 5775);
      Map<String, Object> liveParams2 = new HashMap<String, Object>();
      liveParams2.put(TransportConstants.PORT_PROP_NAME, 6665);

      ConnectionFactoryConfiguration config = new ConnectionFactoryConfigurationImpl("some-name", false, connectorConfigs, str);
      config.setConnectorNames(connectorConfigs );
      List<Pair<String, String>> connectors = new ArrayList<Pair<String,String>>();
      connectors.add(new Pair<String, String>(RandomUtil.randomString(), null));
      //config.setConnectorNames(connectors);
      config.setCallTimeout(RandomUtil.randomPositiveLong());

      addSetting(new PersistedConnectionFactory(config));

      jmsJournal.stop();

      createJMSStorage();

      List<PersistedConnectionFactory> cfs = jmsJournal.recoverConnectionFactories();

      assertEquals(1, cfs.size());

      assertEquals("some-name", cfs.get(0).getName());

      assertEquals(config.getCallTimeout(), cfs.get(0).getConfig().getCallTimeout());
   }
}
