/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.tests.unit.core.config.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.ConnectorServiceConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.server.ConnectorService;
import org.hornetq.core.server.ConnectorServiceFactory;
import org.hornetq.core.server.impl.ConnectorsService;
import org.hornetq.core.server.impl.InjectedObjectRegistry;
import org.hornetq.tests.unit.core.config.impl.fakes.FakeConnectorService;
import org.hornetq.tests.unit.core.config.impl.fakes.FakeConnectorServiceFactory;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class ConnectorsServiceTest extends UnitTestCase
{
   private Configuration configuration;

   private ConnectorServiceConfiguration connectorServiceConfiguration;

   private InjectedObjectRegistry injectedObjectRegistry;

   private ConnectorService connectorService;

   @Before
   public void setUp() throws Exception
   {
      // Setup Configuration
      connectorServiceConfiguration = new ConnectorServiceConfiguration(FakeConnectorServiceFactory.class.getCanonicalName(),
                                                                        new HashMap<String, Object>(), null);
      List<ConnectorServiceConfiguration> connectorServiceConfigurations = new ArrayList<ConnectorServiceConfiguration>();
      connectorServiceConfigurations.add(connectorServiceConfiguration);
      configuration = new ConfigurationImpl();
      configuration.setConnectorServiceConfigurations(connectorServiceConfigurations);

      connectorService = new FakeConnectorService();
      injectedObjectRegistry = new InjectedObjectRegistry();
   }

   @Test
   public void testConnectorsServiceUsesInjectedConnectorServiceFactory() throws Exception
   {
      ConnectorService connectorService = new FakeConnectorService();
      ConnectorServiceFactory connectorServiceFactory = new FakeConnectorServiceFactory(connectorService);
      injectedObjectRegistry.addConnectorServiceFactory(connectorServiceFactory);
      ConnectorsService connectorsService = new ConnectorsService(configuration, null, null, null, injectedObjectRegistry);
      connectorsService.start();

      assertTrue(connectorsService.getConnectors().size() == 1);
      assertTrue(connectorsService.getConnectors().contains(connectorService));
   }

   @Test
   public void testConnectorServiceCreatesNewConnectorServiceFactoryWhenNoInjectedOnesExists() throws Exception
   {
      ConnectorsService connectorsService = new ConnectorsService(configuration, null, null, null, injectedObjectRegistry);
      connectorsService.start();

      assertTrue(connectorsService.getConnectors().size() == 1);
      assertFalse(connectorsService.getConnectors().contains(connectorService));
   }

}
