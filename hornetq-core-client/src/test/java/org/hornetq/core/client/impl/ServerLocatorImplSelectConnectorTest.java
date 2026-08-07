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
package org.hornetq.core.client.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.loadbalance.FirstElementConnectionLoadBalancingPolicy;
import org.junit.Assert;
import org.junit.Test;

public class ServerLocatorImplSelectConnectorTest extends Assert
{
   private static final String INVM_FACTORY = "org.hornetq.core.remoting.impl.invm.InVMConnectorFactory";

   @Test
   public void testFallbackToInitialConnectorsWhenTopologyEntryHasNullLive() throws Exception
   {
      TransportConfiguration fallback = createInVMConfig("fallback");

      ServerLocatorImpl locator = new ServerLocatorImpl(false, fallback);
      setLoadBalancingPolicy(locator);

      Pair<TransportConfiguration, TransportConfiguration>[] topology = new Pair[]{new Pair<TransportConfiguration, TransportConfiguration>(null, null)};
      setTopologyArray(locator, topology);

      TransportConfiguration result = callSelectConnector(locator);
      assertNotNull("Should fall back to initialConnectors when topology entry has null live connector", result);
      assertEquals(fallback, result);
   }

   @Test
   public void testReturnsNullWhenTopologyEntryHasNullLiveAndNoInitialConnectors() throws Exception
   {
      // Constructor requires non-null varargs; clear initialConnectors via reflection to simulate discovery-group config
      ServerLocatorImpl locator = new ServerLocatorImpl(false, new TransportConfiguration[0]);
      setLoadBalancingPolicy(locator);
      setField(locator, "initialConnectors", null);

      Pair<TransportConfiguration, TransportConfiguration>[] topology = new Pair[]{new Pair<TransportConfiguration, TransportConfiguration>(null, null)};
      setTopologyArray(locator, topology);

      TransportConfiguration result = callSelectConnector(locator);
      assertNull("Should return null when no fallback connectors are available", result);
   }

   @Test
   public void testReturnsTopologyLiveConnectorWhenPresent() throws Exception
   {
      TransportConfiguration live = createInVMConfig("live");

      ServerLocatorImpl locator = new ServerLocatorImpl(false, createInVMConfig("initial"));
      setLoadBalancingPolicy(locator);

      Pair<TransportConfiguration, TransportConfiguration>[] topology = new Pair[]{new Pair<TransportConfiguration, TransportConfiguration>(live, null)};
      setTopologyArray(locator, topology);

      TransportConfiguration result = callSelectConnector(locator);
      assertEquals("Should return live connector from topology", live, result);
   }

   private static TransportConfiguration createInVMConfig(String name)
   {
      return new TransportConfiguration(INVM_FACTORY, new HashMap<String, Object>(), name);
   }

   private static void setLoadBalancingPolicy(ServerLocatorImpl locator) throws Exception
   {
      setField(locator, "loadBalancingPolicy", new FirstElementConnectionLoadBalancingPolicy());
   }

   private static void setTopologyArray(ServerLocatorImpl locator, Pair<TransportConfiguration, TransportConfiguration>[] array) throws Exception
   {
      setField(locator, "topologyArray", array);
   }

   private static void setField(ServerLocatorImpl locator, String fieldName, Object value) throws Exception
   {
      Field field = ServerLocatorImpl.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(locator, value);
   }

   private static TransportConfiguration callSelectConnector(ServerLocatorImpl locator) throws Exception
   {
      Method method = ServerLocatorImpl.class.getDeclaredMethod("selectConnector");
      method.setAccessible(true);
      return (TransportConfiguration) method.invoke(locator);
   }
}
