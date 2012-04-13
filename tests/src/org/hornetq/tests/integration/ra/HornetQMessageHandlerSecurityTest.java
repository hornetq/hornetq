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
package org.hornetq.tests.integration.ra;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.hornetq.tests.util.UnitTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Jul 6, 2010
 */
public class HornetQMessageHandlerSecurityTest extends HornetQRATestBase
{
   @Override
   public boolean isSecure()
   {
      return true;
   }

   public void testSimpleMessageReceivedOnQueueWithSecurityFails() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(UnitTestCase.INVM_CONNECTOR_FACTORY);
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.setTransactionManagerLocatorMethod("");
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setUser("dodgyuser");
      spec.setPassword("dodgypassword");
      spec.setSetupAttempts(0);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      Binding binding = server.getPostOffice().getBinding(MDBQUEUEPREFIXEDSIMPLE);
      assertEquals(((LocalQueueBinding)binding).getQueue().getConsumerCount(), 0);
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();
   }

   public void testSimpleMessageReceivedOnQueueWithSecuritySucceeds() throws Exception
   {
      server.getSecurityManager().addUser("testuser", "testpassword");
      server.getSecurityManager().addRole("testuser", "arole");
      Role role = new Role("arole", false, true, false, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
       server.getSecurityRepository().addMatch(MDBQUEUEPREFIXED, roles);
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setTransactionManagerLocatorClass("");
      qResourceAdapter.setTransactionManagerLocatorMethod("");
      qResourceAdapter.setConnectorClassName(UnitTestCase.INVM_CONNECTOR_FACTORY);
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQActivationSpec spec = new HornetQActivationSpec();
      spec.setResourceAdapter(qResourceAdapter);
      spec.setUseJNDI(false);
      spec.setDestinationType("javax.jms.Queue");
      spec.setDestination(MDBQUEUE);
      spec.setUser("testuser");
      spec.setPassword("testpassword");
      spec.setSetupAttempts(0);
      qResourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageEndpoint endpoint = new DummyMessageEndpoint(latch);
      DummyMessageEndpointFactory endpointFactory = new DummyMessageEndpointFactory(endpoint, false);
      qResourceAdapter.endpointActivation(endpointFactory, spec);
      Binding binding = server.getPostOffice().getBinding(MDBQUEUEPREFIXEDSIMPLE);
      assertEquals(((LocalQueueBinding)binding).getQueue().getConsumerCount(), 15);
      qResourceAdapter.endpointDeactivation(endpointFactory, spec);
      qResourceAdapter.stop();
   }
}
