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

import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivation;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.hornetq.tests.unit.ra.MessageEndpointFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author Justin Bertram
 */
public class HornetQRASessionFailureListenerTest extends HornetQRATestBase
{
   public static final int MAX_SESSION = 15;

   public void testHornetQRASessionFailureListener() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();

      ra.setConnectorClassName("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
      ra.setTransactionManagerLocatorClass("");
      ra.setTransactionManagerLocatorMethod("");
      ra.start(new org.hornetq.tests.unit.ra.BootstrapContext());

      HornetQActivationSpec spec = new HornetQActivationSpec();

      spec.setResourceAdapter(ra);
      spec.setUseJNDI(false);
      spec.setUser("user");
      spec.setPassword("password");
      spec.setDestinationType("Queue");
      spec.setDestination(MDBQUEUE);
      spec.setMaxSession(MAX_SESSION);
      spec.setReconnectAttempts(0);
      spec.setSetupAttempts(100);
      spec.setSetupInterval(100);
      spec.setCallTimeout(200L);

      HornetQActivation activation = new HornetQActivation(ra, new MessageEndpointFactory(), spec);
      activation.start();

      Binding binding = server.getPostOffice().getBinding(MDBQUEUEPREFIXEDSIMPLE);
      assertEquals(MAX_SESSION, ((LocalQueueBinding) binding).getQueue().getConsumerCount());

      server.stop();
      server.start();

      // wait for the RA to reconnect fully; timeout after 10 seconds
      assertTrue(activation.getReconnectionLatch().await(10, TimeUnit.SECONDS));

      binding = server.getPostOffice().getBinding(MDBQUEUEPREFIXEDSIMPLE);
      assertEquals(MAX_SESSION, ((LocalQueueBinding) binding).getQueue().getConsumerCount());
      activation.stop();
      ra.stop();
   }

   @Override
   public boolean useSecurity()
   {
      return false;
   }

   @Override
   protected boolean usePersistence()
   {
      return true;
   }
}
