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

package org.hornetq.tests.unit.core.remoting.server.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.server.impl.InjectedObjectRegistry;
import org.hornetq.tests.unit.core.remoting.server.impl.fake.FakeInterceptor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class RemotingServiceImplTest
{
   private InjectedObjectRegistry injectedObjectRegistry;

   private RemotingServiceImpl remotingService;

   private Configuration configuration;

   @Before
   public void setUp() throws Exception
   {
      injectedObjectRegistry = new InjectedObjectRegistry();
      configuration = new ConfigurationImpl();
      configuration.setAcceptorConfigurations(new HashSet<TransportConfiguration>());
      remotingService = new RemotingServiceImpl(null, configuration, null, null, null, null, null, injectedObjectRegistry);
   }

   /**
    * This test ensures that RemotingServiceImpl.getInterceptorImplementation returns an *existing* interceptor instance
    * if there is one available in the injectObjectRegistry that has not already been added to incoming
    * @throws Exception
    */
   @Test
   public void testGetInterceptorImplementationReturnsInjectedInterceptor() throws Exception
   {
      Method method = RemotingServiceImpl.class.getDeclaredMethod("getInterceptorImplementation", List.class, List.class, String.class);
      method.setAccessible(true);

      Interceptor incomingInterceptor = new FakeInterceptor();
      List<Interceptor> injectedInterceptors = new ArrayList<Interceptor>();
      injectedInterceptors.add(incomingInterceptor);

      Interceptor interceptor = (Interceptor) method.invoke(remotingService,
                                                            injectedInterceptors,
                                                            new ArrayList<Interceptor>(),
                                                            FakeInterceptor.class.getCanonicalName());
      assertTrue(interceptor == incomingInterceptor);
   }

   /**
    * This test ensures that RemotingServiceImpl.getInterceptorImplementation returns a *new* interceptor instance
    * if there is one in the injectObjectRegistry but it has already been added to incoming interceptors.
    * @throws Exception
    */
   @Test
   public void testGetInterceptorImplementationReturnsNewInterceptorWhenInjectedAlreadyExists() throws Exception
   {
      Method method = RemotingServiceImpl.class.getDeclaredMethod("getInterceptorImplementation", List.class, List.class, String.class);
      method.setAccessible(true);

      Interceptor incomingInterceptor = new FakeInterceptor();
      List<Interceptor> injectedInterceptors = new ArrayList<Interceptor>();
      injectedInterceptors.add(incomingInterceptor);

      Interceptor interceptor = (Interceptor) method.invoke(remotingService,
                                                            injectedInterceptors,
                                                            injectedInterceptors,
                                                            FakeInterceptor.class.getCanonicalName());
      assertNotNull(interceptor);
      assertFalse(interceptor == incomingInterceptor);
   }

   /**
    * This test ensures that RemotingServiceImpl.getInterceptorImplementation returns a *new* interceptor instance
    * if there is not one in the injectObjectRegistry.
    * @throws Exception
    */
   @Test
   public void testGetInterceptorImplementationReturnsNewInterceptorWhenInjectedDoesNotContainInstace() throws Exception
   {
      Method method = RemotingServiceImpl.class.getDeclaredMethod("getInterceptorImplementation", List.class, List.class, String.class);
      method.setAccessible(true);

      Interceptor interceptor = (Interceptor) method.invoke(remotingService,
                                                            new ArrayList<Interceptor>(),
                                                            new ArrayList<Interceptor>(),
                                                            FakeInterceptor.class.getCanonicalName());
      assertTrue(interceptor instanceof FakeInterceptor);
   }

   /**
    * This test ensures that setInterceptors sets all interceptors instances for each class name added in the config and
    * uses the instances provided by injectedObjectRegistry.
    */
   @Test
   public void testSetInterceptorsUsesProvidedInterceptorsFromInjectedObjectRegistry() throws Exception
   {
      // In order to test that the interceptors are properly set we need a handle on the interceptor list.
      Field incomingInterceptorsField = RemotingServiceImpl.class.getDeclaredField("incomingInterceptors");
      incomingInterceptorsField.setAccessible(true);

      Field outgoingInterceptorsField = RemotingServiceImpl.class.getDeclaredField("incomingInterceptors");
      outgoingInterceptorsField.setAccessible(true);

      outgoingInterceptorsField = RemotingServiceImpl.class.getDeclaredField("outgoingInterceptors");
      outgoingInterceptorsField.setAccessible(true);

      Method method = RemotingServiceImpl.class.getDeclaredMethod("setInterceptors", Configuration.class);
      method.setAccessible(true);

      List<String> interceptorClassNames = new ArrayList<String>();
      List<Interceptor> interceptors = new ArrayList<>();
      for (int i = 0; i < 5; i++)
      {
         interceptorClassNames.add(FakeInterceptor.class.getCanonicalName());
         Interceptor interceptor = new FakeInterceptor();
         interceptors.add(interceptor);
         injectedObjectRegistry.addIncomingInterceptor(interceptor);
         injectedObjectRegistry.addOutgoingInterceptor(interceptor);
      }

      configuration.setIncomingInterceptorClassNames(interceptorClassNames);
      configuration.setOutgoingInterceptorClassNames(interceptorClassNames);

      method.invoke(remotingService, configuration);
      assertEquals(interceptors, incomingInterceptorsField.get(remotingService));
      assertEquals(interceptors, outgoingInterceptorsField.get(remotingService));
   }

   /**
    * This test ensures that setInterceptors does not add the same instance of the interceptor available in the
    * injected object registrty more than once.
    */
   @Test
   public void testSetInterceptorsDoesNotAddInstanceFromInjectedObjectRegistryMoreThanOnce() throws Exception
   {
      // In order to test that the interceptors are properly set we need a handle on the interceptor list.
      Field incomingInterceptorsField = RemotingServiceImpl.class.getDeclaredField("incomingInterceptors");
      incomingInterceptorsField.setAccessible(true);

      Field outgoingInterceptorsField = RemotingServiceImpl.class.getDeclaredField("incomingInterceptors");
      outgoingInterceptorsField.setAccessible(true);

      Method method = RemotingServiceImpl.class.getDeclaredMethod("setInterceptors", Configuration.class);
      method.setAccessible(true);

      List<String> interceptorClassNames = new ArrayList<String>();
      List<Interceptor> interceptors = new ArrayList<>();
      Interceptor interceptor = new FakeInterceptor();
      for (int i = 0; i < 2; i++)
      {
         interceptorClassNames.add(FakeInterceptor.class.getCanonicalName());
         interceptors.add(interceptor);
         injectedObjectRegistry.addIncomingInterceptor(interceptor);
         injectedObjectRegistry.addOutgoingInterceptor(interceptor);
      }

      configuration.setIncomingInterceptorClassNames(interceptorClassNames);
      configuration.setOutgoingInterceptorClassNames(interceptorClassNames);

      method.invoke(remotingService, configuration);
      assertTrue(((List) incomingInterceptorsField.get(remotingService)).size() == 2);
      assertFalse(interceptors.equals(outgoingInterceptorsField.get(remotingService)));
   }
}
