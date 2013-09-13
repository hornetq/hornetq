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

package org.hornetq.tests.integration.cluster.util;

import java.util.concurrent.CountDownLatch;

import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQServer;

/**
 * A TestServer
 * @author jmesnil
 */
public interface TestableServer extends HornetQComponent
{
   HornetQServer getServer();

   public void stop() throws Exception;

   public void setIdentity(String identity);

   public CountDownLatch crash(ClientSession... sessions) throws Exception;

   public CountDownLatch crash(boolean waitFailure, ClientSession... sessions) throws Exception;

   public boolean isActive();

   void addInterceptor(Interceptor interceptor);

   void removeInterceptor(Interceptor interceptor);
}
