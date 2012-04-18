/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.tests.integration.clientcrash;

import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.IntegrationTestLogger;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class DummyInterceptorB implements Interceptor
{

   protected IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   static AtomicInteger syncCounter = new AtomicInteger(0);

   public static int getCounter()
   {
      return DummyInterceptorB.syncCounter.get();
   }

   public static void clearCounter()
   {
      DummyInterceptorB.syncCounter.set(0);
   }

   public boolean intercept(final Packet packet, final RemotingConnection conn) throws HornetQException
   {
      DummyInterceptorB.syncCounter.addAndGet(1);
      log.debug("DummyFilter packet = " + packet);
      return true;
   }
}
