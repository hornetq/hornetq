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

package org.hornetq.tests.integration.cluster.failover;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.NotConnectedException;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.tests.integration.IntegrationTestLogger;

/**
 * A NettyAsynchronousReattachTest
 *
 * @author clebertsuconic
 *
 *
 */
public class NettyAsynchronousReattachTest extends NettyAsynchronousFailoverTest
{

   private final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   @Override
   protected void crash(final ClientSession... sessions) throws Exception
   {
      for (ClientSession session : sessions)
      {
         log.debug("Crashing session " + session);
         ClientSessionInternal internalSession = (ClientSessionInternal) session;
         internalSession.getConnection().fail(new NotConnectedException("oops"));
      }
   }
}
