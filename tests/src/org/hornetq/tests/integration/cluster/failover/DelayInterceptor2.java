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

package org.hornetq.tests.integration.cluster.failover;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * A DelayInterceptor2
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class DelayInterceptor2 implements Interceptor
{
   private static final Logger log = Logger.getLogger(DelayInterceptor2.class);

   private volatile boolean loseResponse = true;

   public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
   {
      if (packet.getType() == PacketImpl.NULL_RESPONSE && loseResponse)
      {
         // Lose the response from the commit - only lose the first one

         loseResponse = false;

         return false;
      }
      else
      {
         return true;
      }
   }
}
