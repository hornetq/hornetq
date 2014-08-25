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

package org.hornetq.core.server.cluster;

import org.hornetq.core.protocol.ServerPacketDecoder;
import org.hornetq.core.protocol.core.impl.HornetQClientProtocolManager;
import org.hornetq.core.protocol.core.impl.PacketDecoder;
import org.hornetq.spi.core.remoting.ClientProtocolManager;
import org.hornetq.spi.core.remoting.ClientProtocolManagerFactory;

/**
 * A protocol manager that will replace the packet manager for inter-server communications
 * @author Clebert Suconic
 */
public class HornetQServerSideProtocolManagerFactory implements ClientProtocolManagerFactory
{
   @Override
   public ClientProtocolManager newProtocolManager()
   {
      return new HornetQReplicationProtocolManager();
   }

   class HornetQReplicationProtocolManager extends HornetQClientProtocolManager
   {
      @Override
      protected PacketDecoder getPacketDecoder()
      {
         return ServerPacketDecoder.INSTANCE;
      }
   }
}