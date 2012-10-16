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

package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SubscribeClusterTopologyUpdatesMessageV2 extends SubscribeClusterTopologyUpdatesMessage
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int clientVersion;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SubscribeClusterTopologyUpdatesMessageV2(final boolean clusterConnection, int clientVersion)
   {
      super(PacketImpl.SUBSCRIBE_TOPOLOGY_V2, clusterConnection);

      this.clientVersion = clientVersion;
   }

   public SubscribeClusterTopologyUpdatesMessageV2()
   {
      super(PacketImpl.SUBSCRIBE_TOPOLOGY_V2);
   }

   // Public --------------------------------------------------------



   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      super.encodeRest(buffer);
      buffer.writeInt(clientVersion);
   }

   /**
    * @return the clientVersion
    */
   public int getClientVersion()
   {
      return clientVersion;
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      super.decodeRest(buffer);
      clientVersion = buffer.readInt();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
