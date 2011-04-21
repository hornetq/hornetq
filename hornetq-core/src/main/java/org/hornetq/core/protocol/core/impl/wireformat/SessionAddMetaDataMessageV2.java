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

package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * A SessionAddMetaDataMessage
 * 
 * This packet replaces {@link SessionAddMetaDataMessage}
 *
 * @author Clebert Suconic</a>
 *
 *
 */
public class SessionAddMetaDataMessageV2 extends PacketImpl
{
   private String key;
   private String data;
   /**
    * It won require confirmation during failover / reconnect
    */
   private boolean requiresConfirmation = true;

   public SessionAddMetaDataMessageV2()
   {
      super(PacketImpl.SESS_ADD_METADATA2);
   }
   
   public SessionAddMetaDataMessageV2(String k, String d)
   {
      this();
      key = k;
      data = d;
   }
   
   public SessionAddMetaDataMessageV2(String k, String d, boolean requiresConfirmation)
   {
      this();
      key = k;
      data = d;
      this.requiresConfirmation = requiresConfirmation;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeString(key);
      buffer.writeString(data);
      buffer.writeBoolean(requiresConfirmation);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      key = buffer.readString();
      data = buffer.readString();
      requiresConfirmation = buffer.readBoolean();
   }
   
   @Override
   public final boolean isRequiresConfirmations()
   {
      return requiresConfirmation;
   }

   public String getKey()
   {
      return key;
   }

   public String getData()
   {
      return data;
   }

}
