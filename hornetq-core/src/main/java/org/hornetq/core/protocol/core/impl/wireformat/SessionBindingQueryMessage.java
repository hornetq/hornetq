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
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 *
 * A SessionQueueQueryMessage
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionBindingQueryMessage extends PacketImpl
{
   private SimpleString address;

   public SessionBindingQueryMessage(final SimpleString address)
   {
      super(PacketImpl.SESS_BINDINGQUERY);

      this.address = address;
   }

   public SessionBindingQueryMessage()
   {
      super(PacketImpl.SESS_BINDINGQUERY);
   }

   public SimpleString getAddress()
   {
      return address;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeSimpleString(address);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      address = buffer.readSimpleString();
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((address == null) ? 0 : address.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionBindingQueryMessage))
         return false;
      SessionBindingQueryMessage other = (SessionBindingQueryMessage)obj;
      if (address == null)
      {
         if (other.address != null)
            return false;
      }
      else if (!address.equals(other.address))
         return false;
      return true;
   }
}
