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

import java.util.ArrayList;
import java.util.List;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 *
 * A SessionBindingQueryResponseMessage
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionBindingQueryResponseMessage extends PacketImpl
{
   private boolean exists;

   private List<SimpleString> queueNames;

   public SessionBindingQueryResponseMessage(final boolean exists, final List<SimpleString> queueNames)
   {
      super(PacketImpl.SESS_BINDINGQUERY_RESP);

      this.exists = exists;

      this.queueNames = queueNames;
   }

   public SessionBindingQueryResponseMessage()
   {
      super(PacketImpl.SESS_BINDINGQUERY_RESP);
   }

   @Override
   public boolean isResponse()
   {
      return true;
   }

   public boolean isExists()
   {
      return exists;
   }

   public List<SimpleString> getQueueNames()
   {
      return queueNames;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeBoolean(exists);
      buffer.writeInt(queueNames.size());
      for (SimpleString queueName : queueNames)
      {
         buffer.writeSimpleString(queueName);
      }
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      exists = buffer.readBoolean();
      int numQueues = buffer.readInt();
      queueNames = new ArrayList<SimpleString>(numQueues);
      for (int i = 0; i < numQueues; i++)
      {
         queueNames.add(buffer.readSimpleString());
      }
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (exists ? 1231 : 1237);
      result = prime * result + ((queueNames == null) ? 0 : queueNames.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionBindingQueryResponseMessage))
         return false;
      SessionBindingQueryResponseMessage other = (SessionBindingQueryResponseMessage)obj;
      if (exists != other.exists)
         return false;
      if (queueNames == null)
      {
         if (other.queueNames != null)
            return false;
      }
      else if (!queueNames.equals(other.queueNames))
         return false;
      return true;
   }
}
