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
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>

 * @version <tt>$Revision$</tt>
 */
public class CreateQueueMessage extends PacketImpl
{

   private SimpleString address;

   private SimpleString queueName;

   private SimpleString filterString;

   private boolean durable;

   private boolean temporary;

   private boolean requiresResponse;

   public CreateQueueMessage(final SimpleString address,
                             final SimpleString queueName,
                             final SimpleString filterString,
                             final boolean durable,
                             final boolean temporary,
                             final boolean requiresResponse)
   {
      this();

      this.address = address;
      this.queueName = queueName;
      this.filterString = filterString;
      this.durable = durable;
      this.temporary = temporary;
      this.requiresResponse = requiresResponse;
   }

   public CreateQueueMessage()
   {
      super(PacketImpl.CREATE_QUEUE);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", address=" + address);
      buff.append(", queueName=" + queueName);
      buff.append(", filterString=" + filterString);
      buff.append(", durable=" + durable);
      buff.append(", temporary=" + temporary);
      buff.append("]");
      return buff.toString();
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }

   public SimpleString getFilterString()
   {
      return filterString;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public boolean isTemporary()
   {
      return temporary;
   }

   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeSimpleString(address);
      buffer.writeSimpleString(queueName);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeBoolean(durable);
      buffer.writeBoolean(temporary);
      buffer.writeBoolean(requiresResponse);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      address = buffer.readSimpleString();
      queueName = buffer.readSimpleString();
      filterString = buffer.readNullableSimpleString();
      durable = buffer.readBoolean();
      temporary = buffer.readBoolean();
      requiresResponse = buffer.readBoolean();
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof CreateQueueMessage == false)
      {
         return false;
      }

      CreateQueueMessage r = (CreateQueueMessage)other;

      return super.equals(other) && r.address.equals(address) &&
             r.queueName.equals(queueName) &&
             (r.filterString == null ? filterString == null : r.filterString.equals(filterString)) &&
             r.durable == durable &&
             r.temporary == temporary;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
