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

package org.hornetq.core.protocol.core.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.core.PacketImpl;
import org.hornetq.core.server.QueueQueryResult;

/**
 * 
 * A SessionQueueQueryResponseMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionQueueQueryResponseMessage extends PacketImpl
{
   private SimpleString name;
   
   private boolean exists;

   private boolean durable;

   private int consumerCount;

   private int messageCount;

   private SimpleString filterString;

   private SimpleString address;
   
   private boolean temporary;

   public SessionQueueQueryResponseMessage(final QueueQueryResult result)
   {
      this(result.getName(), result.getAddress(), result.isDurable(), result.isTemporary(),
           result.getFilterString(), result.getConsumerCount(), result.getMessageCount(), result.isExists());
   }

   public SessionQueueQueryResponseMessage()
   {
      this(null, null, false, false, null, 0, 0, false);
   }

   private SessionQueueQueryResponseMessage(final SimpleString name,
                                            final SimpleString address,
                                            final boolean durable,
                                            final boolean temporary,
                                            final SimpleString filterString,
                                            final int consumerCount,
                                            final int messageCount,
                                            final boolean exists)
   {
      super(PacketImpl.SESS_QUEUEQUERY_RESP);

      this.durable = durable;
      
      this.temporary = temporary;

      this.consumerCount = consumerCount;

      this.messageCount = messageCount;

      this.filterString = filterString;

      this.address = address;
      
      this.name = name;

      this.exists = exists;
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

   public boolean isDurable()
   {
      return durable;
   }

   public int getConsumerCount()
   {
      return consumerCount;
   }

   public int getMessageCount()
   {
      return messageCount;
   }

   public SimpleString getFilterString()
   {
      return filterString;
   }

   public SimpleString getAddress()
   {
      return address;
   }
   
   public SimpleString getName()
   {
      return name;
   }
   
   public boolean isTemporary()
   {
      return temporary;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeBoolean(exists);
      buffer.writeBoolean(durable);
      buffer.writeBoolean(temporary);
      buffer.writeInt(consumerCount);
      buffer.writeInt(messageCount);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeNullableSimpleString(address);
      buffer.writeNullableSimpleString(name);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      exists = buffer.readBoolean();
      durable = buffer.readBoolean();
      temporary = buffer.readBoolean();
      consumerCount = buffer.readInt();
      messageCount = buffer.readInt();
      filterString = buffer.readNullableSimpleString();
      address = buffer.readNullableSimpleString();
      name = buffer.readNullableSimpleString();
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionQueueQueryResponseMessage == false)
      {
         return false;
      }

      SessionQueueQueryResponseMessage r = (SessionQueueQueryResponseMessage)other;

      return super.equals(other) && exists == r.exists &&
             durable == r.durable &&
             consumerCount == r.consumerCount &&
             messageCount == r.messageCount &&
             filterString == null ? r.filterString == null
                                 : filterString.equals(r.filterString) && address == null ? r.address == null
                                                                                         : address.equals(r.address);
   }

}
