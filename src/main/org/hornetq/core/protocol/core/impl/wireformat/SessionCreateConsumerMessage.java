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
 */
public class SessionCreateConsumerMessage extends PacketImpl
{

   private long id;

   private SimpleString queueName;

   private SimpleString filterString;

   private boolean browseOnly;

   private boolean requiresResponse;

   public SessionCreateConsumerMessage(final long id,
                                       final SimpleString queueName,
                                       final SimpleString filterString,
                                       final boolean browseOnly,
                                       final boolean requiresResponse)
   {
      super(PacketImpl.SESS_CREATECONSUMER);

      this.id = id;
      this.queueName = queueName;
      this.filterString = filterString;
      this.browseOnly = browseOnly;
      this.requiresResponse = requiresResponse;
   }

   public SessionCreateConsumerMessage()
   {
      super(PacketImpl.SESS_CREATECONSUMER);
   }

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", queueName=" + queueName);
      buff.append(", filterString=" + filterString);
      buff.append("]");
      return buff.toString();
   }

   public long getID()
   {
      return id;
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }

   public SimpleString getFilterString()
   {
      return filterString;
   }

   public boolean isBrowseOnly()
   {
      return browseOnly;
   }

   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }

   public void setQueueName(SimpleString queueName)
   {
      this.queueName = queueName;
   }

   public void setFilterString(SimpleString filterString)
   {
      this.filterString = filterString;
   }

   public void setBrowseOnly(boolean browseOnly)
   {
      this.browseOnly = browseOnly;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(id);
      buffer.writeSimpleString(queueName);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeBoolean(browseOnly);
      buffer.writeBoolean(requiresResponse);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      id = buffer.readLong();
      queueName = buffer.readSimpleString();
      filterString = buffer.readNullableSimpleString();
      browseOnly = buffer.readBoolean();
      requiresResponse = buffer.readBoolean();
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionCreateConsumerMessage == false)
      {
         return false;
      }

      SessionCreateConsumerMessage r = (SessionCreateConsumerMessage)other;

      return super.equals(other) && queueName.equals(r.queueName) && filterString == null ? r.filterString == null
                                                                                         : filterString.equals(r.filterString);
   }
}
