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

package org.hornetq.core.remoting.impl.wireformat;

import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 */
public class SessionCreateConsumerMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString queueName;

   private SimpleString filterString;

   private boolean browseOnly;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateConsumerMessage(final SimpleString queueName,
                                       final SimpleString filterString,
                                       final boolean browseOnly)
   {
      super(SESS_CREATECONSUMER);

      this.queueName = queueName;
      this.filterString = filterString;
      this.browseOnly = browseOnly;
   }

   public SessionCreateConsumerMessage()
   {
      super(SESS_CREATECONSUMER);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", queueName=" + queueName);
      buff.append(", filterString=" + filterString);
      buff.append("]");
      return buff.toString();
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

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + queueName.sizeof() +
             SimpleString.sizeofNullableString(filterString) +
             DataConstants.SIZE_BOOLEAN;
   }

   @Override
   public void encodeBody(final HornetQBuffer buffer)
   {
      buffer.writeSimpleString(queueName);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeBoolean(browseOnly);
   }

   @Override
   public void decodeBody(final HornetQBuffer buffer)
   {
      queueName = buffer.readSimpleString();
      filterString = buffer.readNullableSimpleString();
      browseOnly = buffer.readBoolean();
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
