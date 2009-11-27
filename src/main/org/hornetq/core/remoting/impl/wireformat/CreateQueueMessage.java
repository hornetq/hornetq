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

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>

 * @version <tt>$Revision$</tt>
 */
public class CreateQueueMessage extends PacketImpl
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(CreateQueueMessage.class);


   // Attributes ----------------------------------------------------

   private SimpleString address;

   private SimpleString queueName;

   private SimpleString filterString;

   private boolean durable;

   private boolean temporary;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateQueueMessage(final SimpleString address,
                             final SimpleString queueName,
                             final SimpleString filterString,
                             final boolean durable,
                             final boolean temporary)
   {
      super(CREATE_QUEUE);

      this.address = address;
      this.queueName = queueName;
      this.filterString = filterString;
      this.durable = durable;
      this.temporary = temporary;
   }

   public CreateQueueMessage()
   {
      super(CREATE_QUEUE);
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
   
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeSimpleString(address);
      buffer.writeSimpleString(queueName);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeBoolean(durable);
      buffer.writeBoolean(temporary);
   }

   public void decodeRest(final HornetQBuffer buffer)
   {
      address = buffer.readSimpleString();
      queueName = buffer.readSimpleString();
      filterString = buffer.readNullableSimpleString();
      durable = buffer.readBoolean();
      temporary = buffer.readBoolean();
   }

   public boolean equals(Object other)
   {
      if (other instanceof CreateQueueMessage == false)
      {
         return false;
      }

      CreateQueueMessage r = (CreateQueueMessage)other;

      return super.equals(other) && r.address.equals(this.address) &&
             r.queueName.equals(this.queueName) &&
             (r.filterString == null ? this.filterString == null : r.filterString.equals(this.filterString)) &&
             r.durable == this.durable &&
             r.temporary == this.temporary;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
