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
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>

 * @version <tt>$Revision$</tt>
 */
public class SessionDeleteQueueMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString queueName;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionDeleteQueueMessage(final SimpleString queueName)
   {
      super(DELETE_QUEUE);

      this.queueName = queueName;
   }

   public SessionDeleteQueueMessage()
   {
      super(DELETE_QUEUE);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", queueName=" + queueName);
      buff.append("]");
      return buff.toString();
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }

   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeSimpleString(queueName);
   }

   public void decodeRest(final HornetQBuffer buffer)
   {
      queueName = buffer.readSimpleString();
   }

   public boolean equals(Object other)
   {
      if (other instanceof SessionDeleteQueueMessage == false)
      {
         return false;
      }

      SessionDeleteQueueMessage r = (SessionDeleteQueueMessage)other;

      return super.equals(other) && r.queueName.equals(this.queueName);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
