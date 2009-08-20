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

package org.hornetq.core.remoting.impl.wireformat.replication;

import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A ReplicateRemoteBindingRemovedMessage
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Mar 2009 18:36:30
 *
 *
 */
public class ReplicateRemoteBindingRemovedMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString uniqueName;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicateRemoteBindingRemovedMessage(final SimpleString uniqueName)
   {
      super(REPLICATE_REMOVE_REMOTE_QUEUE_BINDING);

      this.uniqueName = uniqueName;
   }

   // Public --------------------------------------------------------

   public ReplicateRemoteBindingRemovedMessage()
   {
      super(REPLICATE_REMOVE_REMOTE_QUEUE_BINDING);
   }

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + uniqueName.sizeof();
   }

   @Override
   public void encodeBody(final HornetQBuffer buffer)
   {
      buffer.writeSimpleString(uniqueName);
   }

   @Override
   public void decodeBody(final HornetQBuffer buffer)
   {
      uniqueName = buffer.readSimpleString();
   }

   public SimpleString getUniqueName()
   {
      return uniqueName;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
