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
import org.hornetq.core.protocol.core.PacketImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert Suconic</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionSendLargeMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** Used only if largeMessage */
   private byte[] largeMessageHeader;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendLargeMessage(final byte[] largeMessageHeader)
   {
      super(PacketImpl.SESS_SEND_LARGE);

      this.largeMessageHeader = largeMessageHeader;
   }

   public SessionSendLargeMessage()
   {
      super(PacketImpl.SESS_SEND_LARGE);
   }

   // Public --------------------------------------------------------

   public byte[] getLargeMessageHeader()
   {
      return largeMessageHeader;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(largeMessageHeader.length);
      buffer.writeBytes(largeMessageHeader);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      int largeMessageLength = buffer.readInt();

      largeMessageHeader = new byte[largeMessageLength];

      buffer.readBytes(largeMessageHeader);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
