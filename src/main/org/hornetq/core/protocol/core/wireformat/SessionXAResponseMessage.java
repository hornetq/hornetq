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
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionXAResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean error;

   private int responseCode;

   private String message;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAResponseMessage(final boolean isError, final int responseCode, final String message)
   {
      super(PacketImpl.SESS_XA_RESP);

      error = isError;

      this.responseCode = responseCode;

      this.message = message;
   }

   public SessionXAResponseMessage()
   {
      super(PacketImpl.SESS_XA_RESP);
   }

   // Public --------------------------------------------------------

   @Override
   public boolean isResponse()
   {
      return true;
   }

   public boolean isError()
   {
      return error;
   }

   public int getResponseCode()
   {
      return responseCode;
   }

   public String getMessage()
   {
      return message;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeBoolean(error);
      buffer.writeInt(responseCode);
      buffer.writeNullableString(message);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      error = buffer.readBoolean();
      responseCode = buffer.readInt();
      message = buffer.readNullableString();
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionXAResponseMessage == false)
      {
         return false;
      }

      SessionXAResponseMessage r = (SessionXAResponseMessage)other;

      return super.equals(other) && error == r.error && responseCode == r.responseCode && message.equals(r.message);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
