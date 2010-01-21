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
public class SessionXASetTimeoutMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int timeoutSeconds;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXASetTimeoutMessage(final int timeoutSeconds)
   {
      super(PacketImpl.SESS_XA_SET_TIMEOUT);

      this.timeoutSeconds = timeoutSeconds;
   }

   public SessionXASetTimeoutMessage()
   {
      super(PacketImpl.SESS_XA_SET_TIMEOUT);
   }

   // Public --------------------------------------------------------

   public int getTimeoutSeconds()
   {
      return timeoutSeconds;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(timeoutSeconds);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      timeoutSeconds = buffer.readInt();
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionXASetTimeoutMessage == false)
      {
         return false;
      }

      SessionXASetTimeoutMessage r = (SessionXASetTimeoutMessage)other;

      return super.equals(other) && timeoutSeconds == r.timeoutSeconds;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
