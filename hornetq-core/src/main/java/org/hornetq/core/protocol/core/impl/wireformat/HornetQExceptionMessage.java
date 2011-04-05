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
import org.hornetq.api.core.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class HornetQExceptionMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(HornetQExceptionMessage.class);

   // Attributes ----------------------------------------------------

   private HornetQException exception;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public HornetQExceptionMessage(final HornetQException exception)
   {
      super(PacketImpl.EXCEPTION);

      this.exception = exception;
   }

   public HornetQExceptionMessage()
   {
      super(PacketImpl.EXCEPTION);
   }

   // Public --------------------------------------------------------

   @Override
   public boolean isResponse()
   {
      return true;
   }

   public HornetQException getException()
   {
      return exception;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(exception.getCode());
      buffer.writeNullableString(exception.getMessage());
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      int code = buffer.readInt();
      String msg = buffer.readNullableString();

      exception = new HornetQException(code, msg);
   }

   @Override
   public String toString()
   {
      return getParentString() + ", exception= " + exception + "]";
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof HornetQExceptionMessage == false)
      {
         return false;
      }

      HornetQExceptionMessage r = (HornetQExceptionMessage)other;

      return super.equals(other) && exception.equals(r.exception);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
