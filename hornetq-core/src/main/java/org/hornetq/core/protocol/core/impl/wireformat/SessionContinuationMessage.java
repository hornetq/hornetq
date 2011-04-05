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
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.utils.DataConstants;

/**
 * A SessionContinuationMessage
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 5, 2008 10:08:40 AM
 *
 *
 */
public abstract class SessionContinuationMessage extends PacketImpl
{

   // Constants -----------------------------------------------------

   public static final int SESSION_CONTINUATION_BASE_SIZE = PacketImpl.PACKET_HEADERS_SIZE + DataConstants.SIZE_INT +
                                                            DataConstants.SIZE_BOOLEAN;

   // Attributes ----------------------------------------------------

   protected byte[] body;

   protected boolean continues;
   

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionContinuationMessage(final byte type, final byte[] body, final boolean continues)
   {
      super(type);
      this.body = body;
      this.continues = continues;
   }

   public SessionContinuationMessage(final byte type)
   {
      super(type);
   }

   // Public --------------------------------------------------------

   /**
    * @return the body
    */
   public byte[] getBody()
   {
      return body;
   }

   /**
    * @return the continues
    */
   public boolean isContinues()
   {
      return continues;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(body.length);
      buffer.writeBytes(body);
      buffer.writeBoolean(continues);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      int size = buffer.readInt();
      body = new byte[size];
      buffer.readBytes(body);
      continues = buffer.readBoolean();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
