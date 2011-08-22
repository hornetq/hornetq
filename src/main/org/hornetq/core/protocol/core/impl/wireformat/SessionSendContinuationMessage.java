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
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * A SessionSendContinuationMessage
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 4, 2008 12:25:14 PM
 *
 *
 */
public class SessionSendContinuationMessage extends SessionContinuationMessage
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean requiresResponse;
   
   // Used on confirmation handling
   private MessageInternal message;
   
   /**
    * to be sent on the last package
    */
   private long messageBodySize = -1;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendContinuationMessage()
   {
      super(PacketImpl.SESS_SEND_CONTINUATION);
   }

   /**
    * @param body
    * @param continues
    * @param requiresResponse
    */
   public SessionSendContinuationMessage(final MessageInternal message, final byte[] body, final boolean continues, final boolean requiresResponse)
   {
      super(PacketImpl.SESS_SEND_CONTINUATION, body, continues);
      this.requiresResponse = requiresResponse;
   }

   /**
    * @param body
    * @param continues
    * @param requiresResponse
    */
   public SessionSendContinuationMessage(final MessageInternal message, final byte[] body, final boolean continues, final boolean requiresResponse, final long messageBodySize)
   {
      this(message, body, continues, requiresResponse);
      this.messageBodySize = messageBodySize;
   }

   // Public --------------------------------------------------------

   /**
    * @return the requiresResponse
    */
   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }
   
   public long getMessageBodySize()
   {
      return messageBodySize;
   }
   

   /**
    * @return the message
    */
   public MessageInternal getMessage()
   {
      return message;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      super.encodeRest(buffer);
      if (!continues)
      {
         buffer.writeLong(messageBodySize);
      }
      buffer.writeBoolean(requiresResponse);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      super.decodeRest(buffer);
      if (!continues)
      {
         messageBodySize = buffer.readLong();
      }
      requiresResponse = buffer.readBoolean();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
