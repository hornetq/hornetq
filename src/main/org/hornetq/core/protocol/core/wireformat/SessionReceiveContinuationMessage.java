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
import org.hornetq.utils.DataConstants;

/**
 * A SessionSendContinuationMessage
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 4, 2008 12:25:14 PM
 *
 *
 */
public class SessionReceiveContinuationMessage extends SessionContinuationMessage
{

   // Constants -----------------------------------------------------

   public static final int SESSION_RECEIVE_CONTINUATION_BASE_SIZE = SessionContinuationMessage.SESSION_CONTINUATION_BASE_SIZE + DataConstants.SIZE_LONG;

   // Attributes ----------------------------------------------------

   private long consumerID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionReceiveContinuationMessage()
   {
      super(PacketImpl.SESS_RECEIVE_CONTINUATION);
   }

   /**
    * @param consumerID
    * @param body
    * @param continues
    * @param requiresResponse
    */
   public SessionReceiveContinuationMessage(final long consumerID,
                                            final byte[] body,
                                            final boolean continues,
                                            final boolean requiresResponse)
   {
      super(PacketImpl.SESS_RECEIVE_CONTINUATION, body, continues);
      this.consumerID = consumerID;
   }

   /**
    * @return the consumerID
    */
   public long getConsumerID()
   {
      return consumerID;
   }

   // Public --------------------------------------------------------

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      super.encodeRest(buffer);
      buffer.writeLong(consumerID);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      super.decodeRest(buffer);
      consumerID = buffer.readLong();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
