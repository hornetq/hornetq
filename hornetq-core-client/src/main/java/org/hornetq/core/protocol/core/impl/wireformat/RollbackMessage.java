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

/**
 * A RollbackMessage
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * Created Feb 18, 2009 2:11:17 PM
 *
 *
 */
public class RollbackMessage extends PacketImpl
{

   public RollbackMessage()
   {
      super(SESS_ROLLBACK);
   }

   public RollbackMessage(final boolean considerLastMessageAsDelivered)
   {
      super(SESS_ROLLBACK);

      this.considerLastMessageAsDelivered = considerLastMessageAsDelivered;
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean considerLastMessageAsDelivered;

   /**
    * @return the considerLastMessageAsDelivered
    */
   public boolean isConsiderLastMessageAsDelivered()
   {
      return considerLastMessageAsDelivered;
   }

   /**
    * @param isLastMessageAsDelivered the considerLastMessageAsDelivered to set
    */
   public void setConsiderLastMessageAsDelivered(final boolean isLastMessageAsDelivered)
   {
      considerLastMessageAsDelivered = isLastMessageAsDelivered;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeBoolean(considerLastMessageAsDelivered);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      considerLastMessageAsDelivered = buffer.readBoolean();
   }

   public boolean isAsyncExec()
   {
      return true;
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
