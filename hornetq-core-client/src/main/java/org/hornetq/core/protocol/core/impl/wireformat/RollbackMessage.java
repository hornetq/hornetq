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
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
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

   @Override
   public boolean isAsyncExec()
   {
      return true;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (considerLastMessageAsDelivered ? 1231 : 1237);
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof RollbackMessage))
         return false;
      RollbackMessage other = (RollbackMessage)obj;
      if (considerLastMessageAsDelivered != other.considerLastMessageAsDelivered)
         return false;
      return true;
   }
}
