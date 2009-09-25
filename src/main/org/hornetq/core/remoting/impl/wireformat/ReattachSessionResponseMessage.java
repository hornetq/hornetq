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

package org.hornetq.core.remoting.impl.wireformat;

import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.DataConstants;

/**
 * 
 * A ReattachSessionResponseMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ReattachSessionResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int lastReceivedCommandID;
      
   private boolean sessionFound;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReattachSessionResponseMessage(final int lastReceivedCommandID, final boolean sessionFound)
   {
      super(REATTACH_SESSION_RESP);

      this.lastReceivedCommandID = lastReceivedCommandID;
      
      this.sessionFound = sessionFound;
   }
   
   public ReattachSessionResponseMessage()
   {
      super(REATTACH_SESSION_RESP);
   }

   // Public --------------------------------------------------------

   public int getLastReceivedCommandID()
   {
      return lastReceivedCommandID;
   }
   
   public boolean isSessionFound()
   {
      return sessionFound;
   }
   
   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + DataConstants.SIZE_INT + DataConstants.SIZE_BOOLEAN;
   }
   

   public void encodeBody(final HornetQBuffer buffer)
   { 
      buffer.writeInt(lastReceivedCommandID);
      buffer.writeBoolean(sessionFound);
   }
   
   public void decodeBody(final HornetQBuffer buffer)
   { 
      lastReceivedCommandID = buffer.readInt();
      sessionFound = buffer.readBoolean();
   }
   
   public boolean isResponse()
   {      
      return true;
   }

   public boolean equals(Object other)
   {
      if (other instanceof ReattachSessionResponseMessage == false)
      {
         return false;
      }
            
      ReattachSessionResponseMessage r = (ReattachSessionResponseMessage)other;
      
      return super.equals(other) && this.lastReceivedCommandID == r.lastReceivedCommandID;
   }
   
   public final boolean isRequiresConfirmations()
   {
      return false;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

