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

import org.hornetq.core.buffers.HornetQBuffer;
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
      
   private boolean reattached;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReattachSessionResponseMessage(final int lastReceivedCommandID, final boolean reattached)
   {
      super(REATTACH_SESSION_RESP);

      this.lastReceivedCommandID = lastReceivedCommandID;
      
      this.reattached = reattached;
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
   
   public boolean isReattached()
   {
      return reattached;
   }
   
   public void encodeRest(final HornetQBuffer buffer)
   { 
      buffer.writeInt(lastReceivedCommandID);
      buffer.writeBoolean(reattached);
   }
   
   public void decodeRest(final HornetQBuffer buffer)
   { 
      lastReceivedCommandID = buffer.readInt();
      reattached = buffer.readBoolean();
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

