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
 * A ReattachSessionMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ReattachSessionMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String name;
   
   private int lastReceivedCommandID;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReattachSessionMessage(final String name, final int lastReceivedCommandID)
   {
      super(REATTACH_SESSION);

      this.name = name;
      
      this.lastReceivedCommandID = lastReceivedCommandID;
   }
   
   public ReattachSessionMessage()
   {
      super(REATTACH_SESSION);
   }

   // Public --------------------------------------------------------

   public String getName()
   {
      return name;
   }
   
   public int getLastReceivedCommandID()
   {
      return lastReceivedCommandID;
   }
   
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeString(name);
      buffer.writeInt(lastReceivedCommandID);
   }
   
   public void decodeRest(final HornetQBuffer buffer)
   {
      name = buffer.readString();
      lastReceivedCommandID = buffer.readInt();
   }

   public boolean equals(Object other)
   {
      if (other instanceof ReattachSessionMessage == false)
      {
         return false;
      }
            
      ReattachSessionMessage r = (ReattachSessionMessage)other;
      
      return super.equals(other) && this.name.equals(r.name);
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

