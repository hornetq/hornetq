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
 * A Ping
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Ping extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long connectionTTL;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public Ping(final long connectionTTL)
   {
      super(PING);

      this.connectionTTL = connectionTTL;
   }

   public Ping()
   {
      super(PING);
   }

   // Public --------------------------------------------------------

   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(connectionTTL);
   }

   public void decodeRest(final HornetQBuffer buffer)
   {
      connectionTTL = buffer.readLong();
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", connectionTTL=" + connectionTTL);
      buf.append("]");
      return buf.toString();
   }

   public boolean equals(Object other)
   {
      if (other instanceof Ping == false)
      {
         return false;
      }

      Ping r = (Ping)other;

      return super.equals(other) && this.connectionTTL == r.connectionTTL;
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
