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

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionXAGetInDoubtXidsResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private List<Xid> xids;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAGetInDoubtXidsResponseMessage(final List<Xid> xids)
   {
      super(SESS_XA_INDOUBT_XIDS_RESP);

      this.xids = xids;
   }

   public SessionXAGetInDoubtXidsResponseMessage()
   {
      super(SESS_XA_INDOUBT_XIDS_RESP);
   }

   // Public --------------------------------------------------------

   @Override
   public boolean isResponse()
   {
      return true;
   }

   public List<Xid> getXids()
   {
      return xids;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(xids.size());

      for (Xid xid : xids)
      {
         XidCodecSupport.encodeXid(xid, buffer);
      }
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      int len = buffer.readInt();
      xids = new ArrayList<Xid>(len);
      for (int i = 0; i < len; i++)
      {
         Xid xid = XidCodecSupport.decodeXid(buffer);

         xids.add(xid);
      }
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionXAGetInDoubtXidsResponseMessage == false)
      {
         return false;
      }

      SessionXAGetInDoubtXidsResponseMessage r = (SessionXAGetInDoubtXidsResponseMessage)other;

      if (super.equals(other))
      {
         if (xids.size() == r.xids.size())
         {
            for (int i = 0; i < xids.size(); i++)
            {
               if (!xids.get(i).equals(r.xids.get(i)))
               {
                  return false;
               }
            }
         }
      }
      else
      {
         return false;
      }
      return true;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
