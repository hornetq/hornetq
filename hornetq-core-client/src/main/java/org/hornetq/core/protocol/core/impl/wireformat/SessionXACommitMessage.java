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

import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.utils.XidCodecSupport;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 */
public class SessionXACommitMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean onePhase;

   private Xid xid;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXACommitMessage(final Xid xid, final boolean onePhase)
   {
      super(SESS_XA_COMMIT);

      this.xid = xid;
      this.onePhase = onePhase;
   }

   public SessionXACommitMessage()
   {
      super(SESS_XA_COMMIT);
   }

   // Public --------------------------------------------------------

   public Xid getXid()
   {
      return xid;
   }

   public boolean isOnePhase()
   {
      return onePhase;
   }

   @Override
   public boolean isAsyncExec()
   {
      return true;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      XidCodecSupport.encodeXid(xid, buffer);
      buffer.writeBoolean(onePhase);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      xid = XidCodecSupport.decodeXid(buffer);
      onePhase = buffer.readBoolean();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", xid=" + xid + ", onePhase=" + onePhase + "]";
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionXACommitMessage == false)
      {
         return false;
      }

      SessionXACommitMessage r = (SessionXACommitMessage)other;

      return super.equals(other) && xid.equals(r.xid) && onePhase == r.onePhase;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
