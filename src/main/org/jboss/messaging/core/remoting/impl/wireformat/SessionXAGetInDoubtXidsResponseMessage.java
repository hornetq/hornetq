/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.core.remoting.impl.wireformat;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;


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
  
   public boolean isResponse()
   {
      return true;
   }
   
   public List<Xid> getXids()
   {
      return xids;
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeInt(xids.size());

      for (Xid xid: xids)
      {
         XidCodecSupport.encodeXid(xid, buffer);
      }    
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      int len = buffer.readInt();
      xids = new ArrayList<Xid>(len);      
      for (int i = 0; i < len; i++)
      {
         Xid xid = XidCodecSupport.decodeXid(buffer);
         
         xids.add(xid);
      }      
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionXAGetInDoubtXidsResponseMessage == false)
      {
         return false;
      }
            
      SessionXAGetInDoubtXidsResponseMessage r = (SessionXAGetInDoubtXidsResponseMessage)other;
      
      if (super.equals(other))
      {
         if (this.xids.size() == r.xids.size())
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


