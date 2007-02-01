/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.wireformat;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.messaging.core.tx.MessagingXid;

/**
 * 
 * A ConnectionGetPreparedTransactionsResponse
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ConnectionGetPreparedTransactionsResponse extends ResponseSupport
{
   private MessagingXid[] xids;
   
   public ConnectionGetPreparedTransactionsResponse()
   {      
   }
   
   public ConnectionGetPreparedTransactionsResponse(MessagingXid[] xids)
   {
      super(PacketSupport.RESP_CONNECTION_GETPREPAREDTRANSACTIONS);
      
      this.xids = xids;
   }

   public Object getResponse()
   {
      return xids;
   }
   
   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);
      
      os.writeInt(xids.length);
      
      for (int i = 0; i < xids.length; i++)
      {
         xids[i].write(os);
      }
      
      os.flush();
   }
   
   public void read(DataInputStream is) throws Exception
   {
      int num = is.readInt();
      
      xids = new MessagingXid[num];
      
      for (int i = 0; i < num; i++)
      {
         xids[i] = new MessagingXid();
         
         xids[i].read(is);
      }
   }

}



