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

import org.jboss.jms.delegate.ConnectionEndpoint;

public class ConnectionGetIDBlockRequest extends RequestSupport
{
   private int size;
   
   public ConnectionGetIDBlockRequest()
   {      
   }

   public ConnectionGetIDBlockRequest(String objectId,
                                      byte version,
                                      int size)
   {
      super(objectId, PacketSupport.REQ_CONNECTIONFACTORY_GETIDBLOCK, version);
      
      this.size = size;
   }

   public void read(DataInputStream is) throws Exception
   {
      super.read(is);
      
      size = is.readInt();
   }

   public ResponseSupport serverInvoke() throws Exception
   {
      ConnectionEndpoint endpoint = 
         (ConnectionEndpoint)Dispatcher.instance.getTarget(objectId);
      
      if (endpoint == null)
      {
         throw new IllegalStateException("Cannot find object in dispatcher with id " + objectId);
      }
      return new ConnectionGetIDBlockResponse(endpoint.getIdBlock(size));           
   }

   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);
      
      os.writeInt(size);  
      
      os.flush();
   }

}

