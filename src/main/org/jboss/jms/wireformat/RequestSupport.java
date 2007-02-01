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

import org.jboss.jms.server.ServerPeer;
import org.jboss.remoting.InvocationRequest;

/**
 * 
 * A PacketSupport
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public abstract class RequestSupport extends PacketSupport
{    
   protected int objectId;
  
   protected byte version;
   
   public RequestSupport()
   {      
   }  
   
   public RequestSupport(int objectId, int methodId, byte version)
   {
      super(methodId);
      
      this.objectId = objectId;
            
      this.version = version;
   }
   
   public Object getPayload()
   {
      //Wrap this in an InvocationRequest
         
      InvocationRequest req = new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
                                                    this, null, null, null);
      
      return req;
   }
   
   public int getMethodId()
   {
      return methodId;
   }

   public int getObjectId()
   {
      return objectId;
   }

   public byte getVersion()
   {
      return version;
   }
         
   public abstract ResponseSupport serverInvoke() throws Exception;
      
   public void write(DataOutputStream os) throws Exception
   {      
      super.write(os);
      
      os.writeByte(version);

      os.writeInt(objectId);
   }
   
   public void read(DataInputStream is) throws Exception
   {     
      version = is.readByte();
      
      objectId = is.readInt();
   }
   
}
