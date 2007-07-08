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

import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.delegate.ConnectionEndpoint;

/**
 * 
 * A ConnectionCreateSessionDelegateRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ConnectionCreateSessionDelegateRequest extends RequestSupport
{
   private boolean transacted;
   
   private int acknowledgmentMode;
   
   private boolean xa;
   
   public ConnectionCreateSessionDelegateRequest()
   {      
   }
   
   public ConnectionCreateSessionDelegateRequest(String objectId,
                                                 byte version,
                                                 boolean transacted, int ackMode,
                                                 boolean xa)
   {
      super(objectId, PacketSupport.REQ_CONNECTION_CREATESESSIONDELEGATE, version);
      
      this.transacted = transacted;
      
      this.acknowledgmentMode = ackMode;
      
      this.xa = xa;
   }

   public void read(DataInputStream is) throws Exception
   {
      super.read(is);
      
      transacted = is.readBoolean();
      
      acknowledgmentMode = is.readInt();
      
      xa = is.readBoolean();
   }

   public ResponseSupport serverInvoke() throws Exception
   {
      ConnectionEndpoint endpoint = 
         (ConnectionEndpoint)Dispatcher.instance.getTarget(objectId);
      
      if (endpoint == null)
      {
         throw new IllegalStateException("Cannot find object in dispatcher with id " + objectId);
      }
      
      return new ConnectionCreateSessionDelegateResponse((ClientSessionDelegate)endpoint.createSessionDelegate(transacted, acknowledgmentMode, xa));         
   }

   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);
      
      //Write the args
           
      os.writeBoolean(transacted);
      
      os.writeInt(acknowledgmentMode);
      
      os.writeBoolean(xa);
      
      os.flush();
   }

}

