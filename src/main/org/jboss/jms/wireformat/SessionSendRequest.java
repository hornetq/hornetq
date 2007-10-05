
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

import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.advised.SessionAdvised;
import org.jboss.messaging.core.impl.message.MessageFactory;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.invocation.OnewayInvocation;

/**
 * 
 * A SessionSendRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class SessionSendRequest extends RequestSupport
{
   
   private JBossMessage msg;
   private boolean checkForDuplicates;
   private long sequence;
   
   public SessionSendRequest()
   {      
   }
   
   public SessionSendRequest(String objectId,
                             byte version,
                             JBossMessage msg,
                             boolean checkForDuplicates,
                             long sequence)
   {
      super(objectId, PacketSupport.REQ_SESSION_SEND, version);
      this.msg = msg;
      this.checkForDuplicates = checkForDuplicates;
      this.sequence = sequence;
   }

   public void read(DataInputStream is) throws Exception
   {
      super.read(is);
      
      byte messageType = is.readByte();
      
      msg = (JBossMessage)MessageFactory.createMessage(messageType);

      msg.read(is);

      checkForDuplicates = is.readBoolean();
      
      sequence = is.readLong();
   }

   public ResponseSupport serverInvoke() throws Exception
   {
      SessionAdvised advised = 
         (SessionAdvised)Dispatcher.instance.getTarget(objectId);
      
      if (advised != null)
      {         
         advised.send(msg, checkForDuplicates, sequence);
      }
      else
      {
      	//Since NP messages are sent one way, there is a possibility the session has closed
      	//by the time the message arrives, so we ignore this
      }
      
      return null;
   }

   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);
      
      os.writeByte(msg.getType());
      
      msg.write(os);

      os.writeBoolean(checkForDuplicates);
      
      os.writeLong(sequence);
      
      os.flush();
   }
   
   public Object getPayload()
   {
   	if (sequence != -1)
   	{	   	
	   	OnewayInvocation oi = new OnewayInvocation(this);
	
	   	InvocationRequest request =
	   		new InvocationRequest(null, ServerPeer.REMOTING_JMS_SUBSYSTEM,
	   				oi, ONE_WAY_METADATA, null, null);
	
	   	return request;     
   	}
   	else
   	{
   		return super.getPayload();
   	}
   }

}





