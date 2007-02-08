
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
import org.jboss.jms.server.endpoint.SessionEndpoint;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.message.MessageFactory;

/**
 * 
 * A SessionSendRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class SessionSendRequest extends RequestSupport
{
   private static final Logger log = Logger.getLogger(SessionSendRequest.class);   
   
   
   private JBossMessage msg;
   private boolean retry;
   
   public SessionSendRequest()
   {      
   }
   
   public SessionSendRequest(int objectId,
                             byte version,
                             JBossMessage msg,
                             boolean retry)
   {
      super(objectId, PacketSupport.REQ_SESSION_SEND, version);
      this.msg = msg;
      this.retry = retry;
   }

   public void read(DataInputStream is) throws Exception
   {
      super.read(is);
      
      byte messageType = is.readByte();
      
      msg = (JBossMessage)MessageFactory.createMessage(messageType);

      msg.read(is);

      retry = is.readBoolean();
   }

   public ResponseSupport serverInvoke() throws Exception
   {
      SessionEndpoint endpoint = 
         (SessionEndpoint)Dispatcher.instance.getTarget(objectId);
      
      if (endpoint == null)
      {
         throw new IllegalStateException("Cannot find object in dispatcher with id " + objectId);
      }

      endpoint.send(msg, retry);
      
      return null;
   }

   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);
      
      os.writeByte(msg.getType());
      
      msg.write(os);

      os.writeBoolean(retry);
      
      os.flush();
   }

}





