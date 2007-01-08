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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.message.MessageFactory;

/**
 * 
 * A PullMessageResultRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class PullMessageResultRequest extends ClusterRequest
{
   private static final Logger log = Logger.getLogger(PullMessageResultRequest.class);   
   
   public static final int TYPE = 2;
   
   private long holdingTxId;
   
   private String queueName;
   
   private Message message;
   
   private int remoteNodeId;
   
   PullMessageResultRequest()
   {
   }
   
   PullMessageResultRequest(int remoteNodeId, long holdingTxId, String queueName, Message message)
   {
      this.remoteNodeId = remoteNodeId;
      
      this.holdingTxId = holdingTxId;
      
      this.queueName = queueName;
      
      this.message = message;
   }
   
   Message getMessage()
   {
      return message;
   }
   
   public void read(DataInputStream in) throws Exception
   {
      remoteNodeId = in.readInt();
      
      holdingTxId = in.readLong();
      
      queueName = in.readUTF();
      
      byte type = in.readByte();
      
      message = MessageFactory.createMessage(type);
      
      message.read(in);  
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(remoteNodeId);
      
      out.writeLong(holdingTxId);
      
      out.writeUTF(queueName);
      
      out.writeByte(message.getType());
      
      message.write(out);     
   }

   Object execute(PostOfficeInternal office) throws Throwable
   {
      office.handleMessagePullResult(remoteNodeId, holdingTxId, queueName, message);
      
      return null;
   }

   byte getType()
   {
      return TYPE;
   }
}
