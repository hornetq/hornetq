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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.message.MessageFactory;

/**
 * 
 * A PullMessagesResultRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class PullMessagesResultRequest extends ClusterRequest
{
   public static final int TYPE = 2;
   
   private long holdingTxId;
   
   private String queueName;
   
   private List messages;
   
   private int remoteNodeId;
   
   PullMessagesResultRequest()
   {
   }
   
   PullMessagesResultRequest(int remoteNodeId, long holdingTxId, String queueName, int size)
   {
      this.remoteNodeId = remoteNodeId;
      
      this.holdingTxId = holdingTxId;
      
      this.queueName = queueName;
      
      messages = new ArrayList(size);
   }
   
   void addMessage(Message msg)
   {
      messages.add(msg);
   }
   
   List getMessages()
   {
      return messages;
   }
   
   public void read(DataInputStream in) throws Exception
   {
      remoteNodeId = in.readInt();
      
      holdingTxId = in.readLong();
      
      queueName = in.readUTF();
      
      int num = in.readInt();
      
      messages = new ArrayList(num);
      
      for (int i = 0; i < num; i++)
      {
         byte type = in.readByte();
         
         Message msg = MessageFactory.createMessage(type);
         
         msg.read(in);
         
         messages.add(msg);
      }
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(remoteNodeId);
      
      out.writeLong(holdingTxId);
      
      out.writeUTF(queueName);
      
      out.writeInt(messages.size());
      
      Iterator iter = messages.iterator();
      
      while (iter.hasNext())
      {
         Message msg = (Message)iter.next();
         
         out.writeByte(msg.getType());
         
         msg.write(out);
      }   
   }

   Object execute(PostOfficeInternal office) throws Throwable
   {
      office.handleMessagePullResult(remoteNodeId, holdingTxId, queueName, messages);
      
      return null;
   }

   byte getType()
   {
      return TYPE;
   }
}
