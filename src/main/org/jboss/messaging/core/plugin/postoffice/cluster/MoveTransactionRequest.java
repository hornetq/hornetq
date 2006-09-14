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
import org.jboss.messaging.util.StreamUtils;

/**
 * 
 * A MoveTransactionRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
class MoveTransactionRequest extends TransactionRequest
{
   static final int TYPE = 5;
   
   private String queueName;
   
   private List messages;
   
   MoveTransactionRequest()
   {      
   }
   
   MoveTransactionRequest(String nodeId, long txId, List messages, String queueName)
   {
      super(nodeId, txId, true);
      
      this.messages = messages;
      
      this.queueName = queueName;
   }
   
   MoveTransactionRequest(String nodeId, long txId)
   {
      super(nodeId, txId, false);
   }
   
   public void commit(PostOfficeInternal office) throws Exception
   {
      office.addToQueue(queueName, messages);  
   }
   
   public byte getType()
   {
      return TYPE;
   }

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);
      
      queueName = in.readUTF();
      
      int b = in.readByte();
      
      if (b == StreamUtils.NULL)
      {
         messages = null;
      }
      else
      {
         int size = in.readInt();
         
         messages = new ArrayList(size);
         
         for (int i = 0; i < size; i++)
         {
            byte type = in.readByte();
            Message msg = MessageFactory.createMessage(type);
            msg.read(in);
            messages.add(msg);
         }
      }
   }

   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);
      
      out.writeUTF(queueName);
      
      if (messages == null)
      {
         out.writeByte(StreamUtils.NULL);
      }
      else
      {
         out.writeByte(StreamUtils.LIST);
         
         out.writeInt(messages.size());
         
         Iterator iter = messages.iterator();
         while (iter.hasNext())
         {
            Message message = (Message)iter.next();
            out.writeByte(message.getType());      
            message.write(out);
         }
      }
      
      
   }
}


