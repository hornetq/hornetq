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
package org.jboss.messaging.core.impl.postoffice;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.impl.message.MessageFactory;

/**
 * A MessageRequest
 * 
 * Used when sending a single message non reliably across the group
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2202 $</tt>
 *
 * $Id: MessageRequest.java 2202 2007-02-08 10:50:26Z timfox $
 *
 */
class MessageRequest extends ClusterRequest
{
   private String routingConditionText;   
   
   private Message message;
   
   private Set queueNames;
   
   MessageRequest()
   {      
   }
   
   MessageRequest(String routingConditionText, Message message, Set queueNames)
   {
      this.routingConditionText = routingConditionText;
      
      this.message = message;
      
      this.queueNames = queueNames;
   }
   
   Object execute(RequestTarget office) throws Exception
   {
      office.routeFromCluster(message, routingConditionText, queueNames);      
      
      return null;
   }  
   
   byte getType()
   {
      return ClusterRequest.MESSAGE_REQUEST;
   }
   
   public void read(DataInputStream in) throws Exception
   {
      routingConditionText = in.readUTF();
      
      byte type = in.readByte();
      
      message = MessageFactory.createMessage(type);
      
      message.read(in);  
      
      byte b = in.readByte();
      
      if (b != NULL)
      {
      	int size = in.readInt();
      	
      	queueNames = new HashSet(size);
      	
      	for (int i = 0; i < size; i++)
      	{
      		String queueName = in.readUTF();
      		
      		queueNames.add(queueName);
      	}
      }
   }
   
   public void write(DataOutputStream out) throws Exception
   {
      out.writeUTF(routingConditionText);
      
      out.writeByte(message.getType());      
      
      message.write(out);
      
      if (queueNames == null)
      {
      	out.writeByte(NULL);
      }
      else
      {
      	out.writeByte(NOT_NULL);
      	
      	out.writeInt(queueNames.size());
      	
      	Iterator iter = queueNames.iterator();
      	
      	while (iter.hasNext())
      	{
      		String queueName = (String)iter.next();
      		
      		out.writeUTF(queueName);
      	}      	     
      }
   }
}
