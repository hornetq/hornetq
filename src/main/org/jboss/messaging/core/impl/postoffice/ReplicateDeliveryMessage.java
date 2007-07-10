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

import org.jgroups.Address;
import org.jgroups.stack.IpAddress;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>6 Jul 2007
 *
 * $Id: $
 *
 */
public class ReplicateDeliveryMessage extends ClusterRequest
{
	private int nodeID;
	
	private String queueName;
	
	private String sessionID;
	
	private long messageID;
	
	private long deliveryID;
	
	private Address returnAddress;
	
	public ReplicateDeliveryMessage()
	{		
	}
	
	public ReplicateDeliveryMessage(int nodeID, String queueName, String sessionID, long messageID, long deliveryID,
			                          Address returnAddress)
	{		
		this.nodeID = nodeID;
		
		this.queueName = queueName;
		
		this.sessionID = sessionID;
		
		this.messageID = messageID;
		
		this.deliveryID = deliveryID;
		
		this.returnAddress = returnAddress;
	}
		
	Object execute(RequestTarget office) throws Throwable
	{		
		office.handleReplicateDelivery(nodeID, queueName, sessionID, messageID, deliveryID, returnAddress);
		
		return null;
	}

	byte getType()
	{
		return ClusterRequest.REPLICATE_DELIVERY_REQUEST;
	}

	public void read(DataInputStream in) throws Exception
	{		
		nodeID = in.readInt();
		
		queueName = in.readUTF();
		
		sessionID = in.readUTF();
		
		messageID = in.readLong();
		
		deliveryID = in.readLong();
		
		byte b = in.readByte();
		
		if (b != NULL)
		{
			returnAddress = new IpAddress();
			
			returnAddress.readFrom(in);
		}
	}

	public void write(DataOutputStream out) throws Exception
	{
		out.writeInt(nodeID);
		
		out.writeUTF(queueName);
		
		out.writeUTF(sessionID);
		
		out.writeLong(messageID);
		
		out.writeLong(deliveryID);
		
		if (returnAddress == null)
		{
			out.writeByte(NULL);
		}
		else
		{
			out.writeByte(NOT_NULL);
			
			if (!(returnAddress instanceof IpAddress))
	      {
	         throw new IllegalStateException("Address must be IpAddress");
	      }

	      returnAddress.writeTo(out);
		}				
	}

}
