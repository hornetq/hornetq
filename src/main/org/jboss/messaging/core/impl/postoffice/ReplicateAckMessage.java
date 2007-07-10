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

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>6 Jul 2007
 *
 * $Id: $
 *
 */
public class ReplicateAckMessage extends ClusterRequest
{
	private int nodeID;
		
	private String queueName;
	
	private long messageID;
	
	public ReplicateAckMessage()
	{		
	}
	
	public ReplicateAckMessage(int nodeID, String queueName, long messageID)
	{
		this.nodeID = nodeID;
		
		this.queueName = queueName;
		
		this.messageID = messageID;	
	}
	
	Object execute(RequestTarget office) throws Throwable
	{
		office.handleReplicateAck(nodeID, queueName, messageID);
		
		return null;
	}

	byte getType()
	{
		return ClusterRequest.REPLICATE_ACK_REQUEST;
	}

	public void read(DataInputStream in) throws Exception
	{		
		nodeID = in.readInt();
		
		queueName = in.readUTF();
		
		messageID = in.readLong();
	}

	public void write(DataOutputStream out) throws Exception
	{		
		out.writeInt(nodeID);
		
		out.writeUTF(queueName);
		
		out.writeLong(messageID);
	}

}
