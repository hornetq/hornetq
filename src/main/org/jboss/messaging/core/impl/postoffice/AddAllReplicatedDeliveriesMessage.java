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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>6 Jul 2007
 *
 * $Id: $
 *
 */
public class AddAllReplicatedDeliveriesMessage extends ClusterRequest
{
	private int nodeID;
	
	private Map deliveries;
	
	public AddAllReplicatedDeliveriesMessage()
	{		
	}
	
	public AddAllReplicatedDeliveriesMessage(int nodeID, Map deliveries)
	{
		this.nodeID = nodeID;
		
		this.deliveries = deliveries;		
	}
		
	Object execute(RequestTarget office) throws Throwable
	{		
		office.handleAddAllReplicatedDeliveries(nodeID, deliveries);
		
		return null;
	}

	byte getType()
	{
		return ClusterRequest.ADD_ALL_REPLICATED_DELIVERIES_REQUEST;
	}

	public void read(DataInputStream in) throws Exception
	{
		nodeID = in.readInt();
		
		int size = in.readInt();
		
		deliveries = new HashMap(size);
		
		for (int i = 0; i < size; i++)
		{
			String queueName = in.readUTF();
			
			int size2 = in.readInt();
			
			Map ids = new ConcurrentHashMap(size2);
			
			for (int j = 0; j < size2; j++)
			{
				long id = in.readLong();
				
				String sessionID = in.readUTF();
				
				ids.put(new Long(id), sessionID);
			}
			
			deliveries.put(queueName, ids);
		}
	}

	public void write(DataOutputStream out) throws Exception
	{		
		out.writeInt(nodeID);
		
		out.writeInt(deliveries.size());
		
		Iterator iter = deliveries.entrySet().iterator();
		
		while (iter.hasNext())
		{
			Map.Entry entry = (Map.Entry)iter.next();
			
			String queueName = (String)entry.getKey();
			
			out.writeUTF(queueName);
			
			Map ids = (Map)entry.getValue();
			
			out.writeInt(ids.size());
			
			Iterator iter2 = ids.entrySet().iterator();
			
			while (iter2.hasNext())
			{
				Map.Entry entry2 = (Map.Entry)iter2.next();
				
				Long id = (Long)entry2.getKey();
				
				String sessionID = (String)entry2.getValue();
				
				out.writeLong(id.longValue());
				
				out.writeUTF(sessionID);
			}
		}
	}
}
