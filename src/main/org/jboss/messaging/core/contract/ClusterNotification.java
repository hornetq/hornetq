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

package org.jboss.messaging.core.contract;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>21 Jun 2007
 *
 * $Id: $
 *
 */
public class ClusterNotification
{
	public static final int TYPE_BIND = 0;
	
	public static final int TYPE_UNBIND = 1;
	
	public static final int TYPE_FAILOVER_START = 2;
	
	public static final int TYPE_FAILOVER_END = 3;
	
	public static final int TYPE_NODE_JOIN = 4;
	
	public static final int TYPE_NODE_LEAVE = 5;
	
	public static final int TYPE_REPLICATOR_PUT = 6;
	
	public static final int TYPE_REPLICATOR_REMOVE = 7;
		
	public int type;
	
	public int nodeID;
	
	public Object data;
	
	public ClusterNotification(int type, int nodeID, Object data)
	{
		this.type = type;
		
		this.nodeID = nodeID;
		
		this.data = data;
	}
	
	public String toString()
	{
		return "ClusterNotification:" + System.identityHashCode(this) + " type: " + type + " nodeID: " + nodeID + " data: " + data;
	}
}
