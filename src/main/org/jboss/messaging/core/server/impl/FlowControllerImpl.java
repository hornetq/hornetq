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
package org.jboss.messaging.core.server.impl;

import java.util.List;

import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.FlowController;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerProducer;

/**
 * 
 * A FlowControllerImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FlowControllerImpl implements FlowController
{
	private int tokenPot;
	
	private final int batchSize = 50;
	
	private final PostOffice postOffice;
	
	private final int limit;
	
	private ServerProducer producer;
	
	private volatile boolean waiting;
	
	private final String address;
	
	public FlowControllerImpl(String address, PostOffice postOffice, int limit) throws Exception
	{
		this.address = address;
		
		this.postOffice = postOffice;
		
		this.limit = limit;
		
		getTokens();
	}
	
	public void registerProducer(ServerProducer producer)
	{
		this.producer = producer;
	}
	
	private void getTokens() throws Exception
	{
		List<Binding> bindings = postOffice.getBindingsForAddress(address);
		
		int minAvailable = Integer.MAX_VALUE;
		
		for (Binding binding: bindings)
		{
			Queue queue = binding.getQueue();
			
			int available = limit - queue.getMessageCount();
			
			if (available < 0)
			{
				available = 0;
			}
			
			minAvailable = Math.min(available, minAvailable);			
		}
		
      tokenPot += minAvailable;		
	}
		
	public void messageAcknowledged() throws Exception
	{
		if (waiting)
		{
			getTokens();
			
			if (tokenPot >= batchSize)
			{
				tokenPot -= batchSize;
				
				waiting = false;
				
				producer.sendCredits(batchSize);
			}			
		}
	}
	
	public void checkTokens(ServerProducer producer) throws Exception
	{						
		if (tokenPot < batchSize)
		{
			if (!waiting)
			{
				//Try and get some more
				getTokens();
				
				if (tokenPot >= batchSize)
				{
					tokenPot -= batchSize;
					
					producer.sendCredits(batchSize);
				}
				else
				{
					waiting = true;
				}
			}
		}
		else
		{
			tokenPot -= batchSize;
			
			producer.sendCredits(batchSize);	
		}
	}
}
