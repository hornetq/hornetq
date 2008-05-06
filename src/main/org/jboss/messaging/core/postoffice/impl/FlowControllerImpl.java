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
package org.jboss.messaging.core.postoffice.impl;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.FlowController;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerProducer;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A FlowControllerImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FlowControllerImpl implements FlowController
{
	private static final Logger log = Logger.getLogger(FlowControllerImpl.class);
	
	private int lastPot;
		
	private int tokenPot;
	
	private final PostOffice postOffice;
	
	private final SimpleString address;
	
	private final java.util.Queue<ServerProducer> waitingList = new ConcurrentLinkedQueue<ServerProducer>();
	
	public FlowControllerImpl(final SimpleString address, final PostOffice postOffice) throws Exception
	{
		this.address = address;
		
		this.postOffice = postOffice;
		
		fillPot();
	}
	
	public synchronized int getInitialTokens(final int windowSize, final ServerProducer producer)
	{
		int num = Math.min(windowSize, tokenPot);
		
		tokenPot -= num;
		
		if (num == 0)
		{
			//Register producer as a waiter or will never get any messages
			
			producer.setWaiting(true);
			
			waitingList.add(producer);
		}
		
		return num;
	}
				
	//FIXME - sort out the synchronization on this - don't want to lock the whole thing
	//also don't want to execute the whole method if already waiting
	public synchronized void messageAcknowledged() throws Exception
	{		
		fillPot();
			
		while (tokenPot > 0)
		{
			ServerProducer producer = waitingList.poll();
			
			if (producer == null)
			{
				break;
			}
			
			tokenPot--;
			
			producer.setWaiting(false);
			
			producer.sendCredits();
		}					
	}
		
	public synchronized void messageReceived(final ServerProducer producer, final int windowSize) throws Exception
	{		
		if (tokenPot == 0)
		{
			if (!producer.isWaiting())
			{
				producer.setWaiting(true);
				
				waitingList.add(producer);
			}
		}
		else
		{
			tokenPot--;
			
			producer.sendCredits();
		}
	}
			
	private void fillPot() throws Exception
	{
		List<Binding> bindings = postOffice.getBindingsForAddress(address);
		
		int minAvailable = Integer.MAX_VALUE;
		
		for (Binding binding: bindings)
		{
			Queue queue = binding.getQueue();
			
			int maxSize = queue.getMaxSize();
			
			int available;
			
			if (maxSize == -1)
			{
				available = Integer.MAX_VALUE;
			}
			else
			{
				available = maxSize - queue.getMessageCount();
			}
			
			if (available < 0)
			{
				available = 0;
			}
			
			minAvailable = Math.min(available, minAvailable);			
		}
						
		if (minAvailable > lastPot)
		{
			tokenPot += minAvailable - lastPot;
			
			lastPot = minAvailable;
		}
	}
}
