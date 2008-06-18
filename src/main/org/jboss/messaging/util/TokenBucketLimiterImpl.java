/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.util;

/**
 * 
 * A TokenBucketLimiterImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TokenBucketLimiterImpl implements TokenBucketLimiter
{
	private final int rate;
	
	private final boolean spin;
		
	private volatile long last;
	
	private volatile int tokens;
	
	private volatile int tokensAdded;
		
	public TokenBucketLimiterImpl(final int rate, final boolean spin)
	{
		this.rate = rate;
		
		this.spin = spin;
	}
	
	public int getRate()
	{
	   return rate;
	}
	
	public boolean isSpin()
	{
	   return spin;
	}
		
	public void limit()
	{			
		while (!check())
		{
			if (!spin)
			{
   			try
   			{
   				Thread.sleep(1);
   			}
   			catch (Exception e)
   			{			
   				//Ignore
   			}
			}
		}
	}
	
	private boolean check()
	{					
		long now = System.currentTimeMillis();
		
		if (last == 0)
		{
			last = now;
		}
		
		long diff = now - last;
		
		if (diff >= 1000)
		{
			last = last + 1000;
			
			tokens = 0;
			
			tokensAdded = 0;
		}
														
		int tokensDue = (int)(rate * diff  / 1000);
		
		int tokensToAdd = tokensDue - tokensAdded;
		
		if (tokensToAdd > 0)
		{
			tokens += tokensToAdd;
			
			tokensAdded += tokensToAdd;
		}
							
		if (tokens > 0)
		{
			tokens--;
			
			return true;
		}
		else
		{
			return false;
		}
	}	
}
