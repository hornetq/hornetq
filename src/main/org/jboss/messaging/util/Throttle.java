package org.jboss.messaging.util;

import org.jboss.logging.Logger;

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

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>20 Oct 2007
 *
 * $Id: $
 *
 */
public class Throttle
{
   protected Logger log = Logger.getLogger(Throttle.class);
	
   private long minSleep;
   
   private long every;
	
	public Throttle(double rate, long minSleep)
	{
		this.minSleep = minSleep;
		
		every = (long)(rate * minSleep) / 1000;
		
		log.info("every: " + every);
	}
	
	private int count;
	
	public synchronized void ping()
	{
		count++;
		
		if (count == every)
		{
			try
			{
				Thread.sleep(minSleep);
			}
			catch (InterruptedException e)
			{				
			}
			count = 0;
		}
	}
}
