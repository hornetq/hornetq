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
package org.jboss.messaging.tests.unit.core.util;

import junit.framework.TestCase;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.util.TokenBucketLimiter;

/**
 * 
 * A TokenBucketLimiterTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TokenBucketLimiterTest extends TestCase
{
	private static final Logger log = Logger.getLogger(TokenBucketLimiterTest.class);
	
	public void testRateWithSpin1() throws Exception
	{
		testRate(1, true);		
	}
	
	public void testRateWithSpin10() throws Exception
	{
		testRate(10, true);		
	}
	
	public void testRateWithSpin100() throws Exception
	{
		testRate(100, true);		
	}
	
	public void testRateWithSpin1000() throws Exception
	{
		testRate(1000, true);		
	}
	
	public void testRateWithSpin10000() throws Exception
	{
		testRate(10000, true);		
	}
	
	public void testRateWithSpin100000() throws Exception
	{
		testRate(100000, true);		
	}
		
	public void testRateWithoutSpin1() throws Exception
	{
		testRate(1, false);		
	}
	
	public void testRateWithoutSpin10() throws Exception
	{
		testRate(10, false);		
	}
	
	public void testRateWithoutSpin100() throws Exception
	{
		testRate(100, false);		
	}
	
	public void testRateWithoutSpin1000() throws Exception
	{
		testRate(1000, false);		
	}
	
	public void testRateWithoutSpin10000() throws Exception
	{
		testRate(10000, false);		
	}
	
	public void testRateWithoutSpin100000() throws Exception
	{
		testRate(100000, false);		
	}
	
	private void testRate(int rate, boolean spin) throws Exception
	{		
		final double error = 0.05;    //Allow for 5% error
		
		TokenBucketLimiter tbl = new TokenBucketLimiter(rate, spin);
		
		long start = System.currentTimeMillis();
		
		long count = 0;
		
		final long measureTime = 5000;
		
		while (System.currentTimeMillis() - start < measureTime)
		{				
			tbl.limit();
			
			count++;
		}
				
		long end  = System.currentTimeMillis();
		
		double actualRate = ((double)(1000 * count)) / ( end - start);
    
      log.info("Desired rate: " + rate + " Actual rate " + actualRate + " invs/sec");
      
      assertTrue(actualRate > rate * (1 - error));
      
      assertTrue(actualRate < rate * (1 + error));
		
	}
}
