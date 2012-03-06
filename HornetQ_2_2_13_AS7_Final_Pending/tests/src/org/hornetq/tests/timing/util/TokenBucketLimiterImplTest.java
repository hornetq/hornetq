/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.timing.util;

import junit.framework.Assert;

import org.hornetq.core.logging.Logger;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.TokenBucketLimiterImpl;

/**
 * 
 * A TokenBucketLimiterImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TokenBucketLimiterImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(TokenBucketLimiterImplTest.class);

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

   private void testRate(final int rate, final boolean spin) throws Exception
   {
      final double error = 0.05; // Allow for 5% error

      TokenBucketLimiterImpl tbl = new TokenBucketLimiterImpl(rate, spin);

      long start = System.currentTimeMillis();

      long count = 0;

      final long measureTime = 5000;

      while (System.currentTimeMillis() - start < measureTime)
      {
         tbl.limit();

         count++;
      }

      long end = System.currentTimeMillis();

      double actualRate = (double)(1000 * count) / (end - start);

      TokenBucketLimiterImplTest.log.debug("Desired rate: " + rate + " Actual rate " + actualRate + " invs/sec");

      Assert.assertTrue(actualRate > rate * (1 - error));

      Assert.assertTrue(actualRate < rate * (1 + error));

   }
}
