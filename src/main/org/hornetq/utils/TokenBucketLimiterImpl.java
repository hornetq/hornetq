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

package org.hornetq.utils;

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
         if (spin)
         {
            Thread.yield();
         }
         else
         {
            try
            {
               Thread.sleep(1);
            }
            catch (Exception e)
            {
               // Ignore
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

      int tokensDue = (int)(rate * diff / 1000);

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
