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
 * A Future
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Future implements Runnable
{
   private boolean done;

   public synchronized boolean await(final long timeout)
   {
      long toWait = timeout;

      long start = System.currentTimeMillis();

      while (!done && toWait > 0)
      {
         try
         {
            wait(toWait);
         }
         catch (InterruptedException e)
         {
         }

         long now = System.currentTimeMillis();

         toWait -= now - start;

         start = now;
      }

      return done;
   }

   public synchronized void run()
   {
      done = true;

      notify();
   }

}
