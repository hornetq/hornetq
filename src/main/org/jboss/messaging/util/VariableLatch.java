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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;


/**
 * 
 * <p>This class will use the framework provided to by AbstractQueuedSynchronizer.</p>
 * <p>AbstractQueuedSynchronizer is the framework for any sort of concurrent synchronization, such as Semaphores, events, etc, based on AtomicIntegers.</p>
 * 
 * <p>The idea is, instead of providing each user specific Latch/Synchronization, java.util.concurrent provides the framework for reuses, based on an AtomicInteger (getState())</p>
 * 
 * <p>On JBossMessaging we have the requirement of increment and decrement a counter until the user fires a ready event (commit). At that point we just act as a regular countDown.</p>
 * 
 * <p>Note: This latch is reusable. Once it reaches zero, you can call up again, and reuse it on further waits.</p>
 * 
 * <p>For example: prepareTransaction will wait for the current completions, and further adds will be called on the latch. Later on when commit is called you can reuse the same latch.</p>
 * 
 * @author Clebert Suconic
 * */
public class VariableLatch
{
   /** 
    * Look at the doc and examples provided by AbstractQueuedSynchronizer for more information 
    * @see AbstractQueuedSynchronizer*/
   @SuppressWarnings("serial")
   private static class CountSync extends AbstractQueuedSynchronizer
   {      
      public CountSync ()
      {
         setState(0);
      }
            
      public int getCount()
      {
         return getState();
      }
      
      public int tryAcquireShared(final int numberOfAqcquires)
      {
         return getState()==0 ? 1 : -1;
      }
      
      public void add()
      {
         for (;;)
         {
            int actualState = getState();
            int newState = actualState + 1;
            if (compareAndSetState(actualState, newState))
            {
               return;
            }
         }
      }
            
      public boolean tryReleaseShared(final int numberOfReleases)
      {
         for (;;)
         {
            int actualState = getState();
            if (actualState == 0)
            {
               return true;
            }
            
            int newState = actualState - numberOfReleases;
            
            if (compareAndSetState(actualState, newState))
            {
               return newState == 0; 
            }
         }
      }
   }
   
   private final CountSync control = new CountSync();
      
   public int getCount()
   {
      return control.getCount();
   }
   
   public void up()
   {
      control.add();
   }
   
   public void down()
   {
      control.releaseShared(1);
   }
   
   public void waitCompletion() throws InterruptedException
   {
      control.acquireSharedInterruptibly(1);
   }
   
   public boolean waitCompletion(final long milliseconds) throws InterruptedException
   {
      return control.tryAcquireSharedNanos(1, TimeUnit.MILLISECONDS.toNanos(milliseconds));
   }
}
