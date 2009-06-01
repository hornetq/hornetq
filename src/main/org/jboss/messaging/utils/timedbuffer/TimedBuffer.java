/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.utils.timedbuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.messaging.core.asyncio.AIOCallback;
import org.jboss.messaging.utils.JBMThreadFactory;

/**
 * A TimedBuffer
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class TimedBuffer
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final TimedBufferObserver bufferObserver;

   private final CheckTimer timerRunnable = new CheckTimer();

   private volatile ScheduledFuture<?> futureTimerRunnable;

   private final long timeout;

   private final int bufferSize;

   private volatile ByteBuffer currentBuffer;

   private volatile List<AIOCallback> callbacks;

   private volatile long timeLastWrite = 0;

   private final ScheduledExecutorService schedule = ScheduledSingleton.getScheduledService();
   
   private Lock lock = new ReentrantReadWriteLock().writeLock();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public TimedBuffer(final TimedBufferObserver bufferObserver, final int size, final long timeout)
   {
      bufferSize = size;
      this.bufferObserver = bufferObserver;
      this.timeout = timeout;
   }

   public int position()
   {
      if (currentBuffer == null)
      {
         return 0;
      }
      else
      {
         return currentBuffer.position();
      }
   }

   public void checkTimer()
   {
      if (System.currentTimeMillis() - timeLastWrite > timeout)
      {
         lock.lock();
         try
         {
            flush();
         }
         finally
         {
            lock.unlock();
         }
      }

   }
   
   
   public void lock()
   {
      lock.lock();
   }
   
   public void unlock()
   {
      lock.unlock();
   }

   /**
    * Verify if the size fits the buffer, if it fits we lock the buffer to avoid a flush until add is called
    * @param sizeChecked
    * @return
    */
   public synchronized boolean checkSize(final int sizeChecked)
   {
      final boolean fits;
      if (sizeChecked > bufferSize)
      {
         flush();

         // We transfer the bytes, as the bufferObserver has special alignment restrictions on the buffer addressing
         currentBuffer = bufferObserver.newBuffer(sizeChecked, sizeChecked);

         fits = currentBuffer != null;
      }
      else
      {
         // We verify against the currentBuffer.capacity as the observer may return a smaller buffer
         if (currentBuffer == null || currentBuffer.position() + sizeChecked > currentBuffer.limit())
         {
            flush();
            newBuffer(sizeChecked);
         }

         fits = currentBuffer != null;
      }

      return fits;
   }

   public synchronized void addBytes(final ByteBuffer bytes, final AIOCallback callback)
   {
      if (currentBuffer == null)
      {
         newBuffer(0);
      }

      currentBuffer.put(bytes);
      callbacks.add(callback);

      if (futureTimerRunnable == null)
      {
         futureTimerRunnable = schedule.scheduleAtFixedRate(timerRunnable, timeout, timeout, TimeUnit.MILLISECONDS);
      }

      timeLastWrite = System.currentTimeMillis();

      if (currentBuffer.position() == currentBuffer.capacity())
      {
         flush();
      }
   }

   public synchronized void flush()
   {
      if (currentBuffer != null)
      {
         bufferObserver.flushBuffer(currentBuffer, callbacks);
         currentBuffer = null;
         callbacks = null;
      }

      if (futureTimerRunnable != null)
      {
         futureTimerRunnable.cancel(false);
         futureTimerRunnable = null;
      }

      timeLastWrite = 0;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void newBuffer(final int minSize)
   {
      currentBuffer = bufferObserver.newBuffer(minSize, bufferSize);
      callbacks = new ArrayList<AIOCallback>();
   }

   // Inner classes -------------------------------------------------

   class CheckTimer implements Runnable
   {
      public void run()
      {
         checkTimer();
      }
   }

   // TODO: is there a better place to get this schedule service from?
   static class ScheduledSingleton
   {
      private static ScheduledExecutorService scheduleService;

      private static synchronized ScheduledExecutorService getScheduledService()
      {
         if (scheduleService == null)
         {
            ThreadFactory factory = new JBMThreadFactory("JBM-buffer-scheduled-control", true);

            scheduleService = Executors.newScheduledThreadPool(2, factory);
         }

         return scheduleService;
      }
   }

}
