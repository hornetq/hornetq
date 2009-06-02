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

package org.jboss.messaging.core.asyncio.impl;

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
import org.jboss.messaging.core.logging.Logger;
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

   private static final Logger log = Logger.getLogger(TimedBuffer.class);

   // Attributes ----------------------------------------------------

   private final TimedBufferObserver bufferObserver;

   private final CheckTimer timerRunnable = new CheckTimer();

   private volatile ScheduledFuture<?> futureTimerRunnable;

   private final long timeout;

   private final int bufferSize;

   private final ByteBuffer currentBuffer;

   private volatile List<AIOCallback> callbacks;

   private volatile long timeLastWrite = 0;

   private final ScheduledExecutorService schedule = ScheduledSingleton.getScheduledService();

   private Lock lock = new ReentrantReadWriteLock().writeLock();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // private byte[] data;

   public TimedBuffer(final TimedBufferObserver bufferObserver, final int size, final long timeout)
   {
      bufferSize = size;
      this.bufferObserver = bufferObserver;
      this.timeout = timeout;
      this.currentBuffer = ByteBuffer.wrap(new byte[bufferSize]);
      this.currentBuffer.limit(0);
      this.callbacks = new ArrayList<AIOCallback>();
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
      if (sizeChecked > bufferSize)
      {
         throw new IllegalStateException("Can't write records bigger than the bufferSize(" + bufferSize +
                                         ") on the journal");
      }

      
      if (currentBuffer.limit() == 0 ||  currentBuffer.position() + sizeChecked > currentBuffer.limit())
      {
         flush();

         final int remaining = bufferObserver.getRemainingBytes();
         
         if (sizeChecked > remaining)
         {
            return false;
         }
         else
         {
            currentBuffer.rewind();
            currentBuffer.limit(Math.min(remaining, bufferSize));
            return true;
         }
      }
      else
      {
         return true;
      }

   }

   public synchronized void addBytes(final ByteBuffer bytes, final AIOCallback callback)
   {
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
      if (currentBuffer.limit() > 0)
      {
         ByteBuffer directBuffer = bufferObserver.newBuffer(bufferSize, currentBuffer.position());

         currentBuffer.flip();
         
         directBuffer.put(currentBuffer);

         bufferObserver.flushBuffer(directBuffer, callbacks);
         
         callbacks = new ArrayList<AIOCallback>();
      }

      if (futureTimerRunnable != null)
      {
         futureTimerRunnable.cancel(false);
         futureTimerRunnable = null;
      }

      timeLastWrite = 0;
      currentBuffer.limit(0);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

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
