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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.messaging.core.asyncio.AIOCallback;
import org.jboss.messaging.core.logging.Logger;

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

   private final long timeout;

   private final int bufferSize;

   private final ByteBuffer currentBuffer;

   private List<AIOCallback> callbacks;

   private final Lock lock = new ReentrantReadWriteLock().writeLock();

   private final Timer timer;

   private long lastFlushTime;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public TimedBuffer(final TimedBufferObserver bufferObserver, final int size, final long timeout)
   {
      bufferSize = size;      
      this.bufferObserver = bufferObserver;
      this.timeout = timeout;
      this.currentBuffer = ByteBuffer.wrap(new byte[bufferSize]);
      this.currentBuffer.limit(0);
      this.callbacks = new ArrayList<AIOCallback>();
      this.timer = new Timer("jbm-timed-buffer", true);
      
      this.timer.schedule(timerRunnable, timeout, timeout);
   }
   
   public synchronized void close()
   {
      timerRunnable.cancel();
      
      timer.cancel();
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

      if (currentBuffer.limit() == 0 || currentBuffer.position() + sizeChecked > currentBuffer.limit())
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

      currentBuffer.limit(0);

      this.lastFlushTime = System.currentTimeMillis();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   private void checkTimer()
   {
      if (System.currentTimeMillis() - lastFlushTime >= timeout)
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


   // Inner classes -------------------------------------------------

   class CheckTimer extends TimerTask
   {
      private boolean cancelled;

      @Override
      public synchronized void run()
      {
         if (!cancelled)
         {
            checkTimer();
         }
      }

      @Override
      public synchronized boolean cancel()
      {
         cancelled = true;

         return super.cancel();
      }
   }

}
