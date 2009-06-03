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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.messaging.core.asyncio.AIOCallback;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.utils.VariableLatch;

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

   private TimedBufferObserver bufferObserver;
   
   
   // This is used to pause and resume the timer
   // This is a reusable Latch, that uses java.util.concurrent base classes
   private final VariableLatch latchTimer = new VariableLatch();

   private CheckTimer timerRunnable = new CheckTimer();

   private final long timeout;

   private final int bufferSize;

   private final ByteBuffer currentBuffer;

   private List<AIOCallback> callbacks;

   private final Lock lock = new ReentrantReadWriteLock().writeLock();

   // used to measure inactivity. This buffer will be automatically flushed when more than timeout inactive
   private volatile long timeLastAdd = 0;

   // used to measure sync requests. When a sync is requested, it shouldn't take more than timeout to happen
   private volatile long timeLastSync = 0;

   private Thread timerThread;
   
   private boolean started;
   
   private final boolean flushOnSync;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public TimedBuffer(final int size, final long timeout, final boolean flushOnSync)
   {
      bufferSize = size;
      this.timeout = timeout;      
      currentBuffer = ByteBuffer.wrap(new byte[bufferSize]);
      currentBuffer.limit(0);
      callbacks = new ArrayList<AIOCallback>();
      this.flushOnSync = flushOnSync;
      latchTimer.up();
   }
   
   public synchronized void start()
   {
      if (started)
      {
         return;
      }
      
      timerRunnable = new CheckTimer();
      
      timerThread = new Thread(timerRunnable, "jbm-aio-timer");

      timerThread.start();
      
      started = true;
   }

   public synchronized void stop()
   {
      if (!started)
      {
         return;
      }
      
      latchTimer.down();
      
      timerRunnable.close();

      while (timerThread.isAlive())
      {
         try
         {
            timerThread.join();
         }
         catch (InterruptedException e)
         {
         }
      }
      
      started = false;
   }

   public synchronized void setObserver(TimedBufferObserver observer)
   {
      if (this.bufferObserver != null)
      {
         flush();
      }
      
      this.bufferObserver = observer;
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
    * Verify if the size fits the buffer
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

   public synchronized void addBytes(final ByteBuffer bytes, final boolean sync, final AIOCallback callback)
   {
      long now = System.nanoTime();

      currentBuffer.put(bytes);
      callbacks.add(callback);

      timeLastAdd = now;
      
      latchTimer.down();

      if (sync)
      {
         if (flushOnSync)
         {
            flush();
         }
         else
         {
            // We should flush on the next timeout, no matter what other activity happens on the buffer
            if (timeLastSync == 0)
            {
               timeLastSync = now;
            }
         }
      }
      
      if (currentBuffer.position() == currentBuffer.capacity())
      {
         flush();
      }
   }

   public synchronized void flush()
   {
      if (currentBuffer.limit() > 0)
      {
         latchTimer.up();
         
         ByteBuffer directBuffer = bufferObserver.newBuffer(bufferSize, currentBuffer.position());

         // Putting a byteArray on a native buffer is much faster, since it will do in a single native call.
         // Using directBuffer.put(currentBuffer) would make several append calls for each byte
         directBuffer.put(currentBuffer.array(), 0, currentBuffer.position());

         bufferObserver.flushBuffer(directBuffer, callbacks);

         callbacks = new ArrayList<AIOCallback>();

         timeLastAdd = 0;
         timeLastSync = 0;

         currentBuffer.limit(0);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void checkTimer()
   {
      final long now = System.nanoTime();

      // if inactive for more than the timeout
      // of if a sync happened at more than the the timeout ago
      if (timeLastAdd != 0 && now - timeLastAdd >= timeout || timeLastSync != 0 && now - timeLastSync >= timeout)
      {
         lock.lock();
         try
         {            
            if (bufferObserver != null)
            {
               flush();
            }
         }
         finally
         {
            lock.unlock();
         }
      }
   }

   // Inner classes -------------------------------------------------

   private class CheckTimer implements Runnable
   {
      private volatile boolean closed = false;
      
      public void run()
      {
         while (!closed)
         {
            try
            {
               latchTimer.waitCompletion();
            }
            catch (InterruptedException ignored)
            {
            }

            checkTimer();

            //TODO - this yield is temporary
            
            Thread.yield();
         }
      }

      public void close()
      {
         closed = true;
      }
   }

}
