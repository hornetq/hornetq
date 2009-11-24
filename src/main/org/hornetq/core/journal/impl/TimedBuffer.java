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

package org.hornetq.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.VariableLatch;

/**
 * A TimedBuffer
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

   private final int bufferSize;

   private final HornetQBuffer buffer;

   private int bufferLimit = 0;

   private List<IOAsyncTask> callbacks;

   private final Lock lock = new ReentrantReadWriteLock().writeLock();

   // used to measure inactivity. This buffer will be automatically flushed when more than timeout inactive
   private volatile boolean active = false;

   private final long timeout;

   // used to measure sync requests. When a sync is requested, it shouldn't take more than timeout to happen
   private volatile boolean pendingSync = false;

   private Thread timerThread;

   private volatile boolean started;

   private final boolean flushOnSync;

   // for logging write rates

   private final boolean logRates;

   private volatile long bytesFlushed;

   private Timer logRatesTimer;

   private TimerTask logRatesTimerTask;

   private long lastExecution;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public TimedBuffer(final int size, final long timeout, final boolean flushOnSync, final boolean logRates)
   {
      bufferSize = size;
      this.logRates = logRates;
      if (logRates)
      {
         this.logRatesTimer = new Timer(true);
      }
      // Setting the interval for nano-sleeps

      buffer = ChannelBuffers.buffer(bufferSize);
      buffer.clear();
      bufferLimit = 0;

      callbacks = new ArrayList<IOAsyncTask>();
      this.flushOnSync = flushOnSync;
      latchTimer.up();
      this.timeout = timeout;
   }

   public synchronized void start()
   {
      if (started)
      {
         return;
      }

      timerRunnable = new CheckTimer();

      timerThread = new Thread(timerRunnable, "hornetq-async-buffer");

      timerThread.start();

      if (logRates)
      {
         logRatesTimerTask = new LogRatesTimerTask();

         logRatesTimer.scheduleAtFixedRate(logRatesTimerTask, 2000, 2000);
      }

      started = true;
   }

   public void stop()
   {
      if (!started)
      {
         return;
      }

      this.flush();

      this.bufferObserver = null;

      latchTimer.down();

      timerRunnable.close();

      if (logRates)
      {
         logRatesTimerTask.cancel();
      }

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

   public void disableAutoFlush()
   {
      lock.lock();
   }

   public void enableAutoFlush()
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

      if (bufferLimit == 0 || buffer.writerIndex() + sizeChecked > bufferLimit)
      {
         flush();

         final int remaining = bufferObserver.getRemainingBytes();

         if (sizeChecked > remaining)
         {
            return false;
         }
         else
         {
            buffer.clear();
            bufferLimit = Math.min(remaining, bufferSize);
            return true;
         }
      }
      else
      {
         return true;
      }
   }

   public synchronized void addBytes(final byte[] bytes, final boolean sync, final IOAsyncTask callback)
   {
      if (buffer.writerIndex() == 0)
      {
         // Resume latch
         latchTimer.down();
      }

      buffer.writeBytes(bytes);

      callbacks.add(callback);

      active = true;

      if (sync)
      {
         if (!pendingSync)
         {
            pendingSync = true;
         }

         if (flushOnSync)
         {
            flush();
         }
      }

      if (buffer.writerIndex() == bufferLimit)
      {
         flush();
      }
   }

   public void flush()
   {
      ByteBuffer bufferToFlush = null;
      
      boolean useSync = false;
      
      List<IOAsyncTask> callbacksToCall = null;
      
      synchronized (this)
      {
         if (buffer.writerIndex() > 0)
         {
            latchTimer.up();
   
            int pos = buffer.writerIndex();
   
            if (logRates)
            {
               bytesFlushed += pos;
            }
   
            bufferToFlush = bufferObserver.newBuffer(bufferSize, pos);
   
            // Putting a byteArray on a native buffer is much faster, since it will do in a single native call.
            // Using bufferToFlush.put(buffer) would make several append calls for each byte
   
            bufferToFlush.put(buffer.array(), 0, pos);

            callbacksToCall = callbacks;
            
            callbacks = new LinkedList<IOAsyncTask>();
   
            useSync = pendingSync;
            
            active = false;
            pendingSync = false;
   
            buffer.clear();
            bufferLimit = 0;
         }
      }
      
      // Execute the flush outside of the lock
      // This is important for NIO performance while we are using NIO Callbacks
      if (bufferToFlush != null)
      {
         bufferObserver.flushBuffer(bufferToFlush, useSync, callbacksToCall);
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void checkTimer()
   {
      // if inactive for more than the timeout
      // of if a sync happened at more than the the timeout ago
      if (!active || pendingSync)
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

      // Set the buffer as inactive.. we will flush the buffer next tick if nothing change this
      active = false;
   }

   // Inner classes -------------------------------------------------

   private class LogRatesTimerTask extends TimerTask
   {
      private boolean closed;

      @Override
      public synchronized void run()
      {
         if (!closed)
         {
            long now = System.currentTimeMillis();

            if (lastExecution != 0)
            {
               double rate = 1000 * ((double)bytesFlushed) / (now - lastExecution);
               log.info("Write rate = " + rate + " bytes / sec or " + (long)(rate / (1024 * 1024)) + " MiB / sec");
            }

            lastExecution = now;

            bytesFlushed = 0;
         }
      }

      public synchronized boolean cancel()
      {
         closed = true;

         return super.cancel();
      }
   }

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

            sleep();

            checkTimer();

         }
      }

      /**
       * 
       */
      private void sleep()
      {
         long time = System.nanoTime() + timeout;
         while (time > System.nanoTime())
         {
            Thread.yield();
         }
      }

      public void close()
      {
         closed = true;
      }
   }

}
