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

package org.hornetq.core.asyncio.impl;

import java.nio.ByteBuffer;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hornetq.core.asyncio.AIOCallback;
import org.hornetq.core.asyncio.AsynchronousFile;
import org.hornetq.core.asyncio.BufferCallback;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.VariableLatch;

/**
 * 
 * AsynchronousFile implementation
 * 
 * @author clebert.suconic@jboss.com
 * Warning: Case you refactor the name or the package of this class
 *          You need to make sure you also rename the C++ native calls
 */
public class AsynchronousFileImpl implements AsynchronousFile
{
   // Static ----------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(AsynchronousFileImpl.class);

   private static final AtomicInteger totalMaxIO = new AtomicInteger(0);

   private static boolean loaded = false;

   private static int EXPECTED_NATIVE_VERSION = 27;

   /** Used to determine the next writing sequence */
   private AtomicLong nextWritingSequence = new AtomicLong(0);

   /** Used to determine the next writing sequence.
    *  This is accessed from a single thread (the Poller Thread) */
   private long nextReadSequence = 0;

   /** 
    * AIO can't guarantee ordering over callbacks.
    * We use thie PriorityQueue to hold values until they are in order
    */
   private PriorityQueue<CallbackHolder> pendingCallbacks = new PriorityQueue<CallbackHolder>();

   public static void addMax(final int io)
   {
      totalMaxIO.addAndGet(io);
   }

   /** For test purposes */
   public static int getTotalMaxIO()
   {
      return totalMaxIO.get();
   }

   public static void resetMaxAIO()
   {
      totalMaxIO.set(0);
   }

   private static boolean loadLibrary(final String name)
   {
      try
      {
         log.trace(name + " being loaded");
         System.loadLibrary(name);
         if (getNativeVersion() != EXPECTED_NATIVE_VERSION)
         {
            log.warn("You have a native library with a different version than expected");
            return false;
         }
         else
         {
            // Initializing nanosleep
            setNanoSleepInterval(1);
            return true;
         }
      }
      catch (Throwable e)
      {
         log.trace(name + " -> error loading the native library", e);
         return false;
      }

   }

   static
   {
      String libraries[] = new String[] { "HornetQAIO", "HornetQAIO64", "HornetQAIO32", "HornetQAIO_ia64" };

      for (String library : libraries)
      {
         if (loadLibrary(library))
         {
            loaded = true;
            break;
         }
         else
         {
            log.debug("Library " + library + " not found!");
         }
      }

      if (!loaded)
      {
         log.debug("Couldn't locate LibAIO Wrapper");
      }
   }

   public static boolean isLoaded()
   {
      return loaded;
   }

   // Attributes ------------------------------------------------------------------------

   private boolean opened = false;

   private String fileName;

   /** Used while inside the callbackDone and callbackError
    **/
   private final Lock callbackLock = new ReentrantLock();

   private final VariableLatch pollerLatch = new VariableLatch();

   private volatile Runnable poller;

   private int maxIO;

   private final Lock writeLock = new ReentrantReadWriteLock().writeLock();

   private final VariableLatch pendingWrites = new VariableLatch();

   private Semaphore maxIOSemaphore;

   private BufferCallback bufferCallback;

   /**
    *  Warning: Beware of the C++ pointer! It will bite you! :-)
    */
   private long handler;

   // A context switch on AIO would make it to synchronize the disk before
   // switching to the new thread, what would cause
   // serious performance problems. Because of that we make all the writes on
   // AIO using a single thread.
   private final Executor writeExecutor;

   private final Executor pollerExecutor;

   // AsynchronousFile implementation ---------------------------------------------------

   /**
    * @param writeExecutor It needs to be a single Thread executor. If null it will use the user thread to execute write operations
    * @param pollerExecutor The thread pool that will initialize poller handlers
    */
   public AsynchronousFileImpl(final Executor writeExecutor, final Executor pollerExecutor)
   {
      this.writeExecutor = writeExecutor;
      this.pollerExecutor = pollerExecutor;
   }

   public void open(final String fileName, final int maxIO) throws HornetQException
   {
      writeLock.lock();

      try
      {
         if (opened)
         {
            throw new IllegalStateException("AsynchronousFile is already opened");
         }

         this.maxIO = maxIO;
         maxIOSemaphore = new Semaphore(this.maxIO);

         this.fileName = fileName;

         try
         {
            handler = init(fileName, this.maxIO, log);
         }
         catch (HornetQException e)
         {
            HornetQException ex = null;
            if (e.getCode() == HornetQException.NATIVE_ERROR_CANT_INITIALIZE_AIO)
            {
               ex = new HornetQException(e.getCode(),
                                         "Can't initialize AIO. Currently AIO in use = " + totalMaxIO.get() +
                                                  ", trying to allocate more " +
                                                  maxIO,
                                         e);
            }
            else
            {
               ex = e;
            }
            throw ex;
         }
         opened = true;
         addMax(this.maxIO);
         nextWritingSequence.set(0);
         nextReadSequence = 0;
      }
      finally
      {
         writeLock.unlock();
      }
   }

   public void close() throws Exception
   {
      checkOpened();

      writeLock.lock();

      try
      {

         while (!pendingWrites.waitCompletion(60000))
         {
            log.warn("Couldn't get lock after 60 seconds on closing AsynchronousFileImpl::" + this.fileName);
         }

         while (!maxIOSemaphore.tryAcquire(maxIO, 60, TimeUnit.SECONDS))
         {
            log.warn("Couldn't get lock after 60 seconds on closing AsynchronousFileImpl::" + this.fileName);
         }

         maxIOSemaphore = null;
         if (poller != null)
         {
            stopPoller();
         }

         closeInternal(handler);
         if (handler != 0)
         {
            addMax(-maxIO);
         }
         opened = false;
         handler = 0;
      }
      finally
      {
         writeLock.unlock();
      }
   }

   public void write(final long position,
                     final long size,
                     final ByteBuffer directByteBuffer,
                     final AIOCallback aioCallback)
   {
      if (aioCallback == null)
      {
         throw new NullPointerException("Null Callback");
      }

      checkOpened();
      if (poller == null)
      {
         startPoller();
      }

      pendingWrites.up();

      if (writeExecutor != null)
      {
         writeExecutor.execute(new Runnable()
         {
            public void run()
            {
               maxIOSemaphore.acquireUninterruptibly();

               long sequence = nextWritingSequence.getAndIncrement();

               try
               {
                  write(handler, sequence, position, size, directByteBuffer, aioCallback);
               }
               catch (HornetQException e)
               {
                  callbackError(aioCallback, sequence, directByteBuffer, e.getCode(), e.getMessage());
               }
               catch (RuntimeException e)
               {
                  callbackError(aioCallback,
                                sequence,
                                directByteBuffer,
                                HornetQException.INTERNAL_ERROR,
                                e.getMessage());
               }
            }
         });
      }
      else
      {
         maxIOSemaphore.acquireUninterruptibly();

         long sequence = nextWritingSequence.getAndIncrement();

         try
         {
            write(handler, sequence, position, size, directByteBuffer, aioCallback);
         }
         catch (HornetQException e)
         {
            callbackError(aioCallback, sequence, directByteBuffer, e.getCode(), e.getMessage());
         }
         catch (RuntimeException e)
         {
            callbackError(aioCallback, sequence, directByteBuffer, HornetQException.INTERNAL_ERROR, e.getMessage());
         }
      }

   }

   public void read(final long position,
                    final long size,
                    final ByteBuffer directByteBuffer,
                    final AIOCallback aioPackage) throws HornetQException
   {
      checkOpened();
      if (poller == null)
      {
         startPoller();
      }
      pendingWrites.up();
      maxIOSemaphore.acquireUninterruptibly();
      try
      {
         read(handler, position, size, directByteBuffer, aioPackage);
      }
      catch (HornetQException e)
      {
         // Release only if an exception happened
         maxIOSemaphore.release();
         pendingWrites.down();
         throw e;
      }
      catch (RuntimeException e)
      {
         // Release only if an exception happened
         maxIOSemaphore.release();
         pendingWrites.down();
         throw e;
      }
   }

   public long size() throws HornetQException
   {
      checkOpened();
      return size0(handler);
   }

   public void fill(final long position, final int blocks, final long size, final byte fillChar) throws HornetQException
   {
      checkOpened();
      fill(handler, position, blocks, size, fillChar);
   }

   public int getBlockSize()
   {
      return 512;
   }

   public String getFileName()
   {
      return fileName;
   }

   /**
    * This needs to be synchronized because of 
    * http://bugs.sun.com/view_bug.do?bug_id=6791815
    * http://mail.openjdk.java.net/pipermail/hotspot-runtime-dev/2009-January/000386.html
    * 
    * @param size
    * @return
    */
   public synchronized static ByteBuffer newBuffer(final int size)
   {
      if (size % 512 != 0)
      {
         throw new RuntimeException("Buffer size needs to be aligned to 512");
      }

      return newNativeBuffer(size);
   }

   public void setBufferCallback(final BufferCallback callback)
   {
      bufferCallback = callback;
   }

   /** Return the JNI handler used on C++ */
   public long getHandler()
   {
      return handler;
   }

   public static void clearBuffer(final ByteBuffer buffer)
   {
      resetBuffer(buffer, buffer.limit());
      buffer.position(0);
   }

   // Protected -------------------------------------------------------------------------

   protected void finalize()
   {
      if (opened)
      {
         log.warn("AsynchronousFile: " + this.fileName + " being finalized with opened state");
      }
   }

   // Private ---------------------------------------------------------------------------

   /** */
   @SuppressWarnings("unused")
   private void callbackDone(final AIOCallback callback, final long sequence, final ByteBuffer buffer)
   {
      maxIOSemaphore.release();

      pendingWrites.down();

      callbackLock.lock();

      try
      {

         if (sequence == -1)
         {
            callback.done();
         }
         else
         {
            if (sequence == nextReadSequence)
            {
               nextReadSequence++;
               callback.done();
               flushCallbacks();
            }
            else
            {
               pendingCallbacks.add(new CallbackHolder(sequence, callback));
            }
         }

         // The buffer is not sent on callback for read operations
         if (bufferCallback != null && buffer != null)
         {
            bufferCallback.bufferDone(buffer);
         }
      }
      finally
      {
         callbackLock.unlock();
      }
   }

   private void flushCallbacks()
   {
      while (!pendingCallbacks.isEmpty() && pendingCallbacks.peek().sequence == nextReadSequence)
      {
         CallbackHolder holder = pendingCallbacks.poll();
         if (holder.isError())
         {
            ErrorCallback error = (ErrorCallback) holder;
            holder.callback.onError(error.errorCode, error.message);
         }
         else
         {
            holder.callback.done();
         }
         nextReadSequence++;
      }
   }

   // Called by the JNI layer.. just ignore the
   // warning
   private void callbackError(final AIOCallback callback,
                                           final long sequence,
                                           final ByteBuffer buffer,
                                           final int errorCode,
                                           final String errorMessage)
   {
      log.warn("CallbackError: " + errorMessage);

      maxIOSemaphore.release();

      pendingWrites.down();

      callbackLock.lock();

      try
      {
         if (sequence == -1)
         {
            callback.onError(errorCode, errorMessage);
         }
         else
         {
            if (sequence == nextReadSequence)
            {
               nextReadSequence++;
               callback.onError(errorCode, errorMessage);
               flushCallbacks();
            }
            else
            {
               pendingCallbacks.add(new ErrorCallback(sequence, callback, errorCode, errorMessage));
            }
         }
      }
      finally
      {
         callbackLock.unlock();
      }

      // The buffer is not sent on callback for read operations
      if (bufferCallback != null && buffer != null)
      {
         bufferCallback.bufferDone(buffer);
      }
   }

   private void pollEvents()
   {
      if (!opened)
      {
         return;
      }
      internalPollEvents(handler);
   }

   private void startPoller()
   {
      writeLock.lock();

      try
      {

         if (poller == null)
         {
            pollerLatch.up();
            poller = new PollerRunnable();
            try
            {
               pollerExecutor.execute(poller);
            }
            catch (Exception ex)
            {
               log.error(ex.getMessage(), ex);
            }
         }
      }
      finally
      {
         writeLock.unlock();
      }
   }

   private void checkOpened()
   {
      if (!opened)
      {
         throw new RuntimeException("File is not opened");
      }
   }

   /**
    * @throws HornetQException
    * @throws InterruptedException
    */
   private void stopPoller() throws HornetQException, InterruptedException
   {
      stopPoller(handler);
      // We need to make sure we won't call close until Poller is
      // completely done, or we might get beautiful GPFs
      pollerLatch.waitCompletion();
   }

   // Native ----------------------------------------------------------------------------

   private static native void resetBuffer(ByteBuffer directByteBuffer, int size);

   public static native void destroyBuffer(ByteBuffer buffer);

   /** Instead of passing the nanoSeconds through the stack call every time, we set it statically inside the native method */
   public static native void setNanoSleepInterval(int nanoseconds);

   public static native void nanoSleep();

   private static native ByteBuffer newNativeBuffer(long size);

   private static native long init(String fileName, int maxIO, Logger logger) throws HornetQException;

   private native long size0(long handle) throws HornetQException;

   private native void write(long handle,
                             long sequence,
                             long position,
                             long size,
                             ByteBuffer buffer,
                             AIOCallback aioPackage) throws HornetQException;

   private native void read(long handle, long position, long size, ByteBuffer buffer, AIOCallback aioPackage) throws HornetQException;

   private static native void fill(long handle, long position, int blocks, long size, byte fillChar) throws HornetQException;

   private static native void closeInternal(long handler) throws HornetQException;

   private static native void stopPoller(long handler) throws HornetQException;

   /** A native method that does nothing, and just validate if the ELF dependencies are loaded and on the correct platform as this binary format */
   private static native int getNativeVersion();

   /** Poll asynchronous events from internal queues */
   private static native void internalPollEvents(long handler);

   // Inner classes ---------------------------------------------------------------------

   private static class CallbackHolder implements Comparable<CallbackHolder>
   {
      final long sequence;

      final AIOCallback callback;

      public boolean isError()
      {
         return false;
      }

      public CallbackHolder(final long sequence, final AIOCallback callback)
      {
         this.sequence = sequence;
         this.callback = callback;
      }

      public int compareTo(CallbackHolder o)
      {
         // It shouldn't be equals in any case
         if (sequence <= o.sequence)
         {
            return -1;
         }
         else
         {
            return 1;
         }
      }
   }

   private static class ErrorCallback extends CallbackHolder
   {
      final int errorCode;
      
      final String message;
      
      public boolean isError()
      {
         return true;
      }

      public ErrorCallback(final long sequence, final AIOCallback callback, int errorCode, String message)
      {
         super(sequence, callback);
         
         this.errorCode = errorCode;
         
         this.message = message;
      }
   }

   private class PollerRunnable implements Runnable
   {
      PollerRunnable()
      {
      }

      public void run()
      {
         try
         {
            pollEvents();
         }
         finally
         {
            // This gives us extra protection in cases of interruption
            // Case the poller thread is interrupted, this will allow us to
            // restart the thread when required
            poller = null;
            pollerLatch.down();
         }
      }
   }
}
