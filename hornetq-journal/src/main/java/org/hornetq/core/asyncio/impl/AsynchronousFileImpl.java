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
import java.nio.channels.FileLock;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.asyncio.AIOCallback;
import org.hornetq.core.asyncio.AsynchronousFile;
import org.hornetq.core.asyncio.BufferCallback;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.ReusableLatch;

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

   /** This definition needs to match Version.h on the native sources.
       Or else the native module won't be loaded because of version mismatches */
   private static int EXPECTED_NATIVE_VERSION = 31;

   /** Used to determine the next writing sequence */
   private final AtomicLong nextWritingSequence = new AtomicLong(0);

   /** Used to determine the next writing sequence.
    *  This is accessed from a single thread (the Poller Thread) */
   private long nextReadSequence = 0;

   /** 
    * AIO can't guarantee ordering over callbacks.
    * We use thie PriorityQueue to hold values until they are in order
    */
   private final PriorityQueue<CallbackHolder> pendingCallbacks = new PriorityQueue<CallbackHolder>();

   public static void addMax(final int io)
   {
      AsynchronousFileImpl.totalMaxIO.addAndGet(io);
   }

   /** For test purposes */
   public static int getTotalMaxIO()
   {
      return AsynchronousFileImpl.totalMaxIO.get();
   }

   public static void resetMaxAIO()
   {
      AsynchronousFileImpl.totalMaxIO.set(0);
   }

   private static boolean loadLibrary(final String name)
   {
      try
      {
         AsynchronousFileImpl.log.trace(name + " being loaded");
         System.loadLibrary(name);
         if (AsynchronousFileImpl.getNativeVersion() != AsynchronousFileImpl.EXPECTED_NATIVE_VERSION)
         {
            AsynchronousFileImpl.log.warn("You have a native library with a different version than expected");
            return false;
         }
         else
         {
            // Initializing nanosleep
            AsynchronousFileImpl.setNanoSleepInterval(1);
            return true;
         }
      }
      catch (Throwable e)
      {
         AsynchronousFileImpl.log.debug(name + " -> error loading the native library", e);
         return false;
      }

   }

   static
   {
      String libraries[] = new String[] { "HornetQAIO", "HornetQAIO64", "HornetQAIO32", "HornetQAIO_ia64" };

      for (String library : libraries)
      {
         if (AsynchronousFileImpl.loadLibrary(library))
         {
            AsynchronousFileImpl.loaded = true;
            break;
         }
         else
         {
            AsynchronousFileImpl.log.debug("Library " + library + " not found!");
         }
      }

      if (!AsynchronousFileImpl.loaded)
      {
         AsynchronousFileImpl.log.debug("Couldn't locate LibAIO Wrapper");
      }
   }

   public static boolean isLoaded()
   {
      return AsynchronousFileImpl.loaded;
   }

   // Attributes ------------------------------------------------------------------------

   private boolean opened = false;

   private String fileName;

   /** Used while inside the callbackDone and callbackError
    **/
   private final Lock callbackLock = new ReentrantLock();

   private final ReusableLatch pollerLatch = new ReusableLatch();

   private volatile Runnable poller;

   private int maxIO;

   private final Lock writeLock = new ReentrantReadWriteLock().writeLock();

   private final ReusableLatch pendingWrites = new ReusableLatch();

   private Semaphore maxIOSemaphore;

   private BufferCallback bufferCallback;

   /**
    *  Warning: Beware of the C++ pointer! It will bite you! :-)
    */
   private ByteBuffer handler;

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
            handler = AsynchronousFileImpl.init(fileName, this.maxIO, AsynchronousFileImpl.log);
         }
         catch (HornetQException e)
         {
            HornetQException ex = null;
            if (e.getCode() == HornetQException.NATIVE_ERROR_CANT_INITIALIZE_AIO)
            {
               ex = new HornetQException(e.getCode(),
                                         "Can't initialize AIO. Currently AIO in use = " + AsynchronousFileImpl.totalMaxIO.get() +
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
         AsynchronousFileImpl.addMax(this.maxIO);
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

         while (!pendingWrites.await(60000))
         {
            AsynchronousFileImpl.log.warn("Couldn't get lock after 60 seconds on closing AsynchronousFileImpl::" + fileName);
         }

         while (!maxIOSemaphore.tryAcquire(maxIO, 60, TimeUnit.SECONDS))
         {
            AsynchronousFileImpl.log.warn("Couldn't get lock after 60 seconds on closing AsynchronousFileImpl::" + fileName);
         }

         maxIOSemaphore = null;
         if (poller != null)
         {
            stopPoller();
         }

         if (handler != null)
         {
            AsynchronousFileImpl.closeInternal(handler);
            AsynchronousFileImpl.addMax(-maxIO);
         }
         opened = false;
         handler = null;
      }
      finally
      {
         writeLock.unlock();
      }
   }
   
   
   public void writeInternal(long positionToWrite, long size, ByteBuffer bytes) throws HornetQException
   {
      writeInternal(handler, positionToWrite, size, bytes);
      if (bufferCallback != null)
      {
         bufferCallback.bufferDone(bytes);
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

      pendingWrites.countUp();

      if (writeExecutor != null)
      {
         maxIOSemaphore.acquireUninterruptibly();

         writeExecutor.execute(new Runnable()
         {
            public void run()
            {
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
      pendingWrites.countUp();
      maxIOSemaphore.acquireUninterruptibly();
      try
      {
         read(handler, position, size, directByteBuffer, aioPackage);
      }
      catch (HornetQException e)
      {
         // Release only if an exception happened
         maxIOSemaphore.release();
         pendingWrites.countDown();
         throw e;
      }
      catch (RuntimeException e)
      {
         // Release only if an exception happened
         maxIOSemaphore.release();
         pendingWrites.countDown();
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
      AsynchronousFileImpl.fill(handler, position, blocks, size, fillChar);
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
    */
   public synchronized static ByteBuffer newBuffer(final int size)
   {
      if (size % 512 != 0)
      {
         throw new RuntimeException("Buffer size needs to be aligned to 512");
      }

      return AsynchronousFileImpl.newNativeBuffer(size);
   }

   public void setBufferCallback(final BufferCallback callback)
   {
      bufferCallback = callback;
   }

   /** Return the JNI handler used on C++ */
   public ByteBuffer getHandler()
   {
      return handler;
   }

   public static void clearBuffer(final ByteBuffer buffer)
   {
      AsynchronousFileImpl.resetBuffer(buffer, buffer.limit());
      buffer.position(0);
   }

   // Protected -------------------------------------------------------------------------

   @Override
   protected void finalize()
   {
      if (opened)
      {
         AsynchronousFileImpl.log.warn("AsynchronousFile: " + fileName + " being finalized with opened state");
      }
   }

   // Private ---------------------------------------------------------------------------

   /** */
   @SuppressWarnings("unused")
   private void callbackDone(final AIOCallback callback, final long sequence, final ByteBuffer buffer)
   {
      maxIOSemaphore.release();

      pendingWrites.countDown();

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
            ErrorCallback error = (ErrorCallback)holder;
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
      AsynchronousFileImpl.log.warn("CallbackError: " + errorMessage);

      maxIOSemaphore.release();

      pendingWrites.countDown();

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
      AsynchronousFileImpl.internalPollEvents(handler);
   }

   private void startPoller()
   {
      writeLock.lock();

      try
      {

         if (poller == null)
         {
            pollerLatch.countUp();
            poller = new PollerRunnable();
            try
            {
               pollerExecutor.execute(poller);
            }
            catch (Exception ex)
            {
               AsynchronousFileImpl.log.error(ex.getMessage(), ex);
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
      AsynchronousFileImpl.stopPoller(handler);
      // We need to make sure we won't call close until Poller is
      // completely done, or we might get beautiful GPFs
      pollerLatch.await();
   }
   
   public static FileLock lock(int handle)
   {
      if (flock(handle))
      {
         return new HornetQFileLock(handle);
      }
      else
      {
         return null;
      }
   }

   // Native ----------------------------------------------------------------------------
   
   
   // Functions used for locking files .....
   public static native int openFile(String fileName);
   
   public static native void closeFile(int handle);
   
   private static native boolean flock(int handle);
   // Functions used for locking files ^^^^^^^^

   private static native void resetBuffer(ByteBuffer directByteBuffer, int size);

   public static native void destroyBuffer(ByteBuffer buffer);

   /** Instead of passing the nanoSeconds through the stack call every time, we set it statically inside the native method */
   public static native void setNanoSleepInterval(int nanoseconds);

   public static native void nanoSleep();

   private static native ByteBuffer newNativeBuffer(long size);

   private static native ByteBuffer init(String fileName, int maxIO, Logger logger) throws HornetQException;

   private native long size0(ByteBuffer handle) throws HornetQException;

   private native void write(ByteBuffer handle,
                             long sequence,
                             long position,
                             long size,
                             ByteBuffer buffer,
                             AIOCallback aioPackage) throws HornetQException;

   /** a direct write to the file without the use of libaio's submit. */
   private native void writeInternal(ByteBuffer handle, long positionToWrite, long size, ByteBuffer bytes) throws HornetQException;

   private native void read(ByteBuffer handle, long position, long size, ByteBuffer buffer, AIOCallback aioPackage) throws HornetQException;

   private static native void fill(ByteBuffer handle, long position, int blocks, long size, byte fillChar) throws HornetQException;

   private static native void closeInternal(ByteBuffer handler) throws HornetQException;

   private static native void stopPoller(ByteBuffer handler) throws HornetQException;

   /** A native method that does nothing, and just validate if the ELF dependencies are loaded and on the correct platform as this binary format */
   private static native int getNativeVersion();

   /** Poll asynchronous events from internal queues */
   private static native void internalPollEvents(ByteBuffer handler);

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

      public int compareTo(final CallbackHolder o)
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

      @Override
      public boolean isError()
      {
         return true;
      }

      public ErrorCallback(final long sequence, final AIOCallback callback, final int errorCode, final String message)
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
            pollerLatch.countDown();
         }
      }
   }

}
