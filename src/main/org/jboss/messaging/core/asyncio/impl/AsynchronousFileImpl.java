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

package org.jboss.messaging.core.asyncio.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.messaging.core.asyncio.AIOCallback;
import org.jboss.messaging.core.asyncio.AsynchronousFile;
import org.jboss.messaging.core.asyncio.BufferCallback;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.utils.VariableLatch;

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
   // Static
   // -------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(AsynchronousFileImpl.class);

   private static final AtomicInteger totalMaxIO = new AtomicInteger(0);

   private static boolean loaded = false;

   private static int EXPECTED_NATIVE_VERSION = 19;

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
      String libraries[] = new String[] { "JBMLibAIO", "JBMLibAIO32", "JBMLibAIO64" };

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

   // Attributes
   // ---------------------------------------------------------------------------------

   private boolean opened = false;

   private String fileName;

   private final VariableLatch pollerLatch = new VariableLatch();
   
   private volatile Runnable poller;

   private int maxIO;

   private final Lock writeLock = new ReentrantReadWriteLock().writeLock();

   private Semaphore writeSemaphore;

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

   // AsynchronousFile implementation
   // ------------------------------------------------------------------------------------

   /**
    * @param writeExecutor It needs to be a single Thread executor. If null it will use the user thread to execute write operations
    * @param pollerExecutor The thread pool that will initialize poller handlers
    */
   public AsynchronousFileImpl(Executor writeExecutor, Executor pollerExecutor)
   {
      this.writeExecutor = writeExecutor;
      this.pollerExecutor = pollerExecutor;
   }
   
   public void open(final String fileName, final int maxIO) throws MessagingException
   {
      writeLock.lock();

      try
      {
         if (opened)
         {
            throw new IllegalStateException("AsynchronousFile is already opened");
         }

         this.maxIO = maxIO;
         writeSemaphore = new Semaphore(this.maxIO);

         this.fileName = fileName;

         try
         {
            handler = init(fileName, this.maxIO, log);
         }
         catch (MessagingException e)
         {
            MessagingException ex = null;
            if (e.getCode() == MessagingException.NATIVE_ERROR_CANT_INITIALIZE_AIO)
            {
               ex = new MessagingException(e.getCode(), "Can't initialize AIO. Currently AIO in use = " + totalMaxIO.get() + ", trying to allocate more " + maxIO, e);
            }
            else
            {
               ex = e;
            }
            throw ex;
         }
         opened = true;
         addMax(this.maxIO);
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

         while (!writeSemaphore.tryAcquire(maxIO, 60, TimeUnit.SECONDS))
         {
            log.warn("Couldn't acquire lock after 60 seconds on AIO",
                     new Exception("Warning: Couldn't acquire lock after 60 seconds on AIO"));
         }
         writeSemaphore = null;
         if (poller != null)
         {
            stopPoller(handler);
            // We need to make sure we won't call close until Poller is
            // completely done, or we might get beautiful GPFs
            this.pollerLatch.waitCompletion();
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
      writeSemaphore.acquireUninterruptibly();
      
      if (writeExecutor != null)
      {
         writeExecutor.execute(new Runnable()
         {
            public void run()
            {
               try
               {
                  write(handler, position, size, directByteBuffer, aioCallback);
               }
               catch (MessagingException e)
               {
                  callbackError(aioCallback, e.getCode(), e.getMessage());
               }
               catch (RuntimeException e)
               {
                  callbackError(aioCallback, MessagingException.INTERNAL_ERROR, e.getMessage());
               }
            }
         });
      }
      else
      {
         try
         {
            write(handler, position, size, directByteBuffer, aioCallback);
         }
         catch (MessagingException e)
         {
            callbackError(aioCallback, e.getCode(), e.getMessage());
         }
         catch (RuntimeException e)
         {
            callbackError(aioCallback, MessagingException.INTERNAL_ERROR, e.getMessage());
         }
      }

   }

   public void read(final long position,
                    final long size,
                    final ByteBuffer directByteBuffer,
                    final AIOCallback aioPackage) throws MessagingException
   {
      checkOpened();
      if (poller == null)
      {
         startPoller();
      }
      writeSemaphore.acquireUninterruptibly();
      try
      {
         read(handler, position, size, directByteBuffer, aioPackage);
      }
      catch (MessagingException e)
      {
         // Release only if an exception happened
         writeSemaphore.release();
         throw e;
      }
      catch (RuntimeException e)
      {
         // Release only if an exception happened
         writeSemaphore.release();
         throw e;
      }
   }

   public long size() throws MessagingException
   {
      checkOpened();
      return size0(handler);
   }

   public void fill(final long position, final int blocks, final long size, final byte fillChar) throws MessagingException
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
   
   public static void clearBuffer(ByteBuffer buffer)
   {
      resetBuffer(buffer, buffer.limit());
      buffer.position(0);
   }


   // Private
   // ---------------------------------------------------------------------------------

   /** The JNI layer will call this method, so we could use it to unlock readWriteLocks held in the java layer */
   @SuppressWarnings("unused")
   // Called by the JNI layer.. just ignore the
   // warning
   private void callbackDone(final AIOCallback callback, final ByteBuffer buffer)
   {
      writeSemaphore.release();
      callback.done();
      if (bufferCallback != null)
      {
         bufferCallback.bufferDone(buffer);
      }
   }

   @SuppressWarnings("unused")
   // Called by the JNI layer.. just ignore the
   // warning
   private void callbackError(final AIOCallback callback, final int errorCode, final String errorMessage)
   {
      log.warn("CallbackError: " + errorMessage);
      writeSemaphore.release();
      callback.onError(errorCode, errorMessage);
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
   // Native
   // ------------------------------------------------------------------------------------------

   private static native void resetBuffer(ByteBuffer directByteBuffer, int size);


   // Should we make this method static?
	public static native void destroyBuffer(ByteBuffer buffer);
	
	// Should we make this method static?
	private static native ByteBuffer newNativeBuffer(long size);
	
   
   
   private static native long init(String fileName, int maxIO, Logger logger) throws MessagingException;

   private native long size0(long handle) throws MessagingException;

   private native void write(long handle, long position, long size, ByteBuffer buffer, AIOCallback aioPackage) throws MessagingException;

   private native void read(long handle, long position, long size, ByteBuffer buffer, AIOCallback aioPackage) throws MessagingException;

   private static native void fill(long handle, long position, int blocks, long size, byte fillChar) throws MessagingException;

   private static native void closeInternal(long handler) throws MessagingException;

   private static native void stopPoller(long handler) throws MessagingException;

   /** A native method that does nothing, and just validate if the ELF dependencies are loaded and on the correct platform as this binary format */
   private static native int getNativeVersion();
   

   /** Poll asynchrounous events from internal queues */
   private static native void internalPollEvents(long handler);

   // Inner classes
   // -----------------------------------------------------------------------------------------

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
