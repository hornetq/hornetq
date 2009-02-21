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

   private static int EXPECTED_NATIVE_VERSION = 17;

   static void addMax(final int io)
   {
      totalMaxIO.addAndGet(io);
   }

   /** For test purposes */
   public static int getTotalMaxIO()
   {
      return totalMaxIO.get();
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

   private volatile Thread poller;

   private int maxIO;

   private final Lock writeLock = new ReentrantReadWriteLock().writeLock();

   private Semaphore writeSemaphore;

   private BufferCallback bufferCallback;

   /**
    *  Warning: Beware of the C++ pointer! It will bite you! :-)
    */
   private long handler;

   // AsynchronousFile implementation
   // ------------------------------------------------------------------------------------

   public void open(final String fileName, final int maxIO)
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
         handler = init(fileName, this.maxIO, log);
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
            Thread currentPoller = poller;
            stopPoller(handler);
            // We need to make sure we won't call close until Poller is
            // completely done, or we might get beautiful GPFs
            currentPoller.join();
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
         write(handler, position, size, directByteBuffer, aioPackage);
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

   // Should we make this method static?
   public ByteBuffer newBuffer(final int size)
   {
      if (size % getBlockSize() != 0)
      {
         throw new RuntimeException("Buffer size needs to be aligned to 512");
      }

      return ByteBuffer.allocateDirect(size);
   }

   public void setBufferCallback(final BufferCallback callback)
   {
      bufferCallback = callback;
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
            poller = new PollerThread();
            try
            {
               poller.start();
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

   public static native void resetBuffer(ByteBuffer directByteBuffer, int size);

   private static native long init(String fileName, int maxIO, Logger logger);

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

   private class PollerThread extends Thread
   {
      PollerThread()
      {
         super("NativePoller for " + fileName);
      }

      @Override
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
         }
      }
   }
}
