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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.core.journal.IOCriticalErrorListener;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.HornetQThreadFactory;

/**
 *
 * An abstract SequentialFileFactory containing basic functionality for both AIO and NIO SequentialFactories
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public abstract class AbstractSequentialFileFactory implements SequentialFileFactory
{

   // Timeout used to wait executors to shutdown
   protected static final int EXECUTOR_TIMEOUT = 60;

   private static final Logger log = Logger.getLogger(AbstractSequentialFileFactory.class);

   protected final String journalDir;

   protected final TimedBuffer timedBuffer;

   protected final int bufferSize;

   protected final long bufferTimeout;

   private final IOCriticalErrorListener critialErrorListener;

   /**
    * Asynchronous writes need to be done at another executor.
    * This needs to be done at NIO, or else we would have the callers thread blocking for the return.
    * At AIO this is necessary as context switches on writes would fire flushes at the kernel.
    *  */
   protected ExecutorService writeExecutor;

   public AbstractSequentialFileFactory(final String journalDir,
                                        final boolean buffered,
                                        final int bufferSize,
                                        final int bufferTimeout,
                                        final boolean logRates,
                                        final IOCriticalErrorListener criticalErrorListener)
   {
      this.journalDir = journalDir;

      if (buffered)
      {
         timedBuffer = new TimedBuffer(bufferSize, bufferTimeout, logRates);
      }
      else
      {
         timedBuffer = null;
      }
      this.bufferSize = bufferSize;
      this.bufferTimeout = bufferTimeout;
      this.critialErrorListener = criticalErrorListener;
   }

   public void stop()
   {
      if (timedBuffer != null)
      {
         timedBuffer.stop();
      }

      if (isSupportsCallbacks() && writeExecutor != null)
      {
         writeExecutor.shutdown();

         try
         {
            if (!writeExecutor.awaitTermination(AbstractSequentialFileFactory.EXECUTOR_TIMEOUT, TimeUnit.SECONDS))
            {
               AbstractSequentialFileFactory.log.warn("Timed out on AIO writer shutdown",
                                                      new Exception("Timed out on AIO writer shutdown"));
            }
         }
         catch (InterruptedException e)
         {
            throw new HornetQInterruptedException(e);
         }
      }
   }

   public String getDirectory()
   {
      return journalDir;
   }

   public void start()
   {
      if (timedBuffer != null)
      {
         timedBuffer.start();
      }

      if (isSupportsCallbacks())
      {
         writeExecutor = Executors.newSingleThreadExecutor(new HornetQThreadFactory("HornetQ-Asynchronous-Persistent-Writes" + System.identityHashCode(this),
                                                                                    true,
                                                                                    AbstractSequentialFileFactory.getThisClassLoader()));
      }

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.SequentialFileFactory#onIOError(java.lang.Exception, java.lang.String, org.hornetq.core.journal.SequentialFile)
    */
   public void onIOError(int errorCode, String message, SequentialFile file)
   {
      if (critialErrorListener != null)
      {
         critialErrorListener.onIOException(errorCode, message, file);
      }
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.SequentialFileFactory#activate(org.hornetq.core.journal.SequentialFile)
    */
   public void activateBuffer(final SequentialFile file)
   {
      if (timedBuffer != null)
      {
         file.setTimedBuffer(timedBuffer);
      }
   }

   public void flush()
   {
      if (timedBuffer != null)
      {
         timedBuffer.flush();
      }
   }

   public void deactivateBuffer()
   {
      if (timedBuffer != null)
      {
         // When moving to a new file, we need to make sure any pending buffer will be transfered to the buffer
         timedBuffer.flush();
         timedBuffer.setObserver(null);
      }
   }

   public void releaseBuffer(final ByteBuffer buffer)
   {
   }

   /**
    * Create the directory if it doesn't exist yet
    */
   public void createDirs() throws Exception
   {
      File file = new File(journalDir);
      boolean ok = file.mkdirs();
      if (!ok)
      {
         throw new IOException("Failed to create directory " + journalDir);
      }
   }

   public List<String> listFiles(final String extension) throws Exception
   {
      File dir = new File(journalDir);

      FilenameFilter fnf = new FilenameFilter()
      {
         public boolean accept(final File file, final String name)
         {
            return name.endsWith("." + extension);
         }
      };

      String[] fileNames = dir.list(fnf);

      if (fileNames == null)
      {
         throw new IOException("Failed to list: " + journalDir);
      }

      return Arrays.asList(fileNames);
   }

   private static ClassLoader getThisClassLoader()
   {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
      {
         public ClassLoader run()
         {
            return AbstractSequentialFileFactory.class.getClassLoader();
         }
      });

   }

}
