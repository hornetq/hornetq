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

import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;

/**
 * 
 * A NIOSequentialFileFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class NIOSequentialFileFactory extends AbstractSequentialFactory implements SequentialFileFactory
{
   private static final Logger log = Logger.getLogger(NIOSequentialFileFactory.class);


   public NIOSequentialFileFactory(final String journalDir)
   {
      this(journalDir,
           false,
           ConfigurationImpl.DEFAULT_JOURNAL_BUFFER_SIZE,
           ConfigurationImpl.DEFAULT_JOURNAL_BUFFER_TIMEOUT,
           ConfigurationImpl.DEFAULT_JOURNAL_FLUSH_SYNC,
           false);
   }

   public NIOSequentialFileFactory(final String journalDir, boolean buffered)
   {
      this(journalDir,
           buffered,
           ConfigurationImpl.DEFAULT_JOURNAL_BUFFER_SIZE,
           ConfigurationImpl.DEFAULT_JOURNAL_BUFFER_TIMEOUT,
           ConfigurationImpl.DEFAULT_JOURNAL_FLUSH_SYNC,
           false);
   }

   public NIOSequentialFileFactory(final String journalDir,
                                   final boolean buffered,
                                   final int bufferSize,
                                   final long bufferTimeout,
                                   final boolean flushOnSync,
                                   final boolean logRates)
   {
      super(journalDir, buffered, bufferSize, bufferTimeout, flushOnSync, logRates);
   }

   // maxIO is ignored on NIO
   public SequentialFile createSequentialFile(final String fileName, final int maxIO)
   {
      return new NIOSequentialFile(this, journalDir, fileName);
   }

   public boolean isSupportsCallbacks()
   {
      return timedBuffer != null;
   }

   public ByteBuffer newBuffer(final int size)
   {
      return ByteBuffer.allocate(size);
   }

   public void clearBuffer(final ByteBuffer buffer)
   {
      final int limit = buffer.limit();
      buffer.rewind();

      for (int i = 0; i < limit; i++)
      {
         buffer.put((byte)0);
      }

      buffer.rewind();
   }

   public ByteBuffer wrapBuffer(final byte[] bytes)
   {
      return ByteBuffer.wrap(bytes);
   }

   public int getAlignment()
   {
      return 1;
   }

   public int calculateBlockSize(final int bytes)
   {
      return bytes;
   }

}
