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

package org.hornetq.core.journal;

import java.nio.ByteBuffer;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.impl.TimedBuffer;

/**
 * 
 * A SequentialFile
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * 
 */
public interface SequentialFile
{
   /*
    * Creates the file if it doesn't already exist, then opens it
    */
   void open() throws Exception;

   boolean isOpen();

   boolean exists();

   /**
    * The maximum number of simultaneous writes accepted
    * @param maxIO
    * @throws Exception
    */
   void open(int maxIO, boolean useExecutor) throws Exception;

   boolean fits(int size);

   int getAlignment() throws Exception;

   int calculateBlockStart(int position) throws Exception;

   String getFileName();

   void fill(int position, int size, byte fillCharacter) throws Exception;

   void delete() throws Exception;

   void write(HornetQBuffer bytes, boolean sync, IOAsyncTask callback) throws Exception;

   void write(HornetQBuffer bytes, boolean sync) throws Exception;

   void write(EncodingSupport bytes, boolean sync, IOAsyncTask callback) throws Exception;

   void write(EncodingSupport bytes, boolean sync) throws Exception;

   /** Write directly to the file without using any buffer */
   void writeDirect(ByteBuffer bytes, boolean sync, IOAsyncTask callback);

   /** Write directly to the file without using any buffer */
   void writeDirect(ByteBuffer bytes, boolean sync) throws Exception;

   int read(ByteBuffer bytes, IOAsyncTask callback) throws Exception;

   int read(ByteBuffer bytes) throws Exception;

   void position(long pos) throws Exception;

   long position() throws Exception;

   void close() throws Exception;

   void waitForClose() throws Exception;

   void sync() throws Exception;

   long size() throws Exception;

   void renameTo(String newFileName) throws Exception;

   SequentialFile copy();

   void setTimedBuffer(TimedBuffer buffer);
}
