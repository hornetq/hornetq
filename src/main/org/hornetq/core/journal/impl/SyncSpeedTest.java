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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.hornetq.core.logging.Logger;

/**
 * A SyncSpeedTest
 * 
 * This class just provides some diagnostics on how fast your disk can sync
 * Useful when determining performance issues
 *
 * @author tim fox
 *
 *
 */
public class SyncSpeedTest
{
   private static final Logger log = Logger.getLogger(SyncSpeedTest.class);
   
   public static void main(final String[] args)
   {
      try
      {
         new SyncSpeedTest().run();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
   
   public void run() throws Exception
   {
      int fileSize = 1024 * 1024 * 100;
      
      int recordSize = 1024;
      
      int its = 10 * 1024;
      
      File file = new File("sync-speed-test.dat");
      
      if (file.exists())
      {
         file.delete();
      }
      
      RandomAccessFile rfile = new RandomAccessFile(file, "rw");
      
      FileChannel channel = rfile.getChannel();
      
      ByteBuffer bb = generateBuffer(fileSize, (byte)'x');
      
      write(bb, channel, fileSize);
            
      channel.force(false);
      
      channel.position(0);
      
      MappedByteBuffer mappedBB = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
      
      mappedBB.load();
      // mappedBB.order(java.nio.ByteOrder.LITTLE_ENDIAN);
      System.out.println("isLoaded=" + mappedBB.isLoaded() + "; isDirect=" + mappedBB.isDirect() + "; byteOrder=" + mappedBB.order());
      
      ByteBuffer bb1 = generateBuffer(recordSize, (byte)'h');
            
      System.out.println("Measuring");
      
      long start = System.currentTimeMillis();
      
      for (int i = 0; i < its; i++)
      {
        bb1.flip();
        mappedBB.position(0);
        mappedBB.put(bb1);
        mappedBB.force();
        
         //write(bb1, channel, recordSize);
         // channel.force(false);
      }
      
      long end = System.currentTimeMillis();
      
      double rate = 1000 * ((double)its) / (end - start);
      
      System.out.println("Rate of " + rate + " syncs per sec");
      file.delete();
   }
      
//   public void run() throws Exception
//   {  
//      log.info("******* Starting file sync speed test *******");
//      
//      int fileSize = 1024 * 1024 * 10;
//      
//      int recordSize = 1024;
//      
//      int its = 10 * 1024;
//      
//      File file = new File("sync-speed-test.dat");
//      
//      if (file.exists())
//      {
//         file.delete();
//      }
//      
//      RandomAccessFile rfile = new RandomAccessFile(file, "rw");
//      
//      FileChannel channel = rfile.getChannel();
//      
//      ByteBuffer bb = generateBuffer(fileSize, (byte)'x');
//      
//      write(bb, channel, fileSize);
//            
//      channel.force(false);
//      
//      channel.position(0);
//      
//      ByteBuffer bb1 = generateBuffer(recordSize, (byte)'h');
//            
//      log.info("Measuring");
//      
//      long start = System.currentTimeMillis();
//      
//      for (int i = 0; i < its; i++)
//      {
//         write(bb1, channel, recordSize);
//         
//         channel.force(false);
//      }
//      
//      long end = System.currentTimeMillis();
//      
//      double rate = 1000 * ((double)its) / (end - start);
//      
//      log.info("Rate of " + rate + " syncs per sec");
//      
//      rfile.close();
//      
//      file.delete();
//                  
//      log.info("****** test complete *****");
//   }
   
   private void write(final ByteBuffer buffer, final FileChannel channel, final int size) throws Exception
   {
      buffer.flip();
      
      channel.write(buffer);
   }
   
   private ByteBuffer generateBuffer(final int size, final byte ch)
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(size);

      for (int i = 0; i < size; i++)
      {
         bb.put(ch);
      }
      
      return bb;
   }    
}
