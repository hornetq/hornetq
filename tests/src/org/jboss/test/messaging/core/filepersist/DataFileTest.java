/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

package org.jboss.test.messaging.core.filepersist;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.filepersist.DataFile;
import org.jboss.messaging.core.filepersist.BlockIndex;
import java.io.RandomAccessFile;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class DataFileTest extends MessagingTestCase
{

   // Constants ------------------------------------------------------------------------------------

   File fileIndex = new File("/tmp/file-index.bin");
   File fileData = new File("/tmp/file-data.bin");

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public DataFileTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testAdd() throws Exception
   {
      DataFile transactioned = new DataFile(fileIndex, fileData);

      assertEquals(BlockIndex.REGISTER_SIZE, transactioned.getPosition(0));

      ByteBuffer buffer = ByteBuffer.allocateDirect(4 * 5);
      for (int i = 0; i < 5; i++)
      {
         buffer.putInt(i);
      }
      buffer.mark();


      BlockIndex firstBlock = transactioned.addBlock(buffer);
      BlockIndex currentBlock = firstBlock;
      for (int i = 0; i < 1000; i++)
      {
         currentBlock = transactioned.addBlock(buffer, currentBlock);
      }

      Iterator iter = transactioned.iterateIndexes();
      assertFalse(iter.hasNext());

      transactioned.confirmBlock(firstBlock);

      int elements = 0;

      for (iter = transactioned.iterateIndexes(); iter.hasNext();)
      {
         iter.next();
         elements++;
      }

      assertEquals(1001, elements);

      currentBlock = transactioned.addBlock(buffer);

      elements = 0;

      for (iter = transactioned.iterateIndexes(); iter.hasNext();)
      {
         iter.next();
         elements++;
      }

      assertEquals(1001, elements);

      transactioned.confirmBlock(currentBlock);

      elements = 0;

      for (iter = transactioned.iterateIndexes(); iter.hasNext();)
      {
         iter.next();
         elements++;
      }

      assertEquals(1002, elements);

      transactioned.close();
   }

   public void testAddMultiThread() throws Exception
   {
      final DataFile transactioned = new DataFile(fileIndex, fileData);
      final Object semaphore = new Object();
      final ArrayList failures = new ArrayList();

      Thread threads[] = new Thread[20];

      for (int i=0;i<threads.length;i++)
      {
         threads[i] = new Thread()
         {
            public void run()
            {
               try
               {
                  ByteBuffer buffer = ByteBuffer.allocate(100);
                  for (byte i=0;i<100;i++)
                  {
                     buffer.put(i);
                  }
                  synchronized (semaphore)
                  {
                     semaphore.wait();
                  }

                  BlockIndex first = transactioned.addBlock(buffer);
                  BlockIndex current = first;

                  for (byte i=0;i<99;i++)
                  {
                     current = transactioned.addBlock(buffer, current);
                  }

                  transactioned.confirmBlock(first);
                  
               }
               catch (Exception e)
               {
                  log.error(e);
                  failures.add(e);
               }
            }
         };

      }

      for (int counter=0;counter<threads.length;counter++)
      {
         threads[counter].start();
      }


      Thread.sleep(2000);

      synchronized (semaphore)
      {
         semaphore.notifyAll();
      }

      for (int counter=0;counter<threads.length;counter++)
      {
         threads[counter].join();
      }

      if (failures.size()>0)
      {
         throw (Exception) failures.get(0);
      }

      int elements = 0;

      HashSet set = new HashSet();
      
      for (Iterator iter = transactioned.iterateIndexes(); iter.hasNext();)
      {
         BlockIndex index = (BlockIndex)iter.next();
         set.add(new Integer(index.getBlockId()));
         elements++;
      }

      assertEquals(threads.length * 100, elements);

      for (int counter = 0; counter < threads.length * 100; counter++)
      {
         Integer intKey = new Integer(counter);
         assertTrue("Could not find intKey=" + intKey, set.contains(intKey));
      }


   }

   /*public void testDeleteme() throws Exception
   {
      File fileIndex = new File("/tmp/file-tmp.bin");
      deleteFile(fileIndex);
      RandomAccessFile file1 = new RandomAccessFile(fileIndex,"rw");

      ByteBuffer buffer = ByteBuffer.allocate(100*4);
      for (int i=0;i<100;i++)
      {
         buffer.putInt(i);
      }

      System.out.println("pos(1) = " + buffer.position() + " lim = " + buffer.limit());
      buffer.rewind();
      System.out.println("pos(2) = " + buffer.position() + " lim = " + buffer.limit());

      for (int i=0;i<100;i++)
      {
         buffer.putInt(i);
      }

      buffer.rewind();
      System.out.println("pos(3) = " + buffer.position() + " lim = " + buffer.limit());


      FileChannel channel = file1.getChannel();

      channel.write(buffer);

      System.out.println("Size on file1=" + file1.length());

      file1.close();


   } */

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      deleteFile(fileIndex);
      deleteFile(fileData);
   }

   protected void deleteFile(File file)
   {
      try
      {
         file.delete();
      }
      catch (Exception e)
      {

      }
   }
   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
