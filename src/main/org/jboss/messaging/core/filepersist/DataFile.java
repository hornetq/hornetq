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

package org.jboss.messaging.core.filepersist;

import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * This class can play with BlockIndex
 * A BlockIndex could be added, and later be confirmed.
 * When a block is confirmed its status is set to confirmed, and the list is then updated
 *
 * (This is just an experiment.. there is a lot of work to do here..
 *  For example, we need to support multiple files...
 *  and multiple messages in a single block)
 *
 * I think we should support writing messages in blocks, and confirm a single block.
 *
 * At this point confirming a block is a slow operation, but we can improve this... maybe avoiding
 * double linkes lists what forces two updates on each insert
 * (what is easy to support at this point).
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class DataFile
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Lock used for while the index is being updated. You can't have multiple handles 
   Object lockIndex = new Object();

   // We reuse the same instance of ByteBuffer used on indexes since the access is synchronized 
   ByteBuffer indexBuffer = ByteBuffer.allocate(BlockIndex.REGISTER_SIZE);

   int numberOfBlocks;

   FileChannel indexChannel;
   FileChannel dataChannel;

   RandomAccessFile index;
   RandomAccessFile data;

   BlockIndex root;
   BlockIndex last;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public DataFile(File indexFile, File dataFile) throws IOException
   {
      if (indexFile.exists() && dataFile.exists())
      {
         recover(indexFile, dataFile);
      }
      else
      {
         init(indexFile, dataFile);
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public BlockIndex addBlock(ByteBuffer dataBuffer) throws IOException
   {
      return addBlock(dataBuffer, null);
   }

   /**
    * Use precedent only when adding multiple blocks as part of the same confirmation.
    * Say if you are adding 10 messages, the first message will have no precedence until you
    * confirm it. You don't need to confirm subsequent messages.
    *  */
   public BlockIndex addBlock(ByteBuffer dataBuffer, BlockIndex precedent) throws IOException
   {
      BlockIndex block = null;
      
      synchronized (lockIndex)
      {
         block = new BlockIndex(numberOfBlocks++, dataBuffer.capacity(), dataChannel.size());
         if (precedent!=null)
         {
            precedent.setNextBlock(block.getBlockId());
            block.setPreviousBlock(precedent.getBlockId());
            block.setConfirmed(true);
            updateIndex(precedent);
         }
         updateIndex(block);
      }

      dataBuffer.rewind();
      dataChannel.write(dataBuffer, block.getFilePosition());
      dataChannel.force(false);

      return block;
   }

   public void confirmBlock(BlockIndex block) throws IOException
   {
      if (block.isConfirmed())
      {
         throw new IOException("Block already confirmed!");
      }

      synchronized (lockIndex)
      {
         last.setNextBlock(block.getBlockId());
         BlockIndex currentBlock = null;
         for (Iterator iter = new IteratorImpl(last);iter.hasNext();)
         {
            currentBlock = (BlockIndex)iter.next();
         }
         if (currentBlock==null)
         {
            throw new IOException("Couldn't find last element on index");
         }
         updateIndex(last);
         last = (BlockIndex)currentBlock.clone();
      }
   }

   public BlockIndex readBlock (int blockId) throws IOException
   {
      return readBlock(new BlockIndex(), blockId);
   }

   public BlockIndex readBlock(BlockIndex blockToRead, int blockId) throws IOException
   {
      synchronized (lockIndex)
      {
         indexBuffer.rewind();
         indexChannel.read(indexBuffer,getPosition(blockId));
         indexBuffer.rewind();
         blockToRead.readFromBuffer(indexBuffer);
         return blockToRead;
      }
   }

   public void close() throws IOException
   {
      index.close();
      data.close();
   }

   public long getPosition(int blockId)
   {
      return (blockId+1) * BlockIndex.REGISTER_SIZE;
   }

   public Iterator iterateIndexes()
   {
      return new IteratorImpl();
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void recover(File indexFile, File dataFile) throws IOException
   {
      index = new RandomAccessFile(indexFile,"rw");
      data = new RandomAccessFile(dataFile,"rw");
      indexChannel = index.getChannel();
      dataChannel = data.getChannel();

      root = readBlock(-1);
   }

   protected void init(File indexFile, File dataFile) throws IOException
   {
      index = new RandomAccessFile(indexFile,"rw");
      data = new RandomAccessFile(dataFile,"rw");
      indexChannel = index.getChannel();
      dataChannel = data.getChannel();
      root = new BlockIndex(-1,-1,-1l);
      last = root;
      updateIndex(root);
   }

   // Private --------------------------------------------------------------------------------------

   /** This method should be called within a synchronized(lockIndex)*/
   private void updateIndex(BlockIndex block) throws IOException
   {
      indexBuffer.rewind();

      block.writeToBuffer(indexBuffer);

      indexBuffer.rewind();

      indexChannel.write(indexBuffer, getPosition(block.getBlockId()));

      indexChannel.force(false);
   }

   // Inner classes --------------------------------------------------------------------------------

   class IteratorImpl implements Iterator
   {

      BlockIndex currentBlock;

      IteratorImpl()
      {
         currentBlock = (BlockIndex) DataFile.this.root.clone();
      }

      IteratorImpl(BlockIndex startAt)
      {
         currentBlock = startAt;
      }

      public boolean hasNext()
      {
         return currentBlock.getNextBlock()>=0;
      }

      public Object next()
      {
         try
         {
            if (!hasNext())
            {
               throw new IllegalStateException("Already reached end of blocks");
            }
            currentBlock = DataFile.this.readBlock(currentBlock.getNextBlock());
            return currentBlock;
         }
         catch (IOException e)
         {
            throw new RuntimeException(e);
         }
      }

      public void remove()
      {
         throw new RuntimeException("Not supported!");
      }
   }

}
