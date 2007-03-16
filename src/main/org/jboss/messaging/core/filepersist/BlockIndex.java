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

import java.nio.ByteBuffer;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class BlockIndex implements Cloneable
{

   // Constants ------------------------------------------------------------------------------------

   public static final int REGISTER_SIZE = 4 * 4 + 8 + 1;

   // Attributes -----------------------------------------------------------------------------------

   /** Immutable attribute */ 
   private int blockId;
   
   /** Immutable attribute, you can't change a block's size after allocated */
   private int blockSize;

   /** Immutable attribute, you can't change where a block is written after  */
   private long filePosition;

   private boolean confirmed;
   
   private int nextBlock=-1;

   private int previousBlock=-1;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   BlockIndex()
   {
   }

   public BlockIndex(int blockId, int blockSize, long filePosition)
   {
      this.blockId = blockId;
      this.blockSize = blockSize;
      this.filePosition = filePosition;
   }

   public BlockIndex(int blockId, int blockSize, long filePosition,
                     boolean confirmed, int nextBlock, int previousBlock)
   {
      this(blockId, blockSize, filePosition);
      this.confirmed = confirmed;
      this.nextBlock = nextBlock;
      this.previousBlock = previousBlock;
   }

   // Public ---------------------------------------------------------------------------------------


   public int getBlockId()
   {
      return blockId;
   }

   public int getBlockSize()
   {
      return blockSize;
   }

   public long getFilePosition()
   {
      return filePosition;
   }

   public int getNextBlock()
   {
      return nextBlock;
   }

   public void setNextBlock(int nextBlock)
   {
      this.nextBlock = nextBlock;
   }


   public boolean isConfirmed()
   {
      return confirmed;
   }

   public void setConfirmed(boolean confirmed)
   {
      this.confirmed = confirmed;
   }


   public int getPreviousBlock()
   {
      return previousBlock;
   }

   public void setPreviousBlock(int previousBlock)
   {
      this.previousBlock = previousBlock;
   }

   public void writeToBuffer(ByteBuffer buffer)
   {
      buffer.putInt(nextBlock);
      buffer.putInt(previousBlock);
      buffer.put(confirmed?(byte)1:(byte)0);
      buffer.putLong(filePosition);
      buffer.putInt(blockSize);
      buffer.putInt(blockId);
   }

   public void readFromBuffer(ByteBuffer buffer)
   {
      nextBlock = buffer.getInt();
      previousBlock = buffer.getInt();
      confirmed = (buffer.get()==(byte)1);
      filePosition = buffer.getLong();
      blockSize = buffer.getInt();
      blockId = buffer.getInt();
   }

   public Object clone()
   {
      return new BlockIndex(blockId, blockSize, filePosition, confirmed, nextBlock, previousBlock);
   }

   public String toString()
   {
      return "BlockIndex[" + this.blockId + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
