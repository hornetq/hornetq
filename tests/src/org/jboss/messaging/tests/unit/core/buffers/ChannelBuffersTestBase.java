/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
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
package org.jboss.messaging.tests.unit.core.buffers;

import static org.jboss.messaging.core.buffers.ChannelBuffers.wrappedBuffer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.jboss.messaging.core.buffers.ChannelBuffer;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 * 
 * Converted to JBM Buffers by Clebert Suconic
 *
 * @version $Rev: 472 $, $Date: 2008-11-14 01:45:53 -0600 (Fri, 14 Nov 2008) $
 */
public abstract class ChannelBuffersTestBase extends UnitTestCase
{

   private static final int CAPACITY = 4096; // Must be even

   private static final int BLOCK_SIZE = 128;

   private long seed;

   private Random random;

   private ChannelBuffer buffer;

   protected abstract ChannelBuffer newBuffer(int capacity);

   @Override
   public void setUp()
   {
      buffer = newBuffer(CAPACITY);
      seed = System.currentTimeMillis();
      random = new Random(seed);
   }

   @Override
   public void tearDown()
   {
      buffer = null;
   }

   public void testInitialState()
   {
      assertEquals(CAPACITY, buffer.capacity());
      assertEquals(0, buffer.readerIndex());
   }

   public void testReaderIndexBoundaryCheck1()
   {
      try
      {
         buffer.writerIndex(0);
      }
      catch (IndexOutOfBoundsException e)
      {
         fail();
      }
      try
      {
         buffer.readerIndex(-1);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testReaderIndexBoundaryCheck2()
   {
      try
      {
         buffer.writerIndex(buffer.capacity());
      }
      catch (IndexOutOfBoundsException e)
      {
         fail();
      }
      try
      {
         buffer.readerIndex(buffer.capacity() + 1);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testReaderIndexBoundaryCheck3()
   {
      try
      {
         buffer.writerIndex(CAPACITY / 2);
      }
      catch (IndexOutOfBoundsException e)
      {
         fail();
      }
      try
      {
         buffer.readerIndex(CAPACITY * 3 / 2);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }

   }

   public void testReaderIndexBoundaryCheck4()
   {
      buffer.writerIndex(0);
      buffer.readerIndex(0);
      buffer.writerIndex(buffer.capacity());
      buffer.readerIndex(buffer.capacity());
   }

   public void testWriterIndexBoundaryCheck1()
   {
      try
      {
         buffer.writerIndex(-1);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }

   }

   public void testWriterIndexBoundaryCheck2()
   {
      try
      {
         buffer.writerIndex(CAPACITY);
         buffer.readerIndex(CAPACITY);
      }
      catch (IndexOutOfBoundsException e)
      {
         fail();
      }
      try
      {
         buffer.writerIndex(buffer.capacity() + 1);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testWriterIndexBoundaryCheck3()
   {
      try
      {
         buffer.writerIndex(CAPACITY);
         buffer.readerIndex(CAPACITY / 2);
      }
      catch (IndexOutOfBoundsException e)
      {
         fail();
      }
      try
      {
         buffer.writerIndex(CAPACITY / 4);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testWriterIndexBoundaryCheck4()
   {
      buffer.writerIndex(0);
      buffer.readerIndex(0);
      buffer.writerIndex(CAPACITY);
   }

   public void testGetByteBoundaryCheck1()
   {
      try
      {
         buffer.getByte(-1);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetByteBoundaryCheck2()
   {
      try
      {
         buffer.getByte(buffer.capacity());
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetShortBoundaryCheck1()
   {
      try
      {
         buffer.getShort(-1);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetShortBoundaryCheck2()
   {
      try
      {
         buffer.getShort(buffer.capacity() - 1);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetMediumBoundaryCheck1()
   {
      try
      {
         buffer.getMedium(-1);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetMediumBoundaryCheck2()
   {
      try
      {
         buffer.getMedium(buffer.capacity() - 2);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetIntBoundaryCheck1()
   {
      try
      {
         buffer.getInt(-1);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetIntBoundaryCheck2()
   {
      try
      {
         buffer.getInt(buffer.capacity() - 3);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetLongBoundaryCheck1()
   {
      try
      {
         buffer.getLong(-1);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetLongBoundaryCheck2()
   {
      try
      {
         buffer.getLong(buffer.capacity() - 7);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetByteArrayBoundaryCheck1()
   {
      try
      {
         buffer.getBytes(-1, new byte[0]);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetByteArrayBoundaryCheck2()
   {
      try
      {
         buffer.getBytes(-1, new byte[0], 0, 0);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetByteArrayBoundaryCheck3()
   {
      byte[] dst = new byte[4];
      buffer.setInt(0, 0x01020304);
      try
      {
         buffer.getBytes(0, dst, -1, 4);
         fail();
      }
      catch (IndexOutOfBoundsException e)
      {
         // Success
      }

      // No partial copy is expected.
      assertEquals(0, dst[0]);
      assertEquals(0, dst[1]);
      assertEquals(0, dst[2]);
      assertEquals(0, dst[3]);
   }

   public void testGetByteArrayBoundaryCheck4()
   {
      byte[] dst = new byte[4];
      buffer.setInt(0, 0x01020304);
      try
      {
         buffer.getBytes(0, dst, 1, 4);
         fail();
      }
      catch (IndexOutOfBoundsException e)
      {
         // Success
      }

      // No partial copy is expected.
      assertEquals(0, dst[0]);
      assertEquals(0, dst[1]);
      assertEquals(0, dst[2]);
      assertEquals(0, dst[3]);
   }

   public void testGetByteBufferBoundaryCheck()
   {
      try
      {
         buffer.getBytes(-1, ByteBuffer.allocate(0));
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testSetIndexBoundaryCheck1()
   {
      try
      {
         buffer.setIndex(-1, CAPACITY);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testSetIndexBoundaryCheck2()
   {
      try
      {
         buffer.setIndex(CAPACITY / 2, CAPACITY / 4);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testSetIndexBoundaryCheck3()
   {
      try
      {
         buffer.setIndex(0, CAPACITY + 1);
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetByteBufferState()
   {
      ByteBuffer dst = ByteBuffer.allocate(4);
      dst.position(1);
      dst.limit(3);

      buffer.setByte(0, (byte)1);
      buffer.setByte(1, (byte)2);
      buffer.setByte(2, (byte)3);
      buffer.setByte(3, (byte)4);
      buffer.getBytes(1, dst);

      assertEquals(3, dst.position());
      assertEquals(3, dst.limit());

      dst.clear();
      assertEquals(0, dst.get(0));
      assertEquals(2, dst.get(1));
      assertEquals(3, dst.get(2));
      assertEquals(0, dst.get(3));
   }

   public void testGetDirectByteBufferBoundaryCheck()
   {
      try
      {
         buffer.getBytes(-1, ByteBuffer.allocateDirect(0));
         fail("Exception expected");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   public void testGetDirectByteBufferState()
   {
      ByteBuffer dst = ByteBuffer.allocateDirect(4);
      dst.position(1);
      dst.limit(3);

      buffer.setByte(0, (byte)1);
      buffer.setByte(1, (byte)2);
      buffer.setByte(2, (byte)3);
      buffer.setByte(3, (byte)4);
      buffer.getBytes(1, dst);

      assertEquals(3, dst.position());
      assertEquals(3, dst.limit());

      dst.clear();
      assertEquals(0, dst.get(0));
      assertEquals(2, dst.get(1));
      assertEquals(3, dst.get(2));
      assertEquals(0, dst.get(3));
   }

   public void testRandomByteAccess()
   {
      for (int i = 0; i < buffer.capacity(); i++)
      {
         byte value = (byte)random.nextInt();
         buffer.setByte(i, value);
      }

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity(); i++)
      {
         byte value = (byte)random.nextInt();
         assertEquals(value, buffer.getByte(i));
      }
   }

   public void testRandomUnsignedByteAccess()
   {
      for (int i = 0; i < buffer.capacity(); i++)
      {
         byte value = (byte)random.nextInt();
         buffer.setByte(i, value);
      }

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity(); i++)
      {
         int value = random.nextInt() & 0xFF;
         assertEquals(value, buffer.getUnsignedByte(i));
      }
   }

   public void testRandomShortAccess()
   {
      for (int i = 0; i < buffer.capacity() - 1; i += 2)
      {
         short value = (short)random.nextInt();
         buffer.setShort(i, value);
      }

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity() - 1; i += 2)
      {
         short value = (short)random.nextInt();
         assertEquals(value, buffer.getShort(i));
      }
   }

   public void testRandomUnsignedShortAccess()
   {
      for (int i = 0; i < buffer.capacity() - 1; i += 2)
      {
         short value = (short)random.nextInt();
         buffer.setShort(i, value);
      }

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity() - 1; i += 2)
      {
         int value = random.nextInt() & 0xFFFF;
         assertEquals(value, buffer.getUnsignedShort(i));
      }
   }

   public void testRandomMediumAccess()
   {
      for (int i = 0; i < buffer.capacity() - 2; i += 3)
      {
         int value = random.nextInt();
         buffer.setMedium(i, value);
      }

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity() - 2; i += 3)
      {
         int value = random.nextInt() << 8 >> 8;
         assertEquals(value, buffer.getMedium(i));
      }
   }

   public void testRandomUnsignedMediumAccess()
   {
      for (int i = 0; i < buffer.capacity() - 2; i += 3)
      {
         int value = random.nextInt();
         buffer.setMedium(i, value);
      }

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity() - 2; i += 3)
      {
         int value = random.nextInt() & 0x00FFFFFF;
         assertEquals(value, buffer.getUnsignedMedium(i));
      }
   }

   public void testRandomIntAccess()
   {
      for (int i = 0; i < buffer.capacity() - 3; i += 4)
      {
         int value = random.nextInt();
         buffer.setInt(i, value);
      }

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity() - 3; i += 4)
      {
         int value = random.nextInt();
         assertEquals(value, buffer.getInt(i));
      }
   }

   public void testRandomUnsignedIntAccess()
   {
      for (int i = 0; i < buffer.capacity() - 3; i += 4)
      {
         int value = random.nextInt();
         buffer.setInt(i, value);
      }

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity() - 3; i += 4)
      {
         long value = random.nextInt() & 0xFFFFFFFFL;
         assertEquals(value, buffer.getUnsignedInt(i));
      }
   }

   public void testRandomLongAccess()
   {
      for (int i = 0; i < buffer.capacity() - 7; i += 8)
      {
         long value = random.nextLong();
         buffer.setLong(i, value);
      }

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity() - 7; i += 8)
      {
         long value = random.nextLong();
         assertEquals(value, buffer.getLong(i));
      }
   }

   public void testSetZero()
   {
      buffer.clear();
      while (buffer.writable())
      {
         buffer.writeByte((byte)0xFF);
      }

      for (int i = 0; i < buffer.capacity();)
      {
         int length = Math.min(buffer.capacity() - i, random.nextInt(32));
         buffer.setZero(i, length);
         i += length;
      }

      for (int i = 0; i < buffer.capacity(); i++)
      {
         assertEquals(0, buffer.getByte(i));
      }
   }

   public void testSequentialByteAccess()
   {
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity(); i++)
      {
         byte value = (byte)random.nextInt();
         assertEquals(i, buffer.writerIndex());
         assertTrue(buffer.writable());
         buffer.writeByte(value);
      }

      assertEquals(0, buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.writable());

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity(); i++)
      {
         byte value = (byte)random.nextInt();
         assertEquals(i, buffer.readerIndex());
         assertTrue(buffer.readable());
         assertEquals(value, buffer.readByte());
      }

      assertEquals(buffer.capacity(), buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.readable());
      assertFalse(buffer.writable());
   }

   public void testSequentialUnsignedByteAccess()
   {
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity(); i++)
      {
         byte value = (byte)random.nextInt();
         assertEquals(i, buffer.writerIndex());
         assertTrue(buffer.writable());
         buffer.writeByte(value);
      }

      assertEquals(0, buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.writable());

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity(); i++)
      {
         int value = random.nextInt() & 0xFF;
         assertEquals(i, buffer.readerIndex());
         assertTrue(buffer.readable());
         assertEquals(value, buffer.readUnsignedByte());
      }

      assertEquals(buffer.capacity(), buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.readable());
      assertFalse(buffer.writable());
   }

   public void testSequentialShortAccess()
   {
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity(); i += 2)
      {
         short value = (short)random.nextInt();
         assertEquals(i, buffer.writerIndex());
         assertTrue(buffer.writable());
         buffer.writeShort(value);
      }

      assertEquals(0, buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.writable());

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity(); i += 2)
      {
         short value = (short)random.nextInt();
         assertEquals(i, buffer.readerIndex());
         assertTrue(buffer.readable());
         assertEquals(value, buffer.readShort());
      }

      assertEquals(buffer.capacity(), buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.readable());
      assertFalse(buffer.writable());
   }

   public void testSequentialUnsignedShortAccess()
   {
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity(); i += 2)
      {
         short value = (short)random.nextInt();
         assertEquals(i, buffer.writerIndex());
         assertTrue(buffer.writable());
         buffer.writeShort(value);
      }

      assertEquals(0, buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.writable());

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity(); i += 2)
      {
         int value = random.nextInt() & 0xFFFF;
         assertEquals(i, buffer.readerIndex());
         assertTrue(buffer.readable());
         assertEquals(value, buffer.readUnsignedShort());
      }

      assertEquals(buffer.capacity(), buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.readable());
      assertFalse(buffer.writable());
   }

   public void testSequentialMediumAccess()
   {
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity() / 3 * 3; i += 3)
      {
         int value = random.nextInt();
         assertEquals(i, buffer.writerIndex());
         assertTrue(buffer.writable());
         buffer.writeMedium(value);
      }

      assertEquals(0, buffer.readerIndex());
      assertEquals(buffer.capacity() / 3 * 3, buffer.writerIndex());
      assertEquals(buffer.capacity() % 3, buffer.writableBytes());

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity() / 3 * 3; i += 3)
      {
         int value = random.nextInt() << 8 >> 8;
         assertEquals(i, buffer.readerIndex());
         assertTrue(buffer.readable());
         assertEquals(value, buffer.readMedium());
      }

      assertEquals(buffer.capacity() / 3 * 3, buffer.readerIndex());
      assertEquals(buffer.capacity() / 3 * 3, buffer.writerIndex());
      assertEquals(0, buffer.readableBytes());
      assertEquals(buffer.capacity() % 3, buffer.writableBytes());
   }

   public void testSequentialUnsignedMediumAccess()
   {
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity() / 3 * 3; i += 3)
      {
         int value = random.nextInt() & 0x00FFFFFF;
         assertEquals(i, buffer.writerIndex());
         assertTrue(buffer.writable());
         buffer.writeMedium(value);
      }

      assertEquals(0, buffer.readerIndex());
      assertEquals(buffer.capacity() / 3 * 3, buffer.writerIndex());
      assertEquals(buffer.capacity() % 3, buffer.writableBytes());

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity() / 3 * 3; i += 3)
      {
         int value = random.nextInt() & 0x00FFFFFF;
         assertEquals(i, buffer.readerIndex());
         assertTrue(buffer.readable());
         assertEquals(value, buffer.readUnsignedMedium());
      }

      assertEquals(buffer.capacity() / 3 * 3, buffer.readerIndex());
      assertEquals(buffer.capacity() / 3 * 3, buffer.writerIndex());
      assertEquals(0, buffer.readableBytes());
      assertEquals(buffer.capacity() % 3, buffer.writableBytes());
   }

   public void testSequentialIntAccess()
   {
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity(); i += 4)
      {
         int value = random.nextInt();
         assertEquals(i, buffer.writerIndex());
         assertTrue(buffer.writable());
         buffer.writeInt(value);
      }

      assertEquals(0, buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.writable());

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity(); i += 4)
      {
         int value = random.nextInt();
         assertEquals(i, buffer.readerIndex());
         assertTrue(buffer.readable());
         assertEquals(value, buffer.readInt());
      }

      assertEquals(buffer.capacity(), buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.readable());
      assertFalse(buffer.writable());
   }

   public void testSequentialUnsignedIntAccess()
   {
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity(); i += 4)
      {
         int value = random.nextInt();
         assertEquals(i, buffer.writerIndex());
         assertTrue(buffer.writable());
         buffer.writeInt(value);
      }

      assertEquals(0, buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.writable());

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity(); i += 4)
      {
         long value = random.nextInt() & 0xFFFFFFFFL;
         assertEquals(i, buffer.readerIndex());
         assertTrue(buffer.readable());
         assertEquals(value, buffer.readUnsignedInt());
      }

      assertEquals(buffer.capacity(), buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.readable());
      assertFalse(buffer.writable());
   }

   public void testSequentialLongAccess()
   {
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity(); i += 8)
      {
         long value = random.nextLong();
         assertEquals(i, buffer.writerIndex());
         assertTrue(buffer.writable());
         buffer.writeLong(value);
      }

      assertEquals(0, buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.writable());

      random.setSeed(seed);
      for (int i = 0; i < buffer.capacity(); i += 8)
      {
         long value = random.nextLong();
         assertEquals(i, buffer.readerIndex());
         assertTrue(buffer.readable());
         assertEquals(value, buffer.readLong());
      }

      assertEquals(buffer.capacity(), buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());
      assertFalse(buffer.readable());
      assertFalse(buffer.writable());
   }

   public void testByteArrayTransfer()
   {
      byte[] value = new byte[BLOCK_SIZE * 2];
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(value);
         buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
      }

      random.setSeed(seed);
      byte[] expectedValue = new byte[BLOCK_SIZE * 2];
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValue);
         int valueOffset = random.nextInt(BLOCK_SIZE);
         buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
         for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue[j], value[j]);
         }
      }
   }

   public void testRandomByteArrayTransfer1()
   {
      byte[] value = new byte[BLOCK_SIZE];
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(value);
         buffer.setBytes(i, value);
      }

      random.setSeed(seed);
      byte[] expectedValueContent = new byte[BLOCK_SIZE];
      ChannelBuffer expectedValue = wrappedBuffer(expectedValueContent);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValueContent);
         buffer.getBytes(i, value);
         for (int j = 0; j < BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue.getByte(j), value[j]);
         }
      }
   }

   public void testRandomByteArrayTransfer2()
   {
      byte[] value = new byte[BLOCK_SIZE * 2];
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(value);
         buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
      }

      random.setSeed(seed);
      byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
      ChannelBuffer expectedValue = wrappedBuffer(expectedValueContent);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValueContent);
         int valueOffset = random.nextInt(BLOCK_SIZE);
         buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
         for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue.getByte(j), value[j]);
         }
      }
   }

   public void testRandomHeapBufferTransfer1()
   {
      byte[] valueContent = new byte[BLOCK_SIZE];
      ChannelBuffer value = wrappedBuffer(valueContent);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(valueContent);
         value.setIndex(0, BLOCK_SIZE);
         buffer.setBytes(i, value);
         assertEquals(BLOCK_SIZE, value.readerIndex());
         assertEquals(BLOCK_SIZE, value.writerIndex());
      }

      random.setSeed(seed);
      byte[] expectedValueContent = new byte[BLOCK_SIZE];
      ChannelBuffer expectedValue = wrappedBuffer(expectedValueContent);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValueContent);
         value.clear();
         buffer.getBytes(i, value);
         assertEquals(0, value.readerIndex());
         assertEquals(BLOCK_SIZE, value.writerIndex());
         for (int j = 0; j < BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue.getByte(j), value.getByte(j));
         }
      }
   }

   public void testRandomHeapBufferTransfer2()
   {
      byte[] valueContent = new byte[BLOCK_SIZE * 2];
      ChannelBuffer value = wrappedBuffer(valueContent);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(valueContent);
         buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
      }

      random.setSeed(seed);
      byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
      ChannelBuffer expectedValue = wrappedBuffer(expectedValueContent);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValueContent);
         int valueOffset = random.nextInt(BLOCK_SIZE);
         buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
         for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue.getByte(j), value.getByte(j));
         }
      }
   }

   public void testRandomByteBufferTransfer()
   {
      ByteBuffer value = ByteBuffer.allocate(BLOCK_SIZE * 2);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(value.array());
         value.clear().position(random.nextInt(BLOCK_SIZE));
         value.limit(value.position() + BLOCK_SIZE);
         buffer.setBytes(i, value);
      }

      random.setSeed(seed);
      ByteBuffer expectedValue = ByteBuffer.allocate(BLOCK_SIZE * 2);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValue.array());
         int valueOffset = random.nextInt(BLOCK_SIZE);
         value.clear().position(valueOffset).limit(valueOffset + BLOCK_SIZE);
         buffer.getBytes(i, value);
         assertEquals(valueOffset + BLOCK_SIZE, value.position());
         for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue.get(j), value.get(j));
         }
      }
   }

   public void testSequentialByteArrayTransfer1()
   {
      byte[] value = new byte[BLOCK_SIZE];
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(value);
         assertEquals(0, buffer.readerIndex());
         assertEquals(i, buffer.writerIndex());
         buffer.writeBytes(value);
      }

      random.setSeed(seed);
      byte[] expectedValue = new byte[BLOCK_SIZE];
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValue);
         assertEquals(i, buffer.readerIndex());
         assertEquals(CAPACITY, buffer.writerIndex());
         buffer.readBytes(value);
         for (int j = 0; j < BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue[j], value[j]);
         }
      }
   }

   public void testSequentialByteArrayTransfer2()
   {
      byte[] value = new byte[BLOCK_SIZE * 2];
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(value);
         assertEquals(0, buffer.readerIndex());
         assertEquals(i, buffer.writerIndex());
         int readerIndex = random.nextInt(BLOCK_SIZE);
         buffer.writeBytes(value, readerIndex, BLOCK_SIZE);
      }

      random.setSeed(seed);
      byte[] expectedValue = new byte[BLOCK_SIZE * 2];
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValue);
         int valueOffset = random.nextInt(BLOCK_SIZE);
         assertEquals(i, buffer.readerIndex());
         assertEquals(CAPACITY, buffer.writerIndex());
         buffer.readBytes(value, valueOffset, BLOCK_SIZE);
         for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue[j], value[j]);
         }
      }
   }

   public void testSequentialHeapBufferTransfer1()
   {
      byte[] valueContent = new byte[BLOCK_SIZE * 2];
      ChannelBuffer value = wrappedBuffer(valueContent);
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(valueContent);
         assertEquals(0, buffer.readerIndex());
         assertEquals(i, buffer.writerIndex());
         buffer.writeBytes(value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
         assertEquals(0, value.readerIndex());
         assertEquals(valueContent.length, value.writerIndex());
      }

      random.setSeed(seed);
      byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
      ChannelBuffer expectedValue = wrappedBuffer(expectedValueContent);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValueContent);
         int valueOffset = random.nextInt(BLOCK_SIZE);
         assertEquals(i, buffer.readerIndex());
         assertEquals(CAPACITY, buffer.writerIndex());
         buffer.readBytes(value, valueOffset, BLOCK_SIZE);
         for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue.getByte(j), value.getByte(j));
         }
         assertEquals(0, value.readerIndex());
         assertEquals(valueContent.length, value.writerIndex());
      }
   }

   public void testSequentialHeapBufferTransfer2()
   {
      byte[] valueContent = new byte[BLOCK_SIZE * 2];
      ChannelBuffer value = wrappedBuffer(valueContent);
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(valueContent);
         assertEquals(0, buffer.readerIndex());
         assertEquals(i, buffer.writerIndex());
         int readerIndex = random.nextInt(BLOCK_SIZE);
         value.readerIndex(readerIndex);
         value.writerIndex(readerIndex + BLOCK_SIZE);
         buffer.writeBytes(value);
         assertEquals(readerIndex + BLOCK_SIZE, value.writerIndex());
         assertEquals(value.writerIndex(), value.readerIndex());
      }

      random.setSeed(seed);
      byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
      ChannelBuffer expectedValue = wrappedBuffer(expectedValueContent);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValueContent);
         int valueOffset = random.nextInt(BLOCK_SIZE);
         assertEquals(i, buffer.readerIndex());
         assertEquals(CAPACITY, buffer.writerIndex());
         value.readerIndex(valueOffset);
         value.writerIndex(valueOffset);
         buffer.readBytes(value, BLOCK_SIZE);
         for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue.getByte(j), value.getByte(j));
         }
         assertEquals(valueOffset, value.readerIndex());
         assertEquals(valueOffset + BLOCK_SIZE, value.writerIndex());
      }
   }

   public void testSequentialByteBufferBackedHeapBufferTransfer1()
   {
      byte[] valueContent = new byte[BLOCK_SIZE * 2];
      ChannelBuffer value = wrappedBuffer(ByteBuffer.allocate(BLOCK_SIZE * 2));
      value.writerIndex(0);
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(valueContent);
         value.setBytes(0, valueContent);
         assertEquals(0, buffer.readerIndex());
         assertEquals(i, buffer.writerIndex());
         buffer.writeBytes(value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
         assertEquals(0, value.readerIndex());
         assertEquals(0, value.writerIndex());
      }

      random.setSeed(seed);
      byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
      ChannelBuffer expectedValue = wrappedBuffer(expectedValueContent);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValueContent);
         int valueOffset = random.nextInt(BLOCK_SIZE);
         value.setBytes(0, valueContent);
         assertEquals(i, buffer.readerIndex());
         assertEquals(CAPACITY, buffer.writerIndex());
         buffer.readBytes(value, valueOffset, BLOCK_SIZE);
         for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue.getByte(j), value.getByte(j));
         }
         assertEquals(0, value.readerIndex());
         assertEquals(0, value.writerIndex());
      }
   }

   public void testSequentialByteBufferBackedHeapBufferTransfer2()
   {
      byte[] valueContent = new byte[BLOCK_SIZE * 2];
      ChannelBuffer value = wrappedBuffer(ByteBuffer.allocate(BLOCK_SIZE * 2));
      value.writerIndex(0);
      buffer.writerIndex(0);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(valueContent);
         value.setBytes(0, valueContent);
         assertEquals(0, buffer.readerIndex());
         assertEquals(i, buffer.writerIndex());
         int readerIndex = random.nextInt(BLOCK_SIZE);
         value.readerIndex(0);
         value.writerIndex(readerIndex + BLOCK_SIZE);
         value.readerIndex(readerIndex);
         buffer.writeBytes(value);
         assertEquals(readerIndex + BLOCK_SIZE, value.writerIndex());
         assertEquals(value.writerIndex(), value.readerIndex());
      }

      random.setSeed(seed);
      byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
      ChannelBuffer expectedValue = wrappedBuffer(expectedValueContent);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValueContent);
         value.setBytes(0, valueContent);
         int valueOffset = random.nextInt(BLOCK_SIZE);
         assertEquals(i, buffer.readerIndex());
         assertEquals(CAPACITY, buffer.writerIndex());
         value.readerIndex(valueOffset);
         value.writerIndex(valueOffset);
         buffer.readBytes(value, BLOCK_SIZE);
         for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue.getByte(j), value.getByte(j));
         }
         assertEquals(valueOffset, value.readerIndex());
         assertEquals(valueOffset + BLOCK_SIZE, value.writerIndex());
      }
   }

   public void testSequentialByteBufferTransfer()
   {
      buffer.writerIndex(0);
      ByteBuffer value = ByteBuffer.allocate(BLOCK_SIZE * 2);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(value.array());
         value.clear().position(random.nextInt(BLOCK_SIZE));
         value.limit(value.position() + BLOCK_SIZE);
         buffer.writeBytes(value);
      }

      random.setSeed(seed);
      ByteBuffer expectedValue = ByteBuffer.allocate(BLOCK_SIZE * 2);
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         random.nextBytes(expectedValue.array());
         int valueOffset = random.nextInt(BLOCK_SIZE);
         value.clear().position(valueOffset).limit(valueOffset + BLOCK_SIZE);
         buffer.readBytes(value);
         assertEquals(valueOffset + BLOCK_SIZE, value.position());
         for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++)
         {
            assertEquals(expectedValue.get(j), value.get(j));
         }
      }
   }

   public void testWriteZero()
   {
      try
      {
         buffer.writeZero(-1);
         fail();
      }
      catch (IllegalArgumentException e)
      {
         // Expected
      }

      buffer.clear();
      while (buffer.writable())
      {
         buffer.writeByte((byte)0xFF);
      }

      buffer.clear();
      for (int i = 0; i < buffer.capacity();)
      {
         int length = Math.min(buffer.capacity() - i, random.nextInt(32));
         buffer.writeZero(length);
         i += length;
      }

      assertEquals(0, buffer.readerIndex());
      assertEquals(buffer.capacity(), buffer.writerIndex());

      for (int i = 0; i < buffer.capacity(); i++)
      {
         assertEquals(0, buffer.getByte(i));
      }
   }

   public void testStreamTransfer1() throws Exception
   {
      byte[] expected = new byte[buffer.capacity()];
      random.nextBytes(expected);

      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         ByteArrayInputStream in = new ByteArrayInputStream(expected, i, BLOCK_SIZE);
         assertEquals(BLOCK_SIZE, buffer.setBytes(i, in, BLOCK_SIZE));
         assertEquals(-1, buffer.setBytes(i, in, 0));
      }

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         buffer.getBytes(i, out, BLOCK_SIZE);
      }

      assertTrue(Arrays.equals(expected, out.toByteArray()));
   }

   public void testStreamTransfer2() throws Exception
   {
      byte[] expected = new byte[buffer.capacity()];
      random.nextBytes(expected);
      buffer.clear();

      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         ByteArrayInputStream in = new ByteArrayInputStream(expected, i, BLOCK_SIZE);
         assertEquals(i, buffer.writerIndex());
         buffer.writeBytes(in, BLOCK_SIZE);
         assertEquals(i + BLOCK_SIZE, buffer.writerIndex());
      }

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         assertEquals(i, buffer.readerIndex());
         buffer.readBytes(out, BLOCK_SIZE);
         assertEquals(i + BLOCK_SIZE, buffer.readerIndex());
      }

      assertTrue(Arrays.equals(expected, out.toByteArray()));
   }

   public void testEquals()
   {
      assertFalse(buffer.equals(null));
      assertFalse(buffer.equals(new Object()));

      byte[] value = new byte[32];
      buffer.setIndex(0, value.length);
      random.nextBytes(value);
      buffer.setBytes(0, value);

      assertEquals(buffer, wrappedBuffer(value));

      value[0]++;
      assertFalse(buffer.equals(wrappedBuffer(value)));
   }

   public void testCompareTo()
   {
      try
      {
         buffer.compareTo(null);
         fail();
      }
      catch (NullPointerException e)
      {
         // Expected
      }

      // Fill the random stuff
      byte[] value = new byte[32];
      random.nextBytes(value);
      // Prevent overflow / underflow
      if (value[0] == 0)
      {
         value[0]++;
      }
      else if (value[0] == -1)
      {
         value[0]--;
      }

      buffer.setIndex(0, value.length);
      buffer.setBytes(0, value);

      assertEquals(0, buffer.compareTo(wrappedBuffer(value)));

      value[0]++;
      assertTrue(buffer.compareTo(wrappedBuffer(value)) < 0);
      value[0] -= 2;
      assertTrue(buffer.compareTo(wrappedBuffer(value)) > 0);
      value[0]++;
   }

   public void testToByteBuffer1()
   {
      byte[] value = new byte[buffer.capacity()];
      random.nextBytes(value);
      buffer.clear();
      buffer.writeBytes(value);

      assertEquals(ByteBuffer.wrap(value), buffer.toByteBuffer());
   }

   public void testToByteBuffer2()
   {
      byte[] value = new byte[buffer.capacity()];
      random.nextBytes(value);
      buffer.clear();
      buffer.writeBytes(value);

      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         assertEquals(ByteBuffer.wrap(value, i, BLOCK_SIZE), buffer.toByteBuffer(i, BLOCK_SIZE));
      }
   }

   public void testToByteBuffers2()
   {
      byte[] value = new byte[buffer.capacity()];
      random.nextBytes(value);
      buffer.clear();
      buffer.writeBytes(value);

      for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE)
      {
         ByteBuffer[] nioBuffers = buffer.toByteBuffers(i, BLOCK_SIZE);
         ByteBuffer nioBuffer = ByteBuffer.allocate(BLOCK_SIZE);
         for (ByteBuffer b : nioBuffers)
         {
            nioBuffer.put(b);
         }
         nioBuffer.flip();

         assertEquals(ByteBuffer.wrap(value, i, BLOCK_SIZE), nioBuffer);
      }
   }

   public void testSkipBytes1()
   {
      buffer.setIndex(CAPACITY / 4, CAPACITY / 2);

      buffer.skipBytes(CAPACITY / 4);
      assertEquals(CAPACITY / 4 * 2, buffer.readerIndex());

      try
      {
         buffer.skipBytes(CAPACITY / 4 + 1);
         fail();
      }
      catch (IndexOutOfBoundsException e)
      {
         // Expected
      }

      // Should remain unchanged.
      assertEquals(CAPACITY / 4 * 2, buffer.readerIndex());
   }

}
