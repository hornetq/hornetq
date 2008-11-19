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

package org.jboss.messaging.tests.integration.chunkmessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.FileClientMessage;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.util.DataConstants;
import org.jboss.messaging.util.SimpleString;

/**
 * A ChunkTestBase
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Oct 29, 2008 11:43:52 AM
 *
 *
 */
public class ChunkTestBase extends ServiceTestBase
{

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(ChunkTestBase.class);

   protected final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
      deleteData();
   }

   protected void testChunks(final boolean realFiles,
                             final boolean useFile,
                             final int numberOfMessages,
                             final int numberOfIntegers,
                             final boolean sendingBlocking,
                             final int waitOnConsumer,
                             final long delayDelivery) throws Exception
   {
      testChunks(realFiles,
                 useFile,
                 numberOfMessages,
                 numberOfIntegers,
                 sendingBlocking,
                 waitOnConsumer,
                 delayDelivery,
                 false);
   }

   protected void testChunks(final boolean realFiles,
                             final boolean useFile,
                             final int numberOfMessages,
                             final int numberOfIntegers,
                             final boolean sendingBlocking,
                             final int waitOnConsumer,
                             final long delayDelivery,
                             final boolean testTime) throws Exception
   {

      clearData();

      messagingService = createService(realFiles);
      messagingService.start();

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         if (sendingBlocking)
         {
            sf.setBlockOnNonPersistentSend(true);
            sf.setBlockOnPersistentSend(true);
            sf.setBlockOnAcknowledge(true);
         }

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true, false, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         if (useFile)
         {
            File tmpData = createLargeFile(temporaryDir, "someFile.dat", numberOfIntegers);

            for (int i = 0; i < numberOfMessages; i++)
            {
               ClientMessage message = session.createFileMessage(true);
               ((FileClientMessage)message).setFile(tmpData);
               message.putIntProperty(new SimpleString("counter-message"), i);
               long timeStart = System.currentTimeMillis();
               if (delayDelivery > 0)
               {
                  long time = System.currentTimeMillis();
                  message.putLongProperty(new SimpleString("original-time"), time);
                  message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time + delayDelivery);

                  producer.send(message);
               }
               else
               {
                  producer.send(message);
               }

               if (testTime)
               {
                  System.out.println("Message sent in " + (System.currentTimeMillis() - timeStart));
               }
            }
         }
         else
         {
            for (int i = 0; i < numberOfMessages; i++)
            {
               ClientMessage message = session.createClientMessage(true);
               message.putIntProperty(new SimpleString("counter-message"), i);
               message.setBody(createLargeBuffer(numberOfIntegers));
               long timeStart = System.currentTimeMillis();
               if (delayDelivery > 0)
               {
                  long time = System.currentTimeMillis();
                  message.putLongProperty(new SimpleString("original-time"), time);
                  message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time + delayDelivery);
                  
                  producer.send(message);
               }
               else
               {
                  producer.send(message);
               }
               if (testTime)
               {
                  System.out.println("Message sent in " + (System.currentTimeMillis() - timeStart));
               }
            }
         }

         session.close();

         if (realFiles)
         {
            messagingService.stop();

            messagingService = createService(realFiles);
            messagingService.start();

            sf = createInVMFactory();
         }

         session = sf.createSession(null, null, false, true, true, false, 0);

         ClientConsumer consumer = null;

         if (realFiles)
         {
            consumer = session.createFileConsumer(new File(clientLargeMessagesDir), ADDRESS);
         }
         else
         {
            consumer = session.createConsumer(ADDRESS);
         }

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {
            long start = System.currentTimeMillis();

            ClientMessage message = consumer.receive(waitOnConsumer + delayDelivery);

            assertNotNull(message);
            
            if (realFiles)
            {
               assertTrue (message instanceof FileClientMessage);
            }

            if (testTime)
            {
               System.out.println("Message received in " + (System.currentTimeMillis() - start));
            }
            start = System.currentTimeMillis();

            if (delayDelivery > 0)
            {
               long originalTime = (Long)message.getProperty(new SimpleString("original-time"));
               assertTrue((System.currentTimeMillis() - originalTime) + "<" + delayDelivery,
                          System.currentTimeMillis() - originalTime >= delayDelivery);
            }

            message.acknowledge();

            assertNotNull(message);

            if (delayDelivery <= 0)
            { // right now there is no guarantee of ordered delivered on multiple scheduledMessages
               assertEquals(i, ((Integer)message.getProperty(new SimpleString("counter-message"))).intValue());
            }

            if (!testTime)
            {
               if (message instanceof FileClientMessage)
               {
                  checkFileRead(((FileClientMessage)message).getFile(), numberOfIntegers);
               }
               else
               {
                  MessagingBuffer buffer = message.getBody();
                  buffer.rewind();
                  assertEquals(numberOfIntegers * DataConstants.SIZE_INT, buffer.limit());
                  for (int b = 0; b < numberOfIntegers; b++)
                  {
                     assertEquals(b, buffer.getInt());
                  }
               }
            }
         }

         session.close();

         validateNoFilesOnLargeDir();
      }
      finally
      {
         try
         {
            messagingService.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   protected MessagingBuffer createLargeBuffer(final int numberOfIntegers)
   {
      ByteBuffer ioBuffer = ByteBuffer.allocate(DataConstants.SIZE_INT * numberOfIntegers);
      MessagingBuffer body = new ByteBufferWrapper(ioBuffer);

      for (int i = 0; i < numberOfIntegers; i++)
      {
         body.putInt(i);
      }
      body.flip();

      return body;

   }

   protected FileClientMessage createLargeClientMessage(final ClientSession session, final int numberOfIntegers) throws Exception
   {

      FileClientMessage clientMessage = session.createFileMessage(true);

      File tmpFile = createLargeFile(temporaryDir, "tmpUpload.data", numberOfIntegers);

      clientMessage.setFile(tmpFile);

      return clientMessage;
   }

   /**
    * @param name
    * @param numberOfIntegers
    * @return
    * @throws FileNotFoundException
    * @throws IOException
    */
   protected File createLargeFile(final String directory, final String name, final int numberOfIntegers) throws FileNotFoundException,
                                                                                                        IOException
   {
      File tmpFile = new File(directory + "/" + name);

      log.info("Creating file " + tmpFile);

      RandomAccessFile random = new RandomAccessFile(tmpFile, "rw");
      FileChannel channel = random.getChannel();

      ByteBuffer buffer = ByteBuffer.allocate(4 * 1000);

      for (int i = 0; i < numberOfIntegers; i++)
      {
         if (buffer.position() > 0 && i % 1000 == 0)
         {
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
         }
         buffer.putInt(i);
      }

      if (buffer.position() > 0)
      {
         buffer.flip();
         channel.write(buffer);
      }

      channel.close();
      random.close();

      log.info("file " + tmpFile + " created");

      return tmpFile;
   }

   /**
    * @param session
    * @param queueToRead
    * @param numberOfIntegers
    * @throws MessagingException
    * @throws FileNotFoundException
    * @throws IOException
    */
   protected void readMessage(final ClientSession session, final SimpleString queueToRead, final int numberOfIntegers) throws MessagingException,
                                                                                                                      FileNotFoundException,
                                                                                                                      IOException
   {
      session.start();

      ClientConsumer consumer = session.createFileConsumer(new File(clientLargeMessagesDir), queueToRead);

      ClientMessage clientMessage = consumer.receive(5000);

      assertNotNull(clientMessage);
      
      if (!(clientMessage instanceof FileClientMessage))
      {
         System.out.println("Size = " + clientMessage.getBodySize());
      }

      
      if (clientMessage instanceof FileClientMessage)
      {
         assertTrue(clientMessage instanceof FileClientMessage);
   
         FileClientMessage fileClientMessage = (FileClientMessage)clientMessage;
   
         assertNotNull(fileClientMessage);
         File receivedFile = fileClientMessage.getFile();
   
         checkFileRead(receivedFile, numberOfIntegers);

      }
      
      clientMessage.acknowledge();
      
      session.commit();

      consumer.close();
   }

   /**
    * @param receivedFile
    * @throws FileNotFoundException
    * @throws IOException
    */
   protected void checkFileRead(final File receivedFile, final int numberOfIntegers) throws FileNotFoundException,
                                                                                    IOException
   {
      RandomAccessFile random2 = new RandomAccessFile(receivedFile, "r");
      FileChannel channel2 = random2.getChannel();

      ByteBuffer buffer2 = ByteBuffer.allocate(1000 * 4);

      channel2.position(0l);

      for (int i = 0; i < numberOfIntegers;)
      {
         channel2.read(buffer2);

         buffer2.flip();
         for (int j = 0; j < buffer2.limit() / 4; j++, i++)
         {
            assertEquals(i, buffer2.getInt());
         }

         buffer2.clear();
      }

      channel2.close();
   }

   /**
    * Deleting a file on LargeDire is an asynchronous process. Wee need to keep looking for a while if the file hasn't been deleted yet
    */
   protected void validateNoFilesOnLargeDir() throws Exception
   {
      File largeMessagesFileDir = new File(largeMessagesDir);

      // Deleting the file is async... we keep looking for a period of the time until the file is really gone
      for (int i = 0; i < 100; i++)
      {
         if (largeMessagesFileDir.listFiles().length > 0)
         {
            Thread.sleep(10);
         }
         else
         {
            break;
         }
      }

      assertEquals(0, largeMessagesFileDir.listFiles().length);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
