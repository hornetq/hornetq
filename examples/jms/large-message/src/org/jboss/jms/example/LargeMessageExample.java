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
package org.jboss.jms.example;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;

/**
 * This example demonstrates the ability of JBoss Messaging to send and consume a very large message, much
 * bigger than can fit in RAM.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class LargeMessageExample extends JMSExample
{
   public static void main(String[] args)
   {
      // We limit the server to running in only 50MB of RAM
      String[] serverJMXArgs = new String[] {"-Xms50M",
                                             "-Xmx50M",
                                             "-XX:+UseParallelGC",
                                             "-XX:+AggressiveOpts",
                                             "-XX:+UseFastAccessorMethods"};

      new LargeMessageExample().run(serverJMXArgs, args);
   }

   //The message we will send is size 256MB, even though we are only running in 50MB of RAM on both client and server.
   //JBoss Messaging will support much larger message sizes, but we use 512MB so the example runs in reasonable time.
   private final long FILE_SIZE = 256 * 1024 * 1024;

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;

      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory. This ConnectionFactory has a special attribute set on
         // it.
         // Messages with more than 10K are considered large
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. Create the JMS objects
         connection = cf.createConnection();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(queue);

         // Step 5. Create a huge file - this will form the body of the message we will send.

         System.out.println("Creating a file to send of size " + FILE_SIZE +
                            " bytes. This may take a little while... " +
                            "If this is too big for your disk you can easily change the FILE_SIZE in the example.");

         File fileInput = new File("huge_message_to_send.dat");

         fileInput.createNewFile();

         createFile(fileInput, FILE_SIZE);

         System.out.println("File created.");

         // Step 6. Create a BytesMessage - does it have to be a bytesmessage??
         BytesMessage message = session.createBytesMessage();

         // Step 7. We set the InputStream on the message. When sending the message will read the InputStream
         // until it gets EOF. In this case we point the InputStream at a file on disk, and it will suck up the entire
         // file, however we could use any InputStream not just a FileInputStream.
         FileInputStream fileInputStream = new FileInputStream(fileInput);
         BufferedInputStream bufferedInput = new BufferedInputStream(fileInputStream);
         
         message.setObjectProperty("JMS_JBM_InputStream", bufferedInput);

         System.out.println("Sending the huge message.");

         // Step 9. Send the Message
         producer.send(message);

         System.out.println("Large Message sent");

         System.out.println("Stopping server.");

         // Step 10. To demonstrate that that we're not simply streaming the message from sending to consumer, we stop
         // the server and restart it before consuming the message. This demonstrates that the large message gets
         // persisted, like a
         // normal persistent message, on the server. If you look at ./build/data/largeMessages you will see the
         // largeMessage stored on disk the server

         connection.close();

         initialContext.close();

         stopServer(0);

         // Give the server a little time to shutdown properly
         Thread.sleep(5000);

         startServer(0);

         System.out.println("Server restarted.");

         // Step 11. Now the server is restarted we can recreate the JMS Objects, and start the new connection

         initialContext = getContext(0);

         queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         connection = cf.createConnection();

         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer messageConsumer = session.createConsumer(queue);

         connection.start();

         System.out.println("Receiving message.");

         // Step 12. Receive the message. When we receive the large message we initially just receive the message with
         // an empty body.
         BytesMessage messageReceived = (BytesMessage)messageConsumer.receive(120000);

         System.out.println("Received message with: " + messageReceived.getBodyLength() +
                            " bytes. Now streaming to file on disk.");

         // Step 13. We set an OutputStream on the message. This causes the message body to be written to the
         // OutputStream until there are no more bytes to be written.
         // You don't have to use a FileOutputStream, you can use any OutputStream.
         // You may choose to use the regular BytesMessage or
         // StreamMessage interface but this method is much faster for large messages.

         File outputFile = new File("huge_message_received.dat");

         FileOutputStream fileOutputStream = new FileOutputStream(outputFile);

         BufferedOutputStream bufferedOutput = new BufferedOutputStream(fileOutputStream);

         // Step 14. This will save the stream and wait until the entire message is written before continuing.
         messageReceived.setObjectProperty("JMS_JBM_SaveStream", bufferedOutput);

         fileOutputStream.close();

         System.out.println("File streamed to disk. Size of received file on disk is " + outputFile.length());

         return true;
      }
      finally
      {
         // Step 12. Be sure to close our resources!
         if (initialContext != null)
         {
            initialContext.close();
         }

         if (connection != null)
         {
            connection.close();
         }
      }
   }

   /**
    * @param file
    * @param fileSize
    * @throws FileNotFoundException
    * @throws IOException
    */
   private void createFile(File file, long fileSize) throws FileNotFoundException, IOException
   {
      FileOutputStream fileOut = new FileOutputStream(file);
      BufferedOutputStream buffOut = new BufferedOutputStream(fileOut);
      byte[] outBuffer = new byte[1024 * 1024];
      for (long i = 0; i < fileSize; i += outBuffer.length)
      {
         buffOut.write(outBuffer);
      }
      buffOut.close();
   }

}
