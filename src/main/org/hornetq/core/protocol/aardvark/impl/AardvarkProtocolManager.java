/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.protocol.aardvark.impl;

import java.util.List;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.spi.core.protocol.ConnectionEntry;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.spi.core.remoting.Connection;

/**
 * AardvarkProtocolManager
 * 
 * A stupid protocol to demonstrate how to implement a new protocol in HornetQ
 *
 * @author Tim Fox
 *
 *
 */
public class AardvarkProtocolManager implements ProtocolManager
{
   private static final Logger log = Logger.getLogger(AardvarkProtocolManager.class);

   private final HornetQServer server;

   public AardvarkProtocolManager(final HornetQServer server, final List<Interceptor> interceptors)
   {
      this.server = server;
   }

   public ConnectionEntry createConnectionEntry(final Connection connection)
   {
      AardvarkConnection conn = new AardvarkConnection(connection, this);

      return new ConnectionEntry(conn, 0, 0);
   }
   
   public void removeHandler(String name)
   {
   }

   public void handleBuffer(final RemotingConnection conn, final HornetQBuffer buffer)
   {                 
      try
      {
         ServerSession session = server.createSession("aardvark",
                                                      null,
                                                      null,
                                                      Integer.MAX_VALUE,
                                                      conn,
                                                      true,
                                                      true,
                                                      true,
                                                      false,
                                                      new AardvarkSessionCallback(conn.getTransportConnection()));
         
         final SimpleString name = new SimpleString("hornetq.aardvark");
         
         session.createQueue(name, name, null, false, false);
         
         session.createConsumer(0, name, null, false);
         
         session.receiveConsumerCredits(0, -1); // No flow control
         
         session.start();
         
         ServerMessage message = new ServerMessageImpl(0, 1000);
         
         message.setAddress(name);
         
         message.getBodyBuffer().writeUTF("GIRAFFE\n");
         
         session.send(message);
         
         session.start();
         
         session.closeConsumer(0);
         
         session.deleteQueue(name);
         
         session.close();
      }
      catch (Exception e)
      {
         log.error("Failed to create session", e);
      }
   }

   private class AardvarkSessionCallback implements SessionCallback
   {
      private final Connection connection;
      
      AardvarkSessionCallback(final Connection connection)
      {
         this.connection = connection;
      }

      public void closed()
      {
      }

      public int sendLargeMessage(long consumerID, byte[] headerBuffer, long bodySize, int deliveryCount)
      {
         return 0;
      }

      public int sendLargeMessageContinuation(long consumerID, byte[] body, boolean continues, boolean requiresResponse)
      {
         return 0;
      }

      public int sendMessage(ServerMessage message, long consumerID, int deliveryCount)
      {
         HornetQBuffer buffer = message.getBodyBuffer();
         
         buffer.readerIndex(MessageImpl.BUFFER_HEADER_SPACE);
         
         connection.write(buffer);
         
         return -1;
      }

      public void sendProducerCreditsMessage(int credits, SimpleString address, int offset)
      {
      }
      
   }

   public void bufferReceived(Object connectionID, HornetQBuffer buffer)
   {      
   }

   public int isReadyToHandle(HornetQBuffer buffer)
   {
      //Look for a new-line
      
      //BTW this is very inefficient - in a real protocol you'd want to do this better
      
      for (int i = buffer.readerIndex(); i < buffer.writerIndex(); i++)
      {
         byte b = buffer.getByte(i);
         
         if (b == (byte)'\n')
         {
            return buffer.writerIndex();
         }
      }
            
      return -1;      
   }

}
