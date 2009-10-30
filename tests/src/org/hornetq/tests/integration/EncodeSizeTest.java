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

package org.hornetq.tests.integration;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.wireformat.SessionSendLargeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendMessage;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A EncodeSizeTest
 * 
 * For flow control, it's crucial that encode sizes on client and server are the same
 *
 * @author Tim Fox
 *
 *
 */
public class EncodeSizeTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(EncodeSizeTest.class);

   public void testMessageEncodeSize() throws Exception
   {
      for (int i = 0; i < 10; i++)
      {
         ClientMessage clientMessage = new ClientMessageImpl(0);
         
         clientMessage.putIntProperty(RandomUtil.randomString(), RandomUtil.randomInt());
         clientMessage.putBooleanProperty(RandomUtil.randomString(), RandomUtil.randomBoolean());
         clientMessage.putByteProperty(RandomUtil.randomString(), RandomUtil.randomByte());
         clientMessage.putBytesProperty(RandomUtil.randomString(), RandomUtil.randomBytes(125));
         clientMessage.putDoubleProperty(RandomUtil.randomString(), RandomUtil.randomDouble());
         clientMessage.putFloatProperty(RandomUtil.randomString(), RandomUtil.randomFloat());
         clientMessage.putLongProperty(RandomUtil.randomString(), RandomUtil.randomLong());
         clientMessage.putShortProperty(RandomUtil.randomString(), RandomUtil.randomShort());
         clientMessage.putStringProperty(RandomUtil.randomString(), RandomUtil.randomString());
         
         clientMessage.setDestination(RandomUtil.randomSimpleString());
         
         byte[] bytes = RandomUtil.randomBytes(1000);
         
         HornetQBuffer body = ChannelBuffers.dynamicBuffer(bytes);
         
         clientMessage.setBody(body);
         
         int clientEncodeSize = clientMessage.getEncodeSize();
             
         HornetQBuffer buffer = ChannelBuffers.dynamicBuffer(clientEncodeSize);
                  
         clientMessage.encode(buffer);
         
         int wireSize = buffer.writerIndex();
         
         assertEquals(clientEncodeSize, wireSize);
                       
         ServerMessage serverMessage = new ServerMessageImpl();
         
         serverMessage.decode(buffer);
         
         int serverEncodeSize = serverMessage.getEncodeSize();

         assertEquals(wireSize, serverEncodeSize);
      }
   }
   
   public void testMessageEncodeSizeWithPacket() throws Exception
   {
      for (int i = 0; i < 10; i++)
      {
         ClientMessage clientMessage = new ClientMessageImpl(0);
         
         clientMessage.putIntProperty(RandomUtil.randomString(), RandomUtil.randomInt());
         clientMessage.putBooleanProperty(RandomUtil.randomString(), RandomUtil.randomBoolean());
         clientMessage.putByteProperty(RandomUtil.randomString(), RandomUtil.randomByte());
         clientMessage.putBytesProperty(RandomUtil.randomString(), RandomUtil.randomBytes(125));
         clientMessage.putDoubleProperty(RandomUtil.randomString(), RandomUtil.randomDouble());
         clientMessage.putFloatProperty(RandomUtil.randomString(), RandomUtil.randomFloat());
         clientMessage.putLongProperty(RandomUtil.randomString(), RandomUtil.randomLong());
         clientMessage.putShortProperty(RandomUtil.randomString(), RandomUtil.randomShort());
         clientMessage.putStringProperty(RandomUtil.randomString(), RandomUtil.randomString());
         
         clientMessage.setDestination(RandomUtil.randomSimpleString());
         
         byte[] bytes = RandomUtil.randomBytes(1000);
         
         HornetQBuffer body = ChannelBuffers.dynamicBuffer(bytes);
         
         clientMessage.setBody(body);
         
         int clientEncodeSize = clientMessage.getEncodeSize();
         
         SessionSendMessage packet = new SessionSendMessage(clientMessage, false);
             
         HornetQBuffer buffer = ChannelBuffers.dynamicBuffer(packet.getRequiredBufferSize());
                  
         packet.encode(buffer);
         
         int wireSize = buffer.writerIndex();
         
         assertEquals(wireSize, packet.getRequiredBufferSize());
         
         SessionSendMessage received = new SessionSendMessage();
         
         //The length
         buffer.readInt();
         //The packet type byte
         buffer.readByte();
         
         received.decode(buffer);
                  
         ServerMessage serverMessage = received.getServerMessage();
         
         int serverEncodeSize = serverMessage.getEncodeSize();

         assertEquals(clientEncodeSize, serverEncodeSize);
      }
   }     
}
