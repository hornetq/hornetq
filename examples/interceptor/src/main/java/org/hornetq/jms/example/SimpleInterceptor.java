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

package org.hornetq.jms.example;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.SessionSendMessage;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * A simple Interceptor implementation
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class SimpleInterceptor implements Interceptor
{

   public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
   {
      System.out.println("SimpleInterceptor gets called!");
      System.out.println("Packet: " + packet.getClass().getName());
      System.out.println("RemotingConnection: " + connection.getRemoteAddress());

      if (packet instanceof SessionSendMessage)
      {
         SessionSendMessage realPacket = (SessionSendMessage)packet;
         Message msg = realPacket.getMessage();
         msg.putStringProperty(new SimpleString("newproperty"), new SimpleString("Hello from interceptor!"));
      }
      // We return true which means "call next interceptor" (if there is one) or target.
      // If we returned false, it means "abort call" - no more interceptors would be called and neither would
      // the target
      return true;
   }

}
