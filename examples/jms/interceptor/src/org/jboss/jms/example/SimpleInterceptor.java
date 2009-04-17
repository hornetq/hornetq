/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendMessage;
import org.jboss.messaging.utils.SimpleString;

/**
 * A simple Interceptor implementation
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class SimpleInterceptor implements Interceptor
{
   
   public boolean intercept(Packet packet, RemotingConnection connection) throws MessagingException
   {
      System.out.println("SimpleInterceptor gets called!");
      System.out.println("Packet: " + packet.getClass().getName());
      System.out.println("RemotingConnection: " + connection.getRemoteAddress());

      if (packet instanceof SessionSendMessage)
      {
         SessionSendMessage realPacket = (SessionSendMessage)packet;
         Message msg = realPacket.getServerMessage();
         msg.putStringProperty(new SimpleString("newproperty"), new SimpleString("Hello from interceptor!"));
      }
      return true;
   }

}


