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
package org.jboss.messaging.tests.unit.core.client.impl;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.impl.ClientConsumerInternal;
import org.jboss.messaging.core.client.impl.ClientConsumerPacketHandler;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A ClientConsumerPacketHandlerTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientConsumerPacketHandlerTest extends UnitTestCase
{
   public void testHandle() throws Exception
   {
      ClientConsumerInternal consumer = EasyMock.createStrictMock(ClientConsumerInternal.class);
      
      final int id = 716276;
      
      ClientConsumerPacketHandler handler = new ClientConsumerPacketHandler(consumer, id);
         
      ClientMessage msg = EasyMock.createStrictMock(ClientMessage.class);
      
      ReceiveMessage rm = new ReceiveMessage(msg);
                 
      assertEquals(id, handler.getID());
      
      consumer.handleMessage(msg);
      
      EasyMock.replay(consumer, msg);
      
      handler.handle(rm, EasyMock.createStrictMock(PacketReturner.class));
      
      EasyMock.verify(consumer, msg);
      
      try
      {
         handler.handle(new PacketImpl(PacketImpl.CONN_START), EasyMock.createStrictMock(PacketReturner.class));
         fail("Should throw Exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
   }
   
   // Private -----------------------------------------------------------------------------------------------------------

}
