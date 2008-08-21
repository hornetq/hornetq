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
package org.jboss.messaging.tests.unit.core.server.impl;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.messaging.core.remoting.CommandManager;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.DeliveryImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class DeliveryImplTest extends UnitTestCase
{
   public void testDeliver() throws Exception
   {
      ServerMessage message = EasyMock.createStrictMock(ServerMessage.class);
      MessageReference messageReference = EasyMock.createStrictMock(MessageReference.class);     
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      final long consumerID = 1234l;
      final long deliveryID = 48548l;
      final int deliveryCount  = 252;
      DeliveryImpl delivery = new DeliveryImpl(messageReference, consumerID, deliveryID, cm);
      EasyMock.expect(messageReference.getMessage()).andStubReturn(message);
      EasyMock.expect(messageReference.getDeliveryCount()).andReturn(deliveryCount);
      cm.sendCommandOneway(EasyMock.eq(consumerID), (Packet) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            long targetID = (Long)EasyMock.getCurrentArguments()[0];
            assertEquals(consumerID, targetID);
            ReceiveMessage receiveMessage = (ReceiveMessage) EasyMock.getCurrentArguments()[1];            
            assertEquals(receiveMessage.getDeliveryCount(), deliveryCount + 1);
            return null;
         }
      });      
      EasyMock.replay(messageReference, cm, message);
      delivery.deliver();
      EasyMock.verify(messageReference, cm, message);
      assertEquals(messageReference, delivery.getReference());
   }
}
