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

package org.jboss.messaging.tests.unit.core.client.impl;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.impl.ClientProducerInternal;
import org.jboss.messaging.core.client.impl.ClientProducerPacketHandler;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerFlowCreditMessage;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A ClientProducerPacketHandlerTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientProducerPacketHandlerTest extends UnitTestCase
{
   public void testHandle() throws Exception
   {
      ClientProducerInternal producer = EasyMock.createStrictMock(ClientProducerInternal.class);
      
      final int id = 716276;
      
      ClientProducerPacketHandler handler = new ClientProducerPacketHandler(producer, id);
         
      final int credits = 918298;
      
      ProducerFlowCreditMessage msg = new ProducerFlowCreditMessage(credits);
                 
      assertEquals(id, handler.getID());
      
      producer.receiveCredits(credits);
      
      EasyMock.replay(producer);
      
      handler.handle(msg, EasyMock.createStrictMock(PacketReturner.class));
      
      EasyMock.verify(producer);
      
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
