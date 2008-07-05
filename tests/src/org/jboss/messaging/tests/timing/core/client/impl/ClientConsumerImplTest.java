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
package org.jboss.messaging.tests.timing.core.client.impl;

import java.util.concurrent.ExecutorService;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientConsumerImpl;
import org.jboss.messaging.core.client.impl.ClientConsumerInternal;
import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A ClientConsumerImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientConsumerImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ClientConsumerImplTest.class);

   public void testSetHandlerWhileReceiving() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
      ExecutorService executor = EasyMock.createStrictMock(ExecutorService.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);  
             
      final ClientConsumerInternal consumer =
         new ClientConsumerImpl(session, 675765, 67565, 787, false, rc, pd, executor, 878787);
      
      MessageHandler handler = new MessageHandler()
      {
         public void onMessage(ClientMessage msg)
         {            
         }
      };
      
      Thread t = new Thread()
      {
         public void run()
         {
            try
            {
               consumer.receive(1000);
            }
            catch (Exception e)
            {
            }
         }
      };
      
      t.start();
      
      Thread.sleep(100);
      
      try
      {
         consumer.setMessageHandler(handler);
         
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.ILLEGAL_STATE, e.getCode());
      } 
      finally
      {
         t.interrupt();
      }
   }

   // Private -----------------------------------------------------------------------------------------------------------

}
