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


package org.jboss.messaging.tests.unit.core.remoting.impl.mina;

import java.util.concurrent.ExecutorService;

import org.apache.mina.common.IoSession;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.CleanUpNotifier;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.mina.MinaHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class MinaHandlerTest extends UnitTestCase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private PacketDispatcher dispatcher;
   
   private CleanUpNotifier failureNotifier;
   
   private ExecutorService executorService;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   
   public void setUp()
   {
      dispatcher = null;
      
      failureNotifier = null;
      
      executorService = null;
   }
   
   
   public void testConstructor() throws Exception
   {
      newMinaHandler(true);
      
      try
      {
         new MinaHandler(null, executorService, failureNotifier, true, true);
         fail("Supposed to throw exception");
      }
      catch (IllegalArgumentException e)
      {
         
      }

      try
      {
         new MinaHandler(dispatcher, null, failureNotifier, true, true);
         fail("Supposed to throw exception");
      }
      catch (IllegalArgumentException e)
      {
         
      }

      // As the executor is not being used, it shouldn't throw an exception
      new MinaHandler(dispatcher, null, failureNotifier, true, false);
   }

   public void testExceptionCaught() throws Exception
   {
      MinaHandler handler = newMinaHandler(true);
      
      IoSession session = EasyMock.createMock(IoSession.class);
      
      EasyMock.expect(session.getId()).andStubReturn(1l);
      
      // If the method is refactored to use the Future, this might have to be changed
      EasyMock.expect(session.close()).andReturn(null);
      
      failureNotifier.fireCleanup(EasyMock.eq(1l), messagingExceptionMatch(MessagingException.INTERNAL_ERROR));
      
      EasyMock.replay(dispatcher, failureNotifier, executorService, session);
      
      handler.exceptionCaught(session, new Exception("anything"));

      EasyMock.verify(dispatcher, failureNotifier, executorService, session);
      
   }
   
   public void testMessageReceivedWithExecutor() throws Exception
   {
      internalTestMessageReceived(true);
   }
   
   public void testMessageReceivedWithoutExecutor() throws Exception
   {
      internalTestMessageReceived(false);
   }
   
   private void internalTestMessageReceived(boolean useExecutor) throws Exception
   {
      MinaHandler handler = newMinaHandler(useExecutor);
      
      IoSession session = EasyMock.createNiceMock(IoSession.class);
      
      ClientMessage msg = EasyMock.createNiceMock(ClientMessage.class);
      
      ReceiveMessage rcvMessage = new ReceiveMessage(msg);
      
      setupExecutorService();

      dispatcher.dispatch(EasyMock.isA(ReceiveMessage.class), (PacketReturner)EasyMock.anyObject());
      
      EasyMock.replay(dispatcher, failureNotifier, executorService, session);

      handler.messageReceived(session, rcvMessage);
      
      EasyMock.verify(dispatcher, failureNotifier, executorService, session);
      
      EasyMock.reset(dispatcher, failureNotifier, executorService, session);
      
      setupExecutorService();
      
      
      rcvMessage.setResponseTargetID(33);
      
      assertEquals(33, rcvMessage.getResponseTargetID());
      
      dispatcher.callFilters(EasyMock.isA(ReceiveMessage.class));

      dispatcher.dispatch(EasyMock.isA(ReceiveMessage.class), (PacketReturner)EasyMock.anyObject());
      
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>(){

         public Object answer() throws Throwable
         {
            Packet response = (Packet) EasyMock.getCurrentArguments()[0];
            PacketReturner returner = (PacketReturner)EasyMock.getCurrentArguments()[1];
            if (returner != null)
            {
               returner.send(response);
            }
            return null;
         }});
      
      EasyMock.expect(session.write(EasyMock.isA(ReceiveMessage.class))).andStubReturn(null);
      
      EasyMock.replay(dispatcher, failureNotifier, executorService, session);

      handler.messageReceived(session, rcvMessage);
      
      EasyMock.verify(dispatcher, failureNotifier, executorService, session);
      
   
   }



   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private MinaHandler newMinaHandler(boolean useExecutor)
   {
      dispatcher = EasyMock.createMock(PacketDispatcher.class);
         
      failureNotifier = EasyMock.createMock(CleanUpNotifier.class);
      
      if (executorService == null)
      {
         executorService = EasyMock.createMock(ExecutorService.class);
      }
      
      dispatcher.setListener(EasyMock.isA(MinaHandler.class));
      
      EasyMock.replay(dispatcher, failureNotifier, executorService);
      
      MinaHandler handler = new MinaHandler(dispatcher, executorService, failureNotifier, true, useExecutor);
      
      EasyMock.verify(dispatcher, failureNotifier, executorService);
      
      EasyMock.reset(dispatcher, failureNotifier, executorService);
      return handler;
   }

   private void setupExecutorService()
   {
      executorService.execute(EasyMock.isA(Runnable.class));
      
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {

         public Object answer() throws Throwable
         {
            Runnable run = (Runnable)EasyMock.getCurrentArguments()[0];
            run.run();
            return null;
         }});
      
      EasyMock.expectLastCall().anyTimes();
   }

   
   // Inner classes -------------------------------------------------
   
}
