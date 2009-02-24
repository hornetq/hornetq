/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.jms.client;

import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class JMSMessageListenerWrapperTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testDummy()
   {      
   }
   
//   public void testOnMessage() throws Exception
//   {
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      expect(clientSession.isClosed()).andReturn(false);
//      JBossSession session = createStrictMock(JBossSession.class);
//      expect(session.getCoreSession()).andStubReturn(clientSession);
//      expect(session.isRecoverCalled()).andReturn(false);
//      session.setRecoverCalled(false);
//      MessageListener listener = createStrictMock(MessageListener.class);
//      listener.onMessage(isA(Message.class));
//      ClientMessage clientMessage = createNiceMock(ClientMessage.class);
//      
//      replay(clientSession, session, listener, clientMessage);
//      
//      JMSMessageListenerWrapper wrapper = new JMSMessageListenerWrapper(session, listener , Session.AUTO_ACKNOWLEDGE);
//      wrapper.onMessage(clientMessage);
//      
//      verify(clientSession, session, listener, clientMessage);
//   }
//   
//   public void testOnMessageWithSessionTransacted() throws Exception
//   {
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      JBossSession session = createStrictMock(JBossSession.class);
//      expect(session.getCoreSession()).andStubReturn(clientSession);
//      expect(clientSession.isClosed()).andStubReturn(false);
//      expect(session.isRecoverCalled()).andReturn(false);
//      
//      session.setRecoverCalled(false);
//      MessageListener listener = createStrictMock(MessageListener.class);
//      listener.onMessage(isA(Message.class));
//      ClientMessage clientMessage = createNiceMock(ClientMessage.class);
//      
//      replay(clientSession, session, listener, clientMessage);
//      
//      JMSMessageListenerWrapper wrapper = new JMSMessageListenerWrapper(session, listener , Session.SESSION_TRANSACTED);
//      wrapper.onMessage(clientMessage);
//      
//      verify(clientSession, session, listener, clientMessage);
//   }
//   
//   public void testOnMessageThrowsAndException() throws Exception
//   {
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      clientSession.rollback();
//      JBossSession session = createStrictMock(JBossSession.class);
//      expect(session.getCoreSession()).andStubReturn(clientSession);
//      session.setRecoverCalled(true);
//      expect(session.isRecoverCalled()).andReturn(true);
//      session.setRecoverCalled(false);
//      MessageListener listener = createStrictMock(MessageListener.class);
//      listener.onMessage(isA(Message.class));
//      expectLastCall().andThrow(new RuntimeException());
//      
//      ClientMessage clientMessage = createNiceMock(ClientMessage.class);
//      
//      replay(clientSession, session, listener, clientMessage);
//      
//      JMSMessageListenerWrapper wrapper = new JMSMessageListenerWrapper(session, listener , Session.AUTO_ACKNOWLEDGE);
//      wrapper.onMessage(clientMessage);
//      
//      verify(clientSession, session, listener, clientMessage);
//   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
