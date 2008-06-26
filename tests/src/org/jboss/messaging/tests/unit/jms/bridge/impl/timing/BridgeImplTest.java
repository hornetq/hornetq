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

package org.jboss.messaging.tests.unit.jms.bridge.impl.timing;

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.transaction.TransactionManager;

import junit.framework.TestCase;

import org.easymock.IAnswer;
import org.jboss.messaging.jms.bridge.ConnectionFactoryFactory;
import org.jboss.messaging.jms.bridge.DestinationFactory;
import org.jboss.messaging.jms.bridge.QualityOfServiceMode;
import org.jboss.messaging.jms.bridge.impl.BridgeImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class BridgeImplTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /*
    * we receive only 1 message. The message is sent when the maxBatchTime
    * expires even if the maxBatchSize is not reached
    */
   public void testSendMessagesWhenMaxBatchTimeExpires() throws Exception
   {
      int maxBatchSize = 2;
      long maxBatchTime = 500;

      ConnectionFactoryFactory sourceCFF = createStrictMock(ConnectionFactoryFactory.class);
      ConnectionFactory sourceCF = createStrictMock(ConnectionFactory.class);
      Connection sourceConn = createStrictMock(Connection.class);
      Session sourceSession = createStrictMock(Session.class);
      MessageConsumer sourceConsumer = createStrictMock(MessageConsumer.class);
      DestinationFactory sourceDF = createStrictMock(DestinationFactory.class);
      Destination sourceDest = createStrictMock(Destination.class);
      ConnectionFactoryFactory targetCFF = createStrictMock(ConnectionFactoryFactory.class);
      ConnectionFactory targetCF = createStrictMock(ConnectionFactory.class);
      Connection targetConn = createStrictMock(Connection.class);
      Session targetSession = createStrictMock(Session.class);
      MessageProducer targetProducer = createStrictMock(MessageProducer.class);
      DestinationFactory targetDF = createStrictMock(DestinationFactory.class);
      Destination targetDest = createStrictMock(Destination.class);
      TransactionManager tm = createStrictMock(TransactionManager.class);
      Message message = createNiceMock(Message.class);

      expect(tm.suspend()).andReturn(null);
      expect(sourceDF.createDestination()).andReturn(sourceDest);
      expect(targetDF.createDestination()).andReturn(targetDest);
      expect(sourceCFF.createConnectionFactory()).andReturn(sourceCF);
      expect(sourceCF.createConnection()).andReturn(sourceConn);
      sourceConn.setExceptionListener(isA(ExceptionListener.class));
      expect(sourceConn.createSession(anyBoolean(), anyInt())).andReturn(
            sourceSession);
      expect(sourceSession.createConsumer(sourceDest))
            .andReturn(sourceConsumer);
      SetMessageListenerAnswer answer = new SetMessageListenerAnswer();
      sourceConsumer.setMessageListener(isA(MessageListener.class));
      expectLastCall().andAnswer(answer);
      expect(targetCFF.createConnectionFactory()).andReturn(targetCF);
      expect(targetCF.createConnection()).andReturn(targetConn);
      targetConn.setExceptionListener(isA(ExceptionListener.class));
      expect(targetConn.createSession(anyBoolean(), anyInt())).andReturn(
            targetSession);
      expect(targetSession.createProducer(null)).andReturn(targetProducer);
      sourceConn.start();

      targetProducer.send(targetDest, message, 0, 0, 0);
      targetSession.commit();

      replay(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      replay(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      replay(tm);
      replay(message);

      BridgeImpl bridge = new BridgeImpl();
      assertNotNull(bridge);

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      bridge.setFailureRetryInterval(-1);
      bridge.setMaxRetries(-1);
      bridge.setMaxBatchSize(maxBatchSize);
      bridge.setMaxBatchTime(maxBatchTime);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      assertFalse(bridge.isStarted());
      bridge.start();
      assertTrue(bridge.isStarted());

      answer.listener.onMessage(message);

      Thread.sleep(3 * maxBatchTime);

      verify(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      verify(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      verify(tm);
      verify(message);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   class SetMessageListenerAnswer implements IAnswer
   {
      MessageListener listener = null;

      public Object answer() throws Throwable
      {
         listener = (MessageListener) getCurrentArguments()[0];
         return null;
      }
   }
}
