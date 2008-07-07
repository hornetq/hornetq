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

package org.jboss.messaging.tests.timing.jms.bridge.impl;

import junit.framework.TestCase;
import static org.easymock.EasyMock.*;
import org.easymock.IAnswer;
import org.jboss.messaging.jms.bridge.ConnectionFactoryFactory;
import org.jboss.messaging.jms.bridge.DestinationFactory;
import org.jboss.messaging.jms.bridge.QualityOfServiceMode;
import org.jboss.messaging.jms.bridge.impl.BridgeImpl;

import javax.jms.*;
import javax.transaction.TransactionManager;

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

   public void testStartWithRepeatedFailure() throws Exception
   {
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

      expect(tm.suspend()).andReturn(null);
      expect(sourceDF.createDestination()).andStubReturn(sourceDest);
      expect(targetDF.createDestination()).andStubReturn(targetDest);
      expect(sourceCFF.createConnectionFactory()).andStubReturn(sourceCF);
      // the source connection can not be created
      expect(sourceCF.createConnection()).andStubThrow(
            new JMSException("unable to create a conn"));

      replay(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      replay(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      replay(tm);

      BridgeImpl bridge = new BridgeImpl();

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      // retry after 10 ms
      bridge.setFailureRetryInterval(10);
      // retry only once
      bridge.setMaxRetries(1);
      bridge.setMaxBatchSize(1);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      assertFalse(bridge.isStarted());
      bridge.start();

      Thread.sleep(50);
      assertFalse(bridge.isStarted());
      assertTrue(bridge.isFailed());

      verify(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      verify(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      verify(tm);
   }

   public void testStartWithFailureThenSuccess() throws Exception
   {
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

      expect(tm.suspend()).andReturn(null);
      expect(sourceDF.createDestination()).andStubReturn(sourceDest);
      expect(targetDF.createDestination()).andStubReturn(targetDest);
      expect(sourceCFF.createConnectionFactory()).andStubReturn(sourceCF);
      // the source connection can not be created the 1st time...
      expect(sourceCF.createConnection()).andThrow(
            new JMSException("unable to create a conn"));
      // ... and it succeeds the 2nd time
      expect(sourceCF.createConnection()).andReturn(sourceConn);
      sourceConn.setExceptionListener(isA(ExceptionListener.class));
      expect(sourceConn.createSession(anyBoolean(), anyInt())).andReturn(
            sourceSession);
      expect(sourceSession.createConsumer(sourceDest))
            .andReturn(sourceConsumer);
      sourceConsumer.setMessageListener(isA(MessageListener.class));
      expect(targetCFF.createConnectionFactory()).andReturn(targetCF);
      expect(targetCF.createConnection()).andReturn(targetConn);
      targetConn.setExceptionListener(isA(ExceptionListener.class));
      expect(targetConn.createSession(anyBoolean(), anyInt())).andReturn(
            targetSession);
      expect(targetSession.createProducer(null)).andReturn(targetProducer);
      sourceConn.start();

      replay(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      replay(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      replay(tm);

      BridgeImpl bridge = new BridgeImpl();

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      // retry after 10 ms
      bridge.setFailureRetryInterval(10);
      // retry only once
      bridge.setMaxRetries(1);
      bridge.setMaxBatchSize(1);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      assertFalse(bridge.isStarted());
      bridge.start();

      Thread.sleep(50);
      assertTrue(bridge.isStarted());
      assertFalse(bridge.isFailed());

      verify(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      verify(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      verify(tm);
   }

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

   public void testExceptionOnSourceAndRetrySucceeds() throws Exception
   {
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
      SetExceptionListenerAnswer exceptionListenerAnswer = new SetExceptionListenerAnswer();
      sourceConn.setExceptionListener(isA(ExceptionListener.class));
      expectLastCall().andAnswer(exceptionListenerAnswer);
      expect(sourceConn.createSession(anyBoolean(), anyInt())).andReturn(
            sourceSession);
      expect(sourceSession.createConsumer(sourceDest))
            .andReturn(sourceConsumer);
      sourceConsumer.setMessageListener(isA(MessageListener.class));
      expect(targetCFF.createConnectionFactory()).andReturn(targetCF);
      expect(targetCF.createConnection()).andReturn(targetConn);
      targetConn.setExceptionListener(isA(ExceptionListener.class));
      expect(targetConn.createSession(anyBoolean(), anyInt())).andReturn(
            targetSession);
      expect(targetSession.createProducer(null)).andReturn(targetProducer);
      sourceConn.start();

      //after failure detection, we retry to start the bridge:
      expect(sourceDF.createDestination()).andReturn(sourceDest);
      expect(targetDF.createDestination()).andReturn(targetDest);
      expect(sourceCFF.createConnectionFactory()).andReturn(sourceCF);
      expect(sourceCF.createConnection()).andReturn(sourceConn);
      sourceConn.setExceptionListener(isA(ExceptionListener.class));
      expectLastCall().andAnswer(exceptionListenerAnswer);
      expect(sourceConn.createSession(anyBoolean(), anyInt())).andReturn(
            sourceSession);
      expect(sourceSession.createConsumer(sourceDest))
            .andReturn(sourceConsumer);
      sourceConsumer.setMessageListener(isA(MessageListener.class));
      expect(targetCFF.createConnectionFactory()).andReturn(targetCF);
      expect(targetCF.createConnection()).andReturn(targetConn);
      targetConn.setExceptionListener(isA(ExceptionListener.class));
      expect(targetConn.createSession(anyBoolean(), anyInt())).andReturn(
            targetSession);
      expect(targetSession.createProducer(null)).andReturn(targetProducer);
      sourceConn.start();
      
      
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
      bridge.setFailureRetryInterval(10);
      bridge.setMaxRetries(2);
      bridge.setMaxBatchSize(1);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      assertFalse(bridge.isStarted());
      bridge.start();
      assertTrue(bridge.isStarted());
      
      exceptionListenerAnswer.listener.onException(new JMSException("exception on the source"));
      Thread.sleep(4 * bridge.getFailureRetryInterval());
      // reconnection must have succeded
      assertTrue(bridge.isStarted());
      
      verify(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      verify(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      verify(tm);
      verify(message);
   }
   
   public void testExceptionOnSourceAndRetryFails() throws Exception
   {
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
      SetExceptionListenerAnswer exceptionListenerAnswer = new SetExceptionListenerAnswer();
      sourceConn.setExceptionListener(isA(ExceptionListener.class));
      expectLastCall().andAnswer(exceptionListenerAnswer);
      expect(sourceConn.createSession(anyBoolean(), anyInt())).andReturn(
            sourceSession);
      expect(sourceSession.createConsumer(sourceDest))
            .andReturn(sourceConsumer);
      sourceConsumer.setMessageListener(isA(MessageListener.class));
      expect(targetCFF.createConnectionFactory()).andReturn(targetCF);
      expect(targetCF.createConnection()).andReturn(targetConn);
      targetConn.setExceptionListener(isA(ExceptionListener.class));
      expect(targetConn.createSession(anyBoolean(), anyInt())).andReturn(
            targetSession);
      expect(targetSession.createProducer(null)).andReturn(targetProducer);
      sourceConn.start();

      //after failure detection, we clean up...
      // and it is stopped
      sourceConn.close();
      targetConn.close();
      // ...retry to start the bridge but it fails...
      expect(sourceDF.createDestination()).andReturn(sourceDest);
      expect(targetDF.createDestination()).andReturn(targetDest);
      expect(sourceCFF.createConnectionFactory()).andReturn(sourceCF);
      expect(sourceCF.createConnection()).andThrow(new JMSException("exception while retrying to connect"));
      // ... so we clean up again...
      sourceConn.close();
      targetConn.close();
      // ... and finally stop the bridge
      sourceConn.close();
      targetConn.close();
      
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
      bridge.setFailureRetryInterval(10);
      bridge.setMaxRetries(1);
      bridge.setMaxBatchSize(1);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      assertFalse(bridge.isStarted());
      bridge.start();
      assertTrue(bridge.isStarted());
      
      exceptionListenerAnswer.listener.onException(new JMSException("exception on the source"));
      Thread.sleep(4 * bridge.getFailureRetryInterval());
      // reconnection must have failed
      assertFalse(bridge.isStarted());
      
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
   
   class SetExceptionListenerAnswer implements IAnswer
   {
      ExceptionListener listener = null;

      public Object answer() throws Throwable
      {
         listener = (ExceptionListener) getCurrentArguments()[0];
         return null;
      }
   }
   
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
