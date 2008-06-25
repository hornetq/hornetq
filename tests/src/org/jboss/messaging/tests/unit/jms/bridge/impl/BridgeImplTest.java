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

package org.jboss.messaging.tests.unit.jms.bridge.impl;

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
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.transaction.TransactionManager;

import junit.framework.TestCase;

import org.easymock.IAnswer;
import org.jboss.messaging.jms.bridge.Bridge;
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

   public void testBridge() throws Exception
   {
      ConnectionFactoryFactory sourceCFF = createStrictMock(ConnectionFactoryFactory.class);
      ConnectionFactoryFactory targetCFF = createStrictMock(ConnectionFactoryFactory.class);
      DestinationFactory sourceDF = createStrictMock(DestinationFactory.class);
      DestinationFactory targetDF = createStrictMock(DestinationFactory.class);
      String sourceUsername = randomString();
      String sourcePassword = randomString();
      String targetUsername = randomString();
      String targetPassword = randomString();
      String selector = "color = 'green'";
      long failureRetryInterval = 1000;
      int maxRetries = 1;
      QualityOfServiceMode qosMode = QualityOfServiceMode.AT_MOST_ONCE;
      int maxBatchSize = 1;
      long maxBatchTime = -1;
      String subName = randomString();
      String clientID = randomString();
      boolean addMessageIDInHeader = false;

      replay(sourceCFF, sourceDF, targetCFF, targetDF);

      Bridge bridge = new BridgeImpl(sourceCFF, targetCFF, sourceDF, targetDF,
            sourceUsername, sourcePassword, targetUsername, targetPassword,
            selector, failureRetryInterval, maxRetries, qosMode, maxBatchSize,
            maxBatchTime, subName, clientID, addMessageIDInHeader);
      assertNotNull(bridge);
      assertEquals(sourceDF, bridge.getSourceDestinationFactory());
      assertEquals(targetDF, bridge.getTargetDestinationFactory());
      assertEquals(sourceUsername, bridge.getSourceUsername());
      assertEquals(sourcePassword, bridge.getSourcePassword());
      assertEquals(targetUsername, bridge.getTargetUsername());
      assertEquals(targetPassword, bridge.getTargetPassword());
      assertEquals(selector, bridge.getSelector());
      assertEquals(failureRetryInterval, bridge.getFailureRetryInterval());
      assertEquals(maxRetries, bridge.getMaxRetries());
      assertEquals(qosMode, bridge.getQualityOfServiceMode());
      assertEquals(maxBatchSize, bridge.getMaxBatchSize());
      assertEquals(maxBatchTime, bridge.getMaxBatchTime());
      assertEquals(subName, bridge.getSubscriptionName());
      assertEquals(clientID, bridge.getClientID());
      assertEquals(addMessageIDInHeader, bridge.isAddMessageIDInHeader());

      verify(sourceCFF, sourceDF, targetCFF, targetDF);
   }

   public void testSetSourceDestinationFactoryWithNull() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      try
      {
         bridge.setSourceDestinationFactory(null);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }
   }

   public void testSetSourceDestinationFactory() throws Exception
   {
      DestinationFactory sourceDF = createStrictMock(DestinationFactory.class);

      replay(sourceDF);

      Bridge bridge = new BridgeImpl();
      bridge.setSourceDestinationFactory(sourceDF);
      assertEquals(sourceDF, bridge.getSourceDestinationFactory());

      verify(sourceDF);
   }

   public void testSetSourceUserName() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      bridge.setSourceUsername(null);
      assertNull(bridge.getSourceUsername());

      String sourceUsername = randomString();
      bridge.setSourceUsername(sourceUsername);
      assertEquals(sourceUsername, bridge.getSourceUsername());
   }

   public void testSetSourcePasword() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      bridge.setSourcePassword(null);
      assertNull(bridge.getSourcePassword());

      String sourcePassword = randomString();
      bridge.setSourcePassword(sourcePassword);
      assertEquals(sourcePassword, bridge.getSourcePassword());
   }

   public void testSetTargetDestinationFactoryWithNull() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      try
      {
         bridge.setTargetDestinationFactory(null);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }
   }

   public void testSetTargetDestinationFactory() throws Exception
   {
      DestinationFactory targetDF = createStrictMock(DestinationFactory.class);

      replay(targetDF);

      Bridge bridge = new BridgeImpl();
      bridge.setTargetDestinationFactory(targetDF);
      assertEquals(targetDF, bridge.getTargetDestinationFactory());

      verify(targetDF);
   }

   public void testSetTargetUserName() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      bridge.setTargetUsername(null);
      assertNull(bridge.getTargetUsername());

      String targetUsername = randomString();
      bridge.setTargetUsername(targetUsername);
      assertEquals(targetUsername, bridge.getTargetUsername());
   }

   public void testSetTargetPasword() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      bridge.setTargetPassword(null);
      assertNull(bridge.getTargetPassword());

      String targetPassword = randomString();
      bridge.setTargetPassword(targetPassword);
      assertEquals(targetPassword, bridge.getTargetPassword());
   }

   public void testSetSelector() throws Exception
   {
      Bridge bridge = new BridgeImpl();
      assertNull(bridge.getSelector());

      String selector = "color = 'green'";
      bridge.setSelector(selector);
      assertEquals(selector, bridge.getSelector());

      bridge.setSelector(null);
      assertNull(bridge.getSelector());
   }

   public void testSetFailureRetryInterval() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      bridge.setFailureRetryInterval(-1);
      assertEquals(-1, bridge.getFailureRetryInterval());

      bridge.setFailureRetryInterval(1000);
      assertEquals(1000, bridge.getFailureRetryInterval());

      try
      {
         bridge.setFailureRetryInterval(0);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      try
      {
         bridge.setFailureRetryInterval(-2);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }
   }

   public void testSetMaxRetries() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      bridge.setMaxRetries(-1);
      assertEquals(-1, bridge.getMaxRetries());

      bridge.setMaxRetries(1000);
      assertEquals(1000, bridge.getMaxRetries());

      try
      {
         bridge.setMaxRetries(0);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      try
      {
         bridge.setMaxRetries(-2);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }
   }

   public void testSetMaxBatchSize() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      bridge.setMaxBatchSize(1);
      assertEquals(1, bridge.getMaxBatchSize());

      bridge.setMaxBatchSize(1000);
      assertEquals(1000, bridge.getMaxBatchSize());

      try
      {
         bridge.setMaxBatchSize(0);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      try
      {
         bridge.setMaxBatchSize(-1);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }
   }

   public void testSetMaxBatchTime() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      bridge.setMaxBatchTime(1);
      assertEquals(1, bridge.getMaxBatchTime());

      bridge.setMaxBatchTime(-1);
      assertEquals(-1, bridge.getMaxBatchTime());

      bridge.setMaxBatchTime(1000);
      assertEquals(1000, bridge.getMaxBatchTime());

      try
      {
         bridge.setMaxBatchTime(0);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }

      try
      {
         bridge.setMaxBatchTime(-2);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }
   }

   public void testSetQualityOfServiceMode() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      bridge.setQualityOfServiceMode(QualityOfServiceMode.DUPLICATES_OK);
      assertEquals(QualityOfServiceMode.DUPLICATES_OK, bridge
            .getQualityOfServiceMode());

      try
      {
         bridge.setQualityOfServiceMode(null);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }
   }

   public void testSetClientID() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      String clientID = randomString();
      bridge.setClientID(clientID);
      assertEquals(clientID, bridge.getClientID());

      bridge.setClientID(null);
      assertNull(bridge.getClientID());
   }

   public void testSetSubscriptionName() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      String subscriptionName = randomString();
      bridge.setSubscriptionName(subscriptionName);
      assertEquals(subscriptionName, bridge.getSubscriptionName());

      bridge.setSubscriptionName(null);
      assertNull(bridge.getSubscriptionName());
   }

   public void testSetAddMessageIDInHeader() throws Exception
   {
      Bridge bridge = new BridgeImpl();

      bridge.setAddMessageIDInHeader(true);
      assertTrue(bridge.isAddMessageIDInHeader());

      bridge.setAddMessageIDInHeader(false);
      assertFalse(bridge.isAddMessageIDInHeader());
   }

   public void testStart() throws Exception
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
      expect(sourceDF.createDestination()).andReturn(sourceDest);
      expect(targetDF.createDestination()).andReturn(targetDest);
      expect(sourceCFF.createConnectionFactory()).andReturn(sourceCF);
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
      assertNotNull(bridge);

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      bridge.setFailureRetryInterval(-1);
      bridge.setMaxRetries(-1);
      bridge.setMaxBatchSize(1);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      assertFalse(bridge.isStarted());
      bridge.start();
      assertTrue(bridge.isStarted());

      verify(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      verify(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      verify(tm);
   }

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

   public void testStop() throws Exception
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

      // to start
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
      sourceConsumer.setMessageListener(isA(MessageListener.class));
      expect(targetCFF.createConnectionFactory()).andReturn(targetCF);
      expect(targetCF.createConnection()).andReturn(targetConn);
      targetConn.setExceptionListener(isA(ExceptionListener.class));
      expect(targetConn.createSession(anyBoolean(), anyInt())).andReturn(
            targetSession);
      expect(targetSession.createProducer(null)).andReturn(targetProducer);
      sourceConn.start();

      // to stop
      sourceConn.close();
      targetConn.close();

      replay(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      replay(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      replay(tm);

      BridgeImpl bridge = new BridgeImpl();
      assertNotNull(bridge);

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      bridge.setFailureRetryInterval(-1);
      bridge.setMaxRetries(-1);
      bridge.setMaxBatchSize(1);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);
      bridge.start();
      assertTrue(bridge.isStarted());

      bridge.stop();
      assertFalse(bridge.isStarted());

      verify(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      verify(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      verify(tm);
   }

   public void testPauseResume() throws Exception
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

      // to start
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
      sourceConsumer.setMessageListener(isA(MessageListener.class));
      expect(targetCFF.createConnectionFactory()).andReturn(targetCF);
      expect(targetCF.createConnection()).andReturn(targetConn);
      targetConn.setExceptionListener(isA(ExceptionListener.class));
      expect(targetConn.createSession(anyBoolean(), anyInt())).andReturn(
            targetSession);
      expect(targetSession.createProducer(null)).andReturn(targetProducer);
      sourceConn.start();

      // to pause
      sourceConn.stop();

      // to resume
      sourceConn.start();

      // to stop
      sourceConn.close();
      targetConn.close();

      replay(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      replay(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      replay(tm);

      BridgeImpl bridge = new BridgeImpl();
      assertNotNull(bridge);

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      bridge.setFailureRetryInterval(-1);
      bridge.setMaxRetries(-1);
      bridge.setMaxBatchSize(1);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      assertFalse(bridge.isStarted());
      assertFalse(bridge.isPaused());

      bridge.start();
      assertTrue(bridge.isStarted());
      assertFalse(bridge.isPaused());

      bridge.pause();
      assertTrue(bridge.isStarted());
      assertTrue(bridge.isPaused());

      bridge.resume();
      assertTrue(bridge.isStarted());
      assertFalse(bridge.isPaused());

      bridge.stop();
      assertFalse(bridge.isStarted());
      assertFalse(bridge.isPaused());

      verify(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      verify(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      verify(tm);
   }

   public void testSendMessagesInNoTx_1() throws Exception
   {
      // with batch size of 1, receive 2 messages and send 2
      doSendMessagesByBatchInNoTx(1, 2, 2, 2);
   }
   
   public void testSendMessagesInNoTx_2() throws Exception
   {
      // with batch size of 2, receive 2 messages and send 2
      doSendMessagesByBatchInNoTx(2, 2, 2, 1);
   }
   
   public void testSendMessagesInNoTx_3() throws Exception
   {
      // with batch size of 2, receive 1 messages and do not send any
      doSendMessagesByBatchInNoTx(2, 1, 0, 0);
   }
   
   public void doSendMessagesByBatchInNoTx(int batchSize, int reveivedCount, int sendCount, int batchCount) throws Exception
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
      
      if (sendCount > 0)
      {
         targetProducer.send(targetDest, message, 0, 0, 0);
         expectLastCall().times(sendCount);
      }

      if (batchSize > 1 && batchCount > 0)
      {
         targetSession.commit();
         expectLastCall().times(batchCount);
      }

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
      bridge.setMaxBatchSize(batchSize);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      assertFalse(bridge.isStarted());
      bridge.start();
      assertTrue(bridge.isStarted());

      for (int i = 0; i < reveivedCount; i++)
      {
         answer.listener.onMessage(message);
      }

      verify(sourceCFF, sourceCF, sourceConn, sourceSession, sourceConsumer,
            sourceDF, sourceDest);
      verify(targetCFF, targetCF, targetConn, targetSession, targetProducer,
            targetDF, targetDest);
      verify(tm);
      verify(message);
   }
   
   public void testSendMessagesInLocalTx_1() throws Exception
   {
      // with batch size of 1, receive 2 messages and send 2
      doSendMessagesByBatchInLocalTx(1, 2, 2, 2);
   }
   
   public void testSendMessagesInLocalTx_2() throws Exception
   {
      // with batch size of 2, receive 2 messages and send 2
      doSendMessagesByBatchInLocalTx(2, 2, 2, 1);
   }
   
   public void testSendMessagesInLocalTx_3() throws Exception
   {
      // with batch size of 2, receive 1 messages and do not send any
      doSendMessagesByBatchInLocalTx(2, 1, 0, 0);
   }
   
   public void doSendMessagesByBatchInLocalTx(int batchSize, int reveivedCount, int sendCount, int batchCount) throws Exception
   {
      ConnectionFactoryFactory commonCFF = createStrictMock(ConnectionFactoryFactory.class);
      ConnectionFactory commonCF = createStrictMock(ConnectionFactory.class);
      Connection commonConn = createStrictMock(Connection.class);
      Session commonSession = createStrictMock(Session.class);
      MessageConsumer sourceConsumer = createStrictMock(MessageConsumer.class);
      DestinationFactory sourceDF = createStrictMock(DestinationFactory.class);
      Destination sourceDest = createStrictMock(Destination.class);
      MessageProducer targetProducer = createStrictMock(MessageProducer.class);
      DestinationFactory targetDF = createStrictMock(DestinationFactory.class);
      Destination targetDest = createStrictMock(Destination.class);
      TransactionManager tm = createStrictMock(TransactionManager.class);
      Message message = createNiceMock(Message.class);

      expect(tm.suspend()).andReturn(null);
      expect(sourceDF.createDestination()).andReturn(sourceDest);
      expect(targetDF.createDestination()).andReturn(targetDest);
      expect(commonCFF.createConnectionFactory()).andReturn(commonCF);
      expect(commonCF.createConnection()).andReturn(commonConn);
      commonConn.setExceptionListener(isA(ExceptionListener.class));
      expect(commonConn.createSession(anyBoolean(), anyInt())).andReturn(
            commonSession);
      expect(commonSession.createConsumer(sourceDest))
            .andReturn(sourceConsumer);
      expect(commonSession.createProducer(null)).andReturn(targetProducer);
      SetMessageListenerAnswer answer = new SetMessageListenerAnswer();
      sourceConsumer.setMessageListener(isA(MessageListener.class));
      expectLastCall().andAnswer(answer);
      commonConn.start();
      
      if (sendCount > 0)
      {
         targetProducer.send(targetDest, message, 0, 0, 0);
         expectLastCall().times(sendCount);
      }

      if (batchCount > 0)
      {
         commonSession.commit();
         expectLastCall().times(batchCount);
      }

      replay(commonCFF, commonCF, commonConn, commonSession, sourceConsumer,
            sourceDF, sourceDest);
      replay(targetProducer, targetDF, targetDest);
      replay(tm);
      replay(message);

      BridgeImpl bridge = new BridgeImpl();
      assertNotNull(bridge);

      bridge.setSourceConnectionFactoryFactory(commonCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(commonCFF);
      bridge.setTargetDestinationFactory(targetDF);
      bridge.setFailureRetryInterval(-1);
      bridge.setMaxRetries(-1);
      bridge.setMaxBatchSize(batchSize);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      assertFalse(bridge.isStarted());
      bridge.start();
      assertTrue(bridge.isStarted());

      for (int i = 0; i < reveivedCount; i++)
      {
         answer.listener.onMessage(message);
      }

      verify(commonCFF, commonCF, commonConn, commonSession, sourceConsumer,
            sourceDF, sourceDest);
      verify(targetProducer, targetDF, targetDest);
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
