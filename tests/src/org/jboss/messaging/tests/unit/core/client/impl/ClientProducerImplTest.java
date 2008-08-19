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
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.impl.ClientProducerImpl;
import org.jboss.messaging.core.client.impl.ClientProducerInternal;
import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
import org.jboss.messaging.core.server.CommandManager;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TokenBucketLimiter;
import org.jboss.messaging.util.TokenBucketLimiterImpl;

/**
 * 
 * A ClientProducerImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientProducerImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ClientProducerImplTest.class);

   // Public -----------------------------------------------------------------------------------------------------------

   public void testConstructor() throws Exception
   {
      testConstructor(16521652, false, false);
      testConstructor(16521652, false, true);
      testConstructor(16521652, true, false);
      testConstructor(16521652, true, true);
      testConstructor(-1, false, false);
      testConstructor(-1, false, true);
      testConstructor(-1, true, false);
      testConstructor(-1, true, true);
   }
   
   public void testSend() throws Exception
   {
      testSend(-1, 652652, new SimpleString("uyuyyu"), null, false, false, false);
      testSend(-1, 652652, new SimpleString("uyuyyu"), null, false, false, true);
      testSend(-1, 652652, new SimpleString("uyuyyu"), null, false, true, false);
      testSend(-1, 652652, new SimpleString("uyuyyu"), null, false, true, true);
      testSend(-1, 652652, new SimpleString("uyuyyu"), null, true, false, false);
      testSend(-1, 652652, new SimpleString("uyuyyu"), null, true, false, true);
      testSend(-1, 652652, new SimpleString("uyuyyu"), null, true, true, false);
      testSend(-1, 652652, new SimpleString("uyuyyu"), null, true, true, true);
      
      testSend(-1, 652652, null, new SimpleString("uyuyyu"), false, false, false);
      testSend(-1, 652652, null, new SimpleString("uyuyyu"), false, false, true);
      testSend(-1, 652652, null, new SimpleString("uyuyyu"), false, true, false);
      testSend(-1, 652652, null, new SimpleString("uyuyyu"), false, true, true);
      testSend(-1, 652652, null, new SimpleString("uyuyyu"), true, false, false);
      testSend(-1, 652652, null, new SimpleString("uyuyyu"), true, false, true);
      testSend(-1, 652652, null, new SimpleString("uyuyyu"), true, true, false);
      testSend(-1, 652652, null, new SimpleString("uyuyyu"), true, true, true);
      
      testSend(652652, -1, new SimpleString("uyuyyu"), null, false, false, false);
      testSend(652652, -1, new SimpleString("uyuyyu"), null, false, false, true);
      testSend(652652, -1, new SimpleString("uyuyyu"), null, false, true, false);
      testSend(652652, -1, new SimpleString("uyuyyu"), null, false, true, true);
      testSend(652652, -1, new SimpleString("uyuyyu"), null, true, false, false);
      testSend(652652, -1, new SimpleString("uyuyyu"), null, true, false, true);
      testSend(652652, -1, new SimpleString("uyuyyu"), null, true, true, false);
      testSend(652652, -1, new SimpleString("uyuyyu"), null, true, true, true);
      
      testSend(652652, -1, null, new SimpleString("uyuyyu"), false, false, false);
      testSend(652652, -1, null, new SimpleString("uyuyyu"), false, false, true);
      testSend(652652, -1, null, new SimpleString("uyuyyu"), false, true, false);
      testSend(652652, -1, null, new SimpleString("uyuyyu"), false, true, true);
      testSend(652652, -1, null, new SimpleString("uyuyyu"), true, false, false);
      testSend(652652, -1, null, new SimpleString("uyuyyu"), true, false, true);
      testSend(652652, -1, null, new SimpleString("uyuyyu"), true, true, false);
      testSend(652652, -1, null, new SimpleString("uyuyyu"), true, true, true);
      
      testSend(652652, 476476, new SimpleString("uyuyyu"), null, false, false, false);
      testSend(652652, 476476, new SimpleString("uyuyyu"), null, false, false, true);
      testSend(652652, 476476, new SimpleString("uyuyyu"), null, false, true, false);
      testSend(652652, 476476, new SimpleString("uyuyyu"), null, false, true, true);
      testSend(652652, 476476, new SimpleString("uyuyyu"), null, true, false, false);
      testSend(652652, 476476, new SimpleString("uyuyyu"), null, true, false, true);
      testSend(652652, 476476, new SimpleString("uyuyyu"), null, true, true, false);
      testSend(652652, 476476, new SimpleString("uyuyyu"), null, true, true, true);
      
      testSend(652652, 476476, null, new SimpleString("uyuyyu"), false, false, false);
      testSend(652652, 476476, null, new SimpleString("uyuyyu"), false, false, true);
      testSend(652652, 476476, null, new SimpleString("uyuyyu"), false, true, false);
      testSend(652652, 476476, null, new SimpleString("uyuyyu"), false, true, true);
      testSend(652652, 476476, null, new SimpleString("uyuyyu"), true, false, false);
      testSend(652652, 476476, null, new SimpleString("uyuyyu"), true, false, true);
      testSend(652652, 476476, null, new SimpleString("uyuyyu"), true, true, false);
      testSend(652652, 476476, null, new SimpleString("uyuyyu"), true, true, true);
   }
   
   public void testReceiveCredits() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      
      final int initialCredits = 7612672;

      EasyMock.replay(session, cm, pd);
      
      ClientProducerInternal producer =
         new ClientProducerImpl(session, 7876L, 76767L, new SimpleString("uhasuuhs"), null,
                                false, false, initialCredits,
                                pd, cm);
       
      assertEquals(initialCredits, producer.getAvailableCredits());
      
      final int credits1 = 1928;
      final int credits2 = 18272;
      final int credits3 = 309;
      producer.receiveCredits(credits1);
      assertEquals(initialCredits + credits1, producer.getAvailableCredits());
      producer.receiveCredits(credits2);
      assertEquals(initialCredits + credits1 + credits2, producer.getAvailableCredits());
      producer.receiveCredits(credits3);
      assertEquals(initialCredits + credits1 + credits2 + credits3, producer.getAvailableCredits());
      
      EasyMock.verify(session, cm, pd);      
   }
   
   public void testClose() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
      
      final long clientTargetID = 121212;  
      
      EasyMock.replay(session, cm, pd);
      
      ClientProducerInternal producer =
         new ClientProducerImpl(session, 7876L, clientTargetID, new SimpleString("uhasuuhs"), null,
                                false, false, 8767878,
                                pd, cm);
      
      assertFalse(producer.isClosed());
      
      EasyMock.verify(session, cm, pd);
      EasyMock.reset(session, cm, pd);
            
      session.removeProducer(producer);
      pd.unregister(clientTargetID);
      
      EasyMock.expect(cm.sendCommandBlocking(7876L, new PacketImpl(PacketImpl.CLOSE))).andReturn(null);
      
      EasyMock.replay(session, cm, pd);
      
      producer.close();
      
      EasyMock.verify(session, cm, pd);
      
      assertTrue(producer.isClosed());   
      
      EasyMock.reset(session, cm, pd);
      
      EasyMock.replay(session, cm, pd);
      
      //close again should do nothing
      
      producer.close();
      
      EasyMock.verify(session, cm, pd);      
   }

   public void testCleanUp() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);

      EasyMock.replay(session, cm, pd);
      
      final long clientTargetID = 121212;
      
      ClientProducerInternal producer =
         new ClientProducerImpl(session, 7876L, clientTargetID, new SimpleString("uhasuuhs"), null,
                                false, false, 8767878,
                                pd, cm);

      EasyMock.verify(session, cm, pd);

      EasyMock.reset(session, cm, pd);
      
      session.removeProducer(producer);
      
      pd.unregister(clientTargetID);
      
      EasyMock.replay(session, cm, pd);
      
      producer.cleanUp();

      EasyMock.verify(session, cm, pd);
   }
   
   // Private ----------------------------------------------------------------------------------------
   
   private void testConstructor(final int maxRate, final boolean blockOnNP, final boolean blockOnP) throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
       
      SimpleString address = new SimpleString("uhasuuhs");
      
      final int initialCredits = 7612672;

      EasyMock.replay(session, cm, pd);
      
      TokenBucketLimiter limiter = maxRate != -1 ? new TokenBucketLimiterImpl(maxRate, false) : null;
      
      ClientProducerInternal producer =
         new ClientProducerImpl(session, 7876L, 76767L, address, limiter,
                                blockOnNP, blockOnP, initialCredits,
                                pd, cm);
      
      EasyMock.verify(session, cm, pd);
      
      assertEquals(address, producer.getAddress());
      assertEquals(initialCredits, producer.getInitialWindowSize());
      assertEquals(maxRate, producer.getMaxRate());
      assertEquals(blockOnNP, producer.isBlockOnNonPersistentSend());
      assertEquals(blockOnP, producer.isBlockOnPersistentSend());
      assertFalse(producer.isClosed());      
   }
   
   private void testSend(final int maxRate, final int windowSize,
                         final SimpleString prodAddress, final SimpleString sendAddress,
                         final boolean blockOnNonPersistentSend,
                         final boolean blockOnPersistentSend,
                         boolean durable) throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      PacketDispatcher pd = EasyMock.createStrictMock(PacketDispatcher.class);
      ClientMessage message = EasyMock.createStrictMock(ClientMessage.class);
      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
            
      if (sendAddress != null)
      {
         message.setDestination(sendAddress);
      }
      else
      {
         message.setDestination(prodAddress);
      }
      
      EasyMock.expect(message.isDurable()).andReturn(durable);
            
      TokenBucketLimiter limiter = maxRate != -1 ? EasyMock.createStrictMock(TokenBucketLimiter.class) : null;
      
      if (limiter != null)
      {
         limiter.limit();
      }
      
      final int targetID = 91821982;
      
      boolean sendBlocking = durable ? blockOnPersistentSend : blockOnNonPersistentSend;
 
      if (sendBlocking)
      {
         EasyMock.expect(cm.sendCommandBlocking(targetID, new ProducerSendMessage(message))).andReturn(null);
      }
      else
      {
         cm.sendCommandOneway(targetID, new ProducerSendMessage(message));
      }
      
      final int messageSize = 123;
      
      if (sendAddress == null && windowSize != -1)
      {
         EasyMock.expect(message.getEncodeSize()).andReturn(messageSize);
      }
      
      EasyMock.replay(session, cm, message, pd);
            
      ClientProducerInternal producer =
         new ClientProducerImpl(session, targetID, 76767L, prodAddress, limiter,
               blockOnNonPersistentSend, blockOnPersistentSend, windowSize,
                                pd, cm);
      
      if (sendAddress != null)
      {
         producer.send(sendAddress, message);
      }
      else
      {
         producer.send(message);
      }
      
      EasyMock.verify(session, cm, message, pd);
      
      if (sendAddress == null && windowSize != -1)
      {
         //Credits should have been depleted
         
         assertEquals(windowSize - messageSize, producer.getAvailableCredits());
      }
      else
      {
         assertEquals(windowSize, producer.getAvailableCredits());
      }
   }
   
}

