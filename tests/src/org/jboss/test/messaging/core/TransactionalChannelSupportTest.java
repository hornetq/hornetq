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
package org.jboss.test.messaging.core;




/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class TransactionalChannelSupportTest extends ChannelSupportTest
{
//   // Attributes ----------------------------------------------------
//
//   protected TransactionManagerImpl tm;
//   private TransactionalChannel transactionalChannel;
//
//   // Constructors --------------------------------------------------
//
   public TransactionalChannelSupportTest(String name)
   {
      super(name);
   }
//
//   public void setUp() throws Exception
//   {
//      if (channel != null)
//      {
//         transactionalChannel = (TransactionalChannel)channel;
//         tm = TransactionManagerImpl.getInstance();
//         transactionalChannel.setTransactionManager(tm);
//      }
//
//      super.setUp();
//   }
//
//   public void tearDown()throws Exception
//   {
//      transactionalChannel = null;
//      if (tm != null)
//      {
//         tm.setState(TransactionManagerImpl.OPERATIONAL);
//      }
//      tm = null;
//      super.tearDown();
//   }
//
//
//   //
//   // Will also run all ChannelSupportTest's tests
//   //
//
//   public void testSetTransactionManager()
//   {
//      if (skip()) { return; }
//
//      assertTrue(tm == transactionalChannel.getTransactionManager());
//
//      transactionalChannel.setTransactionManager(null);
//      assertNull(transactionalChannel.getTransactionManager());
//
//      transactionalChannel.setTransactionManager(tm);
//      assertTrue(tm == transactionalChannel.getTransactionManager());
//   }
//
//   public void testIsTransactional()
//   {
//      if (skip()) { return; }
//
//      assertTrue(transactionalChannel.isTransactional());
//
//      transactionalChannel.setTransactionManager(null);
//      assertFalse(transactionalChannel.isTransactional());
//   }
//
//
//   //
//   // Transacted handle() tests
//   //
//
//   public void testHandleNoTransactionManager()
//   {
//      if (skip()) { return; }
//
//      transactionalChannel.setTransactionManager(null);
//      assertNull(transactionalChannel.getTransactionManager());
//
//      RoutableSupport r = new RoutableSupport("one");
//
//      // no transaction manager means regular non-transactional handling
//      assertTrue(transactionalChannel.handle(r));
//
//      assertFalse(transactionalChannel.hasMessages());
//      List l = receiverOne.getMessages();
//      assertEquals(1, l.size());
//      assertEquals("one", ((RoutableSupport)l.get(0)).getMessageID());
//   }
//
//   public void testHandleBrokenTransactionManager()
//   {
//      if (skip()) { return; }
//
//      tm.setState(TransactionManagerImpl.BROKEN);
//
//      try
//      {
//         transactionalChannel.handle(new RoutableSupport(""));
//         fail("Should have thrown exception");
//      }
//      catch(IllegalStateException e)
//      {
//         // OK
//      }
//   }
//
//   public void testHandleNoActiveTransaction()
//   {
//      if (skip()) { return; }
//
//      RoutableSupport r = new RoutableSupport("one");
//
//      // no active transaction means regular non-transactional handling
//      assertTrue(transactionalChannel.handle(r));
//
//      assertFalse(transactionalChannel.hasMessages());
//      List l = receiverOne.getMessages();
//      assertEquals(1, l.size());
//      assertEquals("one", ((RoutableSupport)l.get(0)).getMessageID());
//   }
//
//   public void testHandleActiveTransactionSynchronousChannel() throws Exception
//   {
//      if (skip()) { return; }
//
//      assertTrue(transactionalChannel.setSynchronous(true));
//
//      RoutableSupport r = new RoutableSupport("one");
//
//      tm.begin();
//      Transaction transaction = tm.getTransaction();
//
//      transactionalChannel.handle(r);
//
//      assertFalse(transactionalChannel.hasMessages());
//      assertTrue(receiverOne.getMessages().isEmpty());
//
//      try
//      {
//         transaction.commit();
//         fail("should have thrown RollbackException");
//      }
//      catch(RollbackException e)
//      {
//         // OK
//      }
//   }
//
//   public void testHandleActiveTransactionAsynchronousChannel() throws Exception
//   {
//      if (skip()) { return; }
//
//      assertTrue(transactionalChannel.setSynchronous(false));
//
//      RoutableSupport r = new RoutableSupport("one");
//
//      tm.begin();
//      Transaction transaction = tm.getTransaction();
//
//      transactionalChannel.handle(r);
//
//      assertFalse(transactionalChannel.hasMessages());
//      assertTrue(receiverOne.getMessages().isEmpty());
//
//      transaction.commit();
//
//      assertFalse(transactionalChannel.hasMessages());
//      List l = receiverOne.getMessages();
//      assertEquals(1, l.size());
//      assertEquals("one", ((RoutableSupport)l.get(0)).getMessageID());
//   }
//
//   public void testHandleActiveTransactionAsynchronousChannel2() throws Exception
//   {
//      if (skip()) { return; }
//
//      assertTrue(transactionalChannel.setSynchronous(false));
//
//      RoutableSupport r1 = new RoutableSupport("1");
//      RoutableSupport r2 = new RoutableSupport("2");
//      RoutableSupport r3 = new RoutableSupport("3");
//
//      tm.begin();
//      Transaction transaction = tm.getTransaction();
//
//
//      transactionalChannel.handle(r1);
//      transactionalChannel.handle(r2);
//      transactionalChannel.handle(r3);
//
//      assertFalse(transactionalChannel.hasMessages());
//      assertTrue(receiverOne.getMessages().isEmpty());
//
//      transaction.commit();
//
//      // all messages went to the receiver
//      assertFalse(transactionalChannel.hasMessages());
//      List l = receiverOne.getMessages();
//      assertEquals(3, l.size());
//
//      Set ids = new HashSet();
//      for(Iterator i = receiverOne.iterator(); i.hasNext(); )
//      {
//         ids.add(((RoutableSupport)i.next()).getMessageID());
//      }
//      assertTrue(ids.contains("1"));
//      assertTrue(ids.contains("2"));
//      assertTrue(ids.contains("3"));
//
//   }
//
//   //
//   // Transacted acknowledge() tests
//   //
//
//   public void testAcknowledgeNoTransactionManager()
//   {
//      if (skip()) { return; }
//
//      transactionalChannel.setTransactionManager(null);
//      assertNull(transactionalChannel.getTransactionManager());
//
//      // add an undeliverable message to the channel
//      assertTrue(transactionalChannel.setSynchronous(false));
//      receiverOne.setState(ReceiverImpl.NACKING);
//      RoutableSupport r = new RoutableSupport("one");
//      assertTrue(transactionalChannel.handle(r));
//      assertTrue(transactionalChannel.hasMessages());
//      Set undelivered = transactionalChannel.getUndelivered();
//      assertEquals(1, undelivered.size());
//      assertEquals("one", undelivered.iterator().next());
//
//      // no transaction manager means regular non-transactional acknowledgment
//      transactionalChannel.acknowledge("one", receiverOne.getReceiverID());
//
//      assertFalse(transactionalChannel.hasMessages());
//      assertTrue(transactionalChannel.getUndelivered().isEmpty());
//   }
//
//   public void testAcknowledgeBrokenTransactionManager()
//   {
//      if (skip()) { return; }
//
//      // add an undeliverable message to the channel
//      assertTrue(transactionalChannel.setSynchronous(false));
//      receiverOne.setState(ReceiverImpl.NACKING);
//      RoutableSupport r = new RoutableSupport("one");
//      assertTrue(transactionalChannel.handle(r));
//      assertTrue(transactionalChannel.hasMessages());
//      Set undelivered = transactionalChannel.getUndelivered();
//      assertEquals(1, undelivered.size());
//      assertEquals("one", undelivered.iterator().next());
//
//
//      // break the transaction manager
//      tm.setState(TransactionManagerImpl.BROKEN);
//
//      try
//      {
//         transactionalChannel.acknowledge("one", receiverOne.getReceiverID());
//         fail("Should have thrown exception");
//      }
//      catch(IllegalStateException e)
//      {
//         // OK
//      }
//
//      assertTrue(transactionalChannel.hasMessages());
//      undelivered = transactionalChannel.getUndelivered();
//      assertEquals(1, undelivered.size());
//      assertEquals("one", undelivered.iterator().next());
//   }
//
//   public void testAcknowledgeNoActiveTransaction()
//   {
//      if (skip()) { return; }
//
//      // add an undeliverable message to the channel
//      assertTrue(transactionalChannel.setSynchronous(false));
//      receiverOne.setState(ReceiverImpl.NACKING);
//      RoutableSupport r = new RoutableSupport("one");
//      assertTrue(transactionalChannel.handle(r));
//      assertTrue(transactionalChannel.hasMessages());
//      Set undelivered = transactionalChannel.getUndelivered();
//      assertEquals(1, undelivered.size());
//      assertEquals("one", undelivered.iterator().next());
//
//      // no active transaction means regular non-transactional acknowledgment
//      transactionalChannel.acknowledge("one", receiverOne.getReceiverID());
//
//      assertFalse(transactionalChannel.hasMessages());
//      assertTrue(transactionalChannel.getUndelivered().isEmpty());
//   }
//
//   public void testAcknowledgeActiveTransaction() throws Exception
//   {
//      if (skip()) { return; }
//
//      // add an undeliverable message to the channel
//      assertTrue(transactionalChannel.setSynchronous(false));
//      receiverOne.setState(ReceiverImpl.NACKING);
//      RoutableSupport r = new RoutableSupport("one");
//      assertTrue(transactionalChannel.handle(r));
//      assertTrue(transactionalChannel.hasMessages());
//      Set undelivered = transactionalChannel.getUndelivered();
//      assertEquals(1, undelivered.size());
//      assertEquals("one", undelivered.iterator().next());
//
//
//      tm.begin();
//      Transaction transaction = tm.getTransaction();
//
//      // acknowledge transactionally
//      transactionalChannel.acknowledge("one", receiverOne.getReceiverID());
//
//      // the channel should still keep the message
//      assertTrue(transactionalChannel.hasMessages());
//      undelivered = transactionalChannel.getUndelivered();
//      assertEquals(1, undelivered.size());
//      assertEquals("one", undelivered.iterator().next());
//
//      transaction.commit();
//
//      assertFalse(transactionalChannel.hasMessages());
//      assertTrue(transactionalChannel.getUndelivered().isEmpty());
//   }
//
//   public void testAcknowledgeActiveTransactionMultipleMessages() throws Exception
//   {
//      if (skip()) { return; }
//
//      // add an undeliverable message to the channel
//      assertTrue(transactionalChannel.setSynchronous(false));
//      receiverOne.setState(ReceiverImpl.NACKING);
//      assertTrue(transactionalChannel.handle(new RoutableSupport("one")));
//      assertTrue(transactionalChannel.handle(new RoutableSupport("two")));
//      assertTrue(transactionalChannel.handle(new RoutableSupport("three")));
//      assertTrue(transactionalChannel.hasMessages());
//      Set undelivered = transactionalChannel.getUndelivered();
//      assertEquals(3, undelivered.size());
//      assertTrue(undelivered.contains("one"));
//      assertTrue(undelivered.contains("two"));
//      assertTrue(undelivered.contains("three"));
//
//      tm.begin();
//      Transaction transaction = tm.getTransaction();
//
//      // acknowledge transactionally
//      transactionalChannel.acknowledge("one", receiverOne.getReceiverID());
//      transactionalChannel.acknowledge("two", receiverOne.getReceiverID());
//
//      // the channel should still keep the message
//      assertTrue(transactionalChannel.hasMessages());
//      undelivered = transactionalChannel.getUndelivered();
//      assertEquals(3, undelivered.size());
//      assertTrue(undelivered.contains("one"));
//      assertTrue(undelivered.contains("two"));
//      assertTrue(undelivered.contains("three"));
//
//      transaction.commit();
//
//      // the channel should still keep one message
//      assertTrue(transactionalChannel.hasMessages());
//      undelivered = transactionalChannel.getUndelivered();
//      assertEquals(1, undelivered.size());
//      assertTrue(undelivered.contains("three"));
//
//      tm.begin();
//      transaction = tm.getTransaction();
//      transactionalChannel.acknowledge("three", receiverOne.getReceiverID());
//
//      // the channel should still keep one message
//      assertTrue(transactionalChannel.hasMessages());
//      undelivered = transactionalChannel.getUndelivered();
//      assertEquals(1, undelivered.size());
//      assertTrue(undelivered.contains("three"));
//
//      transaction.commit();
//
//      assertFalse(transactionalChannel.hasMessages());
//      assertTrue(transactionalChannel.getUndelivered().isEmpty());
//   }
//
//
//   private boolean skip()
//   {
//      return transactionalChannel == null;
//   }

}