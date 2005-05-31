/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.TransactionalChannel;
import org.jboss.messaging.core.message.RoutableSupport;
import org.jboss.messaging.core.util.transaction.TransactionManagerImpl;

import javax.transaction.Transaction;
import javax.transaction.RollbackException;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;



/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class TransactionalChannelSupportTest extends ChannelSupportTest
{
   // Attributes ----------------------------------------------------

   protected TransactionManagerImpl tm;
   private TransactionalChannel transactionalChannel;

   // Constructors --------------------------------------------------

   public TransactionalChannelSupportTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      if (channel != null)
      {
         transactionalChannel = (TransactionalChannel)channel;
         tm = TransactionManagerImpl.getInstance();
         transactionalChannel.setTransactionManager(tm);
      }

      super.setUp();
   }

   public void tearDown()throws Exception
   {
      transactionalChannel = null;
      if (tm != null)
      {
         tm.setState(TransactionManagerImpl.OPERATIONAL);
      }
      tm = null;
      super.tearDown();
   }


   //
   // Will also run all ChannelSupportTest's tests
   //

   public void testSetTransactionManager()
   {
      if (skip()) { return; }

      assertTrue(tm == transactionalChannel.getTransactionManager());

      transactionalChannel.setTransactionManager(null);
      assertNull(transactionalChannel.getTransactionManager());

      transactionalChannel.setTransactionManager(tm);
      assertTrue(tm == transactionalChannel.getTransactionManager());
   }

   public void testHandleNoTransactionManager()
   {
      if (skip()) { return; }

      transactionalChannel.setTransactionManager(null);
      assertNull(transactionalChannel.getTransactionManager());

      RoutableSupport r = new RoutableSupport("one");

      // no transaction manager means regular non-transactional handling
      assertTrue(transactionalChannel.handle(r));

      assertFalse(transactionalChannel.hasMessages());
      List l = receiverOne.getMessages();
      assertEquals(1, l.size());
      assertEquals("one", ((RoutableSupport)l.get(0)).getMessageID());
   }

   public void testHandleBrokenTransactionManager()
   {
      if (skip()) { return; }

      tm.setState(TransactionManagerImpl.BROKEN);

      try
      {
         transactionalChannel.handle(new RoutableSupport(""));
         fail("Should have thrown exception");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }

   public void testHandleNoActiveTransaction()
   {
      if (skip()) { return; }

      RoutableSupport r = new RoutableSupport("one");

      // no active transaction means regular non-transactional handling
      assertTrue(transactionalChannel.handle(r));

      assertFalse(transactionalChannel.hasMessages());
      List l = receiverOne.getMessages();
      assertEquals(1, l.size());
      assertEquals("one", ((RoutableSupport)l.get(0)).getMessageID());
   }

   public void testHandleActiveTransactionSynchronousChannel() throws Exception
   {
      if (skip()) { return; }

      assertTrue(transactionalChannel.setSynchronous(true));

      RoutableSupport r = new RoutableSupport("one");

      tm.begin();
      Transaction transaction = tm.getTransaction();

      transactionalChannel.handle(r);

      assertFalse(transactionalChannel.hasMessages());
      assertTrue(receiverOne.getMessages().isEmpty());

      try
      {
         transaction.commit();
         fail("should have thrown RollbackException");
      }
      catch(RollbackException e)
      {
         // OK
      }
   }

   public void testHandleActiveTransactionAsynchronousChannel() throws Exception
   {
      if (skip()) { return; }

      assertTrue(transactionalChannel.setSynchronous(false));

      RoutableSupport r = new RoutableSupport("one");

      tm.begin();
      Transaction transaction = tm.getTransaction();

      transactionalChannel.handle(r);

      assertFalse(transactionalChannel.hasMessages());
      assertTrue(receiverOne.getMessages().isEmpty());

      transaction.commit();

      assertFalse(transactionalChannel.hasMessages());
      List l = receiverOne.getMessages();
      assertEquals(1, l.size());
      assertEquals("one", ((RoutableSupport)l.get(0)).getMessageID());
   }



   public void testHandleActiveTransactionAsynchronousChannel2() throws Exception
   {
      if (skip()) { return; }

      assertTrue(transactionalChannel.setSynchronous(false));

      RoutableSupport r1 = new RoutableSupport("1");
      RoutableSupport r2 = new RoutableSupport("2");
      RoutableSupport r3 = new RoutableSupport("3");

      tm.begin();
      Transaction transaction = tm.getTransaction();


      transactionalChannel.handle(r1);
      transactionalChannel.handle(r2);
      transactionalChannel.handle(r3);

      assertFalse(transactionalChannel.hasMessages());
      assertTrue(receiverOne.getMessages().isEmpty());

      transaction.commit();

      // all messages went to the receiver
      assertFalse(transactionalChannel.hasMessages());
      List l = receiverOne.getMessages();
      assertEquals(3, l.size());

      Set ids = new HashSet();
      for(Iterator i = receiverOne.iterator(); i.hasNext(); )
      {
         ids.add(((RoutableSupport)i.next()).getMessageID());
      }
      assertTrue(ids.contains("1"));
      assertTrue(ids.contains("2"));
      assertTrue(ids.contains("3"));

   }

   private boolean skip()
   {
      return transactionalChannel == null;
   }

}