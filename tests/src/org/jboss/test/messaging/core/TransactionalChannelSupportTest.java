/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.TransactionalChannel;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.message.RoutableSupport;
import org.jboss.messaging.core.util.transaction.TransactionManagerImpl;

import javax.transaction.TransactionManager;
import javax.transaction.Transaction;
import java.util.List;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class TransactionalChannelSupportTest extends ChannelSupportTest
{
   // Attributes ----------------------------------------------------

   protected TransactionManager tm;
   private TransactionalChannel transactionalChannel;

   protected boolean runTransactionalChannelSupportTests = true;

   // Constructors --------------------------------------------------

   public TransactionalChannelSupportTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      if (runTransactionalChannelSupportTests)
      {
         transactionalChannel = (TransactionalChannel)channel;
         tm = TransactionManagerImpl.getInstance();
      }

      super.setUp();
   }

   public void tearDown()throws Exception
   {
      transactionalChannel = null;
      tm = null;
      super.tearDown();
   }


   //
   // Will also run all ChannelSupportTest's tests
   //

   public void testSetTransactionManager()
   {
      if (skip()) { return; }

      transactionalChannel.setTransactionManager(tm);
      assertTrue(tm == transactionalChannel.getTransactionManager());

   }

   public void testIsTransactional()
   {
      if (skip()) { return; }

      assertFalse(transactionalChannel.isTransactional());
      transactionalChannel.setTransactionManager(tm);
      assertTrue(transactionalChannel.isTransactional());
   }


   public void testTransactionalSend() throws Exception
   {
      if (skip()) { return; }

      transactionalChannel.setTransactionManager(tm);

      tm.begin();
      Transaction transaction = tm.getTransaction();

      Routable r1 = new RoutableSupport("1");
      Routable r2 = new RoutableSupport("2");
      Routable r3 = new RoutableSupport("3");

      transactionalChannel.handle(r1);
      transactionalChannel.handle(r2);
      transactionalChannel.handle(r3);

      // no messages are being held by the channel
      assertFalse(transactionalChannel.hasMessages());
      // no messages were delivered to the receiver
      assertTrue(receiverOne.getMessages().isEmpty());

      transaction.commit();

      // all messages went to the receiver
      assertFalse(transactionalChannel.hasMessages());
      List l = receiverOne.getMessages();
      assertEquals(3, l.size());
      assertTrue(l.contains(r1));
      assertTrue(l.contains(r2));
      assertTrue(l.contains(r3));

   }






   private boolean skip()
   {
      return transactionalChannel == null;
   }

}