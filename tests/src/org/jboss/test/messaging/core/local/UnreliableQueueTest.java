/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.local;

import org.jboss.test.messaging.core.local.base.QueueTestBase;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.message.TransactionalMessageStore;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;

import javax.transaction.TransactionManager;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class UnreliableQueueTest extends QueueTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private ServiceContainer sc;
   private MessageStore ms;
   private TransactionManager tm;

   // Constructors --------------------------------------------------

   public UnreliableQueueTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      sc = new ServiceContainer("transaction, jca, database");
      sc.start();

      InitialContext ic = new InitialContext();
      tm = (TransactionManager)ic.lookup("java:/TransactionManager");
      ic.close();

      ms = new TransactionalMessageStore("store0", tm);

      channel = new Queue("test", ms, tm);
      super.setUp();
   }

   public void tearDown() throws Exception
   {
      channel.close();
      channel = null;

      tm = null;

      sc.stop();

      super.tearDown();
   }

   public void crashChannel() throws Exception
   {
      // doesn't matter
   }

   public void recoverChannel() throws Exception
   {
      // doesn't matter
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
