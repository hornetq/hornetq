/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.test.messaging.core.base.StateTestBase;
import org.jboss.jms.server.plugin.PersistentMessageStore;
import org.jboss.messaging.core.NonRecoverableState;
import org.jboss.messaging.core.plugin.contract.TransactionLogDelegate;
import org.jboss.messaging.core.plugin.JDBCTransactionLog;
import org.jboss.jms.server.plugin.PersistentMessageStore;


/**
 * Tests a non-recoverable state that accepts reliable messages.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class NonRecoverableState2Test extends StateTestBase
{
   // Attributes ----------------------------------------------------

   // requuired by the message store
   TransactionLogDelegate transactionLogDelegate;

   // Constructors --------------------------------------------------

   public NonRecoverableState2Test(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      transactionLogDelegate =
         new JDBCTransactionLog(sc.getDataSource(), sc.getTransactionManager());

      ((JDBCTransactionLog)transactionLogDelegate).start();

      ms = new PersistentMessageStore("ms0", transactionLogDelegate);

      channel = new SimpleChannel("test-channel", ms);

      // the state ACCEPTS reliable messages
      state = new NonRecoverableState(channel, true);
   }

   public void tearDown()throws Exception
   {
      ms = null;
      transactionLogDelegate = null;
      state = null;
      channel = null;

      super.tearDown();
   }

   // Protected ------------------------------------------------------

   protected void crashState() throws Exception
   {
      // irrelevant
   }

   protected void recoverState() throws Exception
   {
      // irrelevant
   }

   // Private --------------------------------------------------------

}