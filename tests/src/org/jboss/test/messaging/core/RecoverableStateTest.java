/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.test.messaging.core.base.StateTestBase;
import org.jboss.messaging.core.RecoverableState;
import org.jboss.jms.server.plugin.JDBCMessageStore;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class RecoverableStateTest extends StateTestBase
{
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public RecoverableStateTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------
   
   public void setUp() throws Exception
   {
      super.setUp();

      ms = new JDBCMessageStore("s60", sc.getDataSource(), sc.getTransactionManager());
      ((JDBCMessageStore)ms).start();

      channel = new SimpleChannel("test-channel", ms);

      // the state accepts reliable messages
      state = new RecoverableState(channel, transactionLogDelegate);

      log.debug("setup done");
   }

   public void tearDown()throws Exception
   {
      ms = null;
      state = null;
      channel = null;

      super.tearDown();

      log.debug("tearDown done");
   }

   // Protected ------------------------------------------------------

   protected void crashState() throws Exception
   {
      // TODO
   }

   protected void recoverState() throws Exception
   {
      // TODO
   }

   // Private --------------------------------------------------------

}