/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.NonRecoverableState;
import org.jboss.messaging.core.plugin.PersistentMessageStore;
import org.jboss.test.messaging.core.base.StateTestBase;


/**
 * Tests a non-recoverable state that accepts reliable messages.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class NonRecoverableState2Test extends StateTestBase
{
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public NonRecoverableState2Test(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ms = new PersistentMessageStore("s61", persistenceManagerDelegate);      

      channel = new SimpleChannel(1, ms);

      // the state ACCEPTS reliable messages
      state = new NonRecoverableState(channel, true);
   }

   public void tearDown()throws Exception
   {
      ms = null;
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