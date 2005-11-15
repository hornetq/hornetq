/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.test.messaging.core.base.StateTestBase;
import org.jboss.messaging.core.message.MemoryMessageStore;
import org.jboss.messaging.core.NonRecoverableState;


/**
 * Tests a non-recoverable state that DOES NOT accept reliable messages.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class NonRecoverableState1Test extends StateTestBase
{
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public NonRecoverableState1Test(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ms = new MemoryMessageStore("ms0");
      channel = new SimpleChannel("test-channel", ms);

      // the state DOES not accept reliable messages
      state = new NonRecoverableState(channel, false);
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