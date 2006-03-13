/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.ChannelState;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.test.messaging.core.base.StateTestBase;



/**
 * Tests a non-recoverable state that DOES NOT accept reliable messages.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

      ms = new SimpleMessageStore("ms0");
      channel = new SimpleChannel(1, ms);
   
      // the state DOES not accept reliable messages
      state = new ChannelState(channel, persistenceManagerDelegate, false, false, 100, 20, 10);
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