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
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

      ms = new SimpleMessageStore("s60");

      channel = new SimpleChannel(1, ms);
      
      // the state accepts reliable messages
      state = new ChannelState(channel, persistenceManagerDelegate, null, true, true, 100, 20, 10);

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