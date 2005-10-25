/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.local;

import org.jboss.test.messaging.core.local.base.QueueTestBase;
import org.jboss.messaging.core.local.Subscription;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class SubscriptionTest extends QueueTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public SubscriptionTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      channel = new Subscription(null, null, ms);
      
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      channel.close();
      channel = null;

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
