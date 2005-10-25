/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.local;

import org.jboss.test.messaging.core.base.ChannelTestBase;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.local.DurableSubscription;
import org.jboss.messaging.core.persistence.HSQLDBPersistenceManager;
import org.jboss.messaging.core.message.PersistentMessageStore;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DurableSubscriptionTest extends ChannelTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private HSQLDBPersistenceManager pm;

   // Constructors --------------------------------------------------

   public DurableSubscriptionTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      pm = new HSQLDBPersistenceManager("jdbc:hsqldb:mem:messaging");
      ms = new PersistentMessageStore("persistent-message-store", pm);
      tr.setPersistenceManager(pm);

      channel = new DurableSubscription("testDurableSubscription", null, null, ms, pm);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      log.debug("tearing down");

      channel.close();
      channel = null;

      pm.stop();
      ms = null;

      super.tearDown();
   }

   public void crashChannel() throws Exception
   {
      channel.close();
      channel = null;

   }

   public void recoverChannel() throws Exception
   {
      channel = new Queue("test", ms, pm);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
