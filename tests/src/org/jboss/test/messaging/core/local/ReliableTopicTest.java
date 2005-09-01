/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.local;

import org.jboss.test.messaging.core.local.base.TopicTestBase;
import org.jboss.messaging.core.local.Topic;
import org.jboss.messaging.core.persistence.HSQLDBPersistenceManager;
import org.jboss.messaging.core.message.PersistentMessageStore;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ReliableTopicTest extends TopicTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private HSQLDBPersistenceManager pm;

   // Constructors --------------------------------------------------

   public ReliableTopicTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      pm = new HSQLDBPersistenceManager();
      ms = new PersistentMessageStore("persistent-message-store", pm, tm);

      channel = new Topic("test", ms, pm, tm);
   }

   public void tearDown() throws Exception
   {
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
      channel = new Topic("test", ms, pm, tm);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
