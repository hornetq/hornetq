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
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.message.PersistentMessageStore;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;

import javax.transaction.TransactionManager;
import javax.naming.InitialContext;

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

   private ServiceContainer sc;
   private HSQLDBPersistenceManager pm;
   private MessageStore ms;
   private TransactionManager tm;

   // Constructors --------------------------------------------------

   public ReliableTopicTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      sc = new ServiceContainer("transaction, jca, database");                            
      sc.start();

      pm = new HSQLDBPersistenceManager();

      ms = new PersistentMessageStore("store0", pm, tm);

      InitialContext ic = new InitialContext();
      tm = (TransactionManager)ic.lookup("java:/TransactionManager");
      ic.close();

      channel = new Topic("test", ms, pm, tm);
      super.setUp();
   }

   public void tearDown() throws Exception
   {
      channel.close();
      channel = null;


      tm = null;

      pm.stop();
      sc.stop();

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
