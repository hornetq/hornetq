/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.jboss.test.messaging.core.message;

import org.jboss.test.messaging.core.message.base.MessageStoreTestBase;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.message.PersistentMessageStore;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.persistence.JDBCPersistenceManager;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class PersistentMessageStoreTest extends MessageStoreTestBase
{
   // Constants -----------------------------------------------------

   protected Logger log = Logger.getLogger(PersistentMessageStoreTest.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected PersistenceManager pm;

   protected PersistenceManager pm2;
   protected MessageStore ms2;

   // Constructors --------------------------------------------------

   public PersistentMessageStoreTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      pm = new JDBCPersistenceManager();
      pm.start();
      ms = new PersistentMessageStore("test-persitent-store", pm);

      pm2 = new JDBCPersistenceManager();
      pm2.start();
      ms2 = new PersistentMessageStore("test-persistent-store2", pm2);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      ms = null;
      pm = null;

      super.tearDown();
   }

   /**
    * In a distributed configuration, multiple persistent store may access the same database.
    */
   public void testTwoStoresSameDatabase() throws Exception
   {
      Message m = MessageFactory.createMessage("message0", true, 777l, 888l, 9, headers, "payload");

      assertEquals(0, ((JDBCPersistenceManager)pm).getMessageReferenceCount(m.getMessageID()));

      MessageReference ref = ms.reference(m);
      log.debug("referenced " + m + " using " + ms);
      assertCorrectReference(ref, ms.getStoreID(), m);
      assertEquals(1, ((JDBCPersistenceManager)pm).getMessageReferenceCount(m.getMessageID()));

      // add the same message to the second store
      MessageReference ref2 = ms2.reference(m);
      log.debug("referenced " + m + " using " + ms2);
      assertCorrectReference(ref2, ms2.getStoreID(), m);
      assertEquals(2, ((JDBCPersistenceManager)pm2).getMessageReferenceCount(m.getMessageID()));

      assertFalse(ref == ref2);

      // send ref out of scope and call a full GC
      ref = null;
      System.gc();

      // TODO - do I need to keep this here?
      // wait a while (?) for garbage collection (on a multi-processor machine)
      Thread.sleep(3000);

      // the reference is garbage collected and the message store is evicted ...

      assertEquals(1, ((JDBCPersistenceManager)pm2).getMessageReferenceCount(m.getMessageID()));

      // ... but because the message is still in the database, trying to get a new reference
      // is successful.

      ref = ms.getReference(m.getMessageID());
      assertCorrectReference(ref, ms.getStoreID(), m);
      assertCorrectReference(ms2.getReference(m.getMessageID()), ms2.getStoreID(), m);
      assertEquals(2, ((JDBCPersistenceManager)pm2).getMessageReferenceCount(m.getMessageID()));

      // send both references out of scope and call a full GC
      ref = null;
      ref2 = null;
      System.gc();

      assertNull(ms.getReference(m.getMessageID()));
      assertNull(ms2.getReference(m.getMessageID()));
      assertEquals(0, ((JDBCPersistenceManager)pm2).getMessageReferenceCount(m.getMessageID()));
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}