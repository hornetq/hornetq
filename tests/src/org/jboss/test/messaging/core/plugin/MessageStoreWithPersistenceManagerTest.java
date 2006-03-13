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
package org.jboss.test.messaging.core.plugin;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.test.messaging.core.plugin.base.MessageStoreTestBase;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessageStoreWithPersistenceManagerTest extends MessageStoreTestBase
{
   // Constants -----------------------------------------------------

   protected Logger log = Logger.getLogger(MessageStoreWithPersistenceManagerTest.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected MessageStore ms2;
   
   protected PersistenceManager pm;

   // Constructors --------------------------------------------------

   public MessageStoreWithPersistenceManagerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      pm = new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager());
      pm.start();
      
      ms = new SimpleMessageStore("s9");

      ms2 = new SimpleMessageStore("s10");

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      ms = null;
      ms2 = null;
      super.tearDown();
   }

   /**
    * In a distributed configuration, multiple persistent store may access the same database.
    */
   
   //Note - this test is commented out since it no longer makes sense because we no longer record message store id
   //in the message reference table
   //It is testing behaviour that no longer exists
   
  
   
//   public void testTwoStoresSameDatabase() throws Exception
//   {
//      Message m =
//         MessageFactory.createMessage("message0", true, 777l, 888l, (byte)9, headers, "payload");
//
//      assertEquals(0, pm.getMessageReferenceCount(m.getMessageID()));
//
//      MessageReference ref = ms.reference(m);
//      ref.incrementChannelCount();
//      pm.addReference("channel1", ref, null);
//      log.debug("referenced " + m + " using " + ms);
//      assertCorrectReference(ref, ms.getStoreID(), m);
//      assertEquals(1, pm.getMessageReferenceCount(m.getMessageID()));
//
//      // add the same message to the second store
//      MessageReference ref2 = ms2.reference(m);
//      ref2.incrementChannelCount();
//      pm.addReference("channel1", ref2, null);
//      log.debug("referenced " + m + " using " + ms2);
//      assertCorrectReference(ref2, ms2.getStoreID(), m);
//      assertEquals(2, pm.getMessageReferenceCount(m.getMessageID()));
//
//      assertFalse(ref == ref2);      
//                  
//      pm.removeReference("channel1", ref, null);
//      
//      ref.decrementChannelCount();
//
//      assertEquals(1, pm.getMessageReferenceCount(m.getMessageID()));
//
//      // ... but because the message is still in the database, trying to get a new reference
//      // is successful.
//
//      ref = ms.reference((String)m.getMessageID());
//      ref.incrementChannelCount();
//      assertCorrectReference(ref, ms.getStoreID(), m);
//      
//      assertEquals(2, pm.getMessageReferenceCount(m.getMessageID()));
//
//      
//      log.info("ref cc:" + ref.getChannelCount());
//      pm.removeReference("channel1", ref, null);
//      ref.decrementChannelCount();
//      
//      
//      pm.removeReference("channel1", ref2, null);
//      ref2.decrementChannelCount();
//
//      assertNull(ms.reference((String)m.getMessageID()));
//      assertNull(ms2.reference((String)m.getMessageID()));
//      assertEquals(0, pm.getMessageReferenceCount(m.getMessageID()));
//   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}