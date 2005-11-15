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
import org.jboss.messaging.core.PersistenceManager;
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
      ms = new PersistentMessageStore("test-persitent-store", pm);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      ms = null;
      pm = null;

      super.tearDown();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------


}