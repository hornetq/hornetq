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
package org.jboss.test.messaging.core.paging;

import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.impl.MessagingQueue;
import org.jboss.messaging.core.impl.message.CoreMessage;
import org.jboss.messaging.core.impl.message.SimpleMessageStore;
import org.jboss.test.messaging.JBMServerTestCase;
import org.jboss.test.messaging.util.CoreMessageFactory;


/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class PagingTest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected MessageStore ms;

   // Constructors --------------------------------------------------

   public PagingTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testPaging() throws Exception
   {
      MessagingQueue p = new MessagingQueue(1, "queue0", 1, ms, getPersistenceManager(), true, -1, null, 100, 20, 10, false, 300000);
      p.activate();
           
      CoreMessage m = null;

      m = CoreMessageFactory.createCoreMessage(0);
      p.handle(null, m.createReference(), null);

      m = CoreMessageFactory.createCoreMessage(1);
      p.handle(null, m.createReference(), null);

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

     /* getPersistenceManager() =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(),
                  sc.getPersistenceManagerSQLProperties(),
                  true, true, true, false, 100, !sc.getDatabaseName().equals("oracle"));   
      ((JDBCPersistenceManager)getPersistenceManager()).injectNodeID(1);*/
            
      ms = new SimpleMessageStore();
      ms.start();
   }

   public void tearDown() throws Exception
   {
      ms.stop();
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
