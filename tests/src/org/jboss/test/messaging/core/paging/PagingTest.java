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

import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.message.CoreMessage;
import org.jboss.messaging.core.message.MessageFactory;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class PagingTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ServiceContainer sc;
   protected PersistenceManager pm;
   protected MessageStore ms;

   // Constructors --------------------------------------------------

   public PagingTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testPaging() throws Exception
   {
      Queue p = new Queue(0, ms, pm, true, true, 100, 20, 10, new QueuedExecutor(), null);

      CoreMessage m = null;

      m = MessageFactory.createCoreMessage(0);
      p.handle(null, ms.reference(m), null);

      m = MessageFactory.createCoreMessage(1);
      p.handle(null, ms.reference(m), null);

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      sc = new ServiceContainer("all,-remoting,-security");
      sc.start();

      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(), null,
                                    true, true, true, 100);      
      pm.start();
            
      ms = new SimpleMessageStore();
      ms.start();
   }

   public void tearDown() throws Exception
   {
      pm.stop();
      ms.stop();
      sc.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
