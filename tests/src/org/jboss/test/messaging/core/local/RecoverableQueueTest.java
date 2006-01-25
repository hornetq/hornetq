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
package org.jboss.test.messaging.core.local;

import org.jboss.messaging.core.local.Queue;
import org.jboss.jms.server.plugin.PersistentMessageStore;
import org.jboss.messaging.core.plugin.JDBCTransactionLog;
import org.jboss.test.messaging.core.local.base.QueueTestBase;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RecoverableQueueTest extends QueueTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private JDBCTransactionLog tl;

   // Constructors --------------------------------------------------

   public RecoverableQueueTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      tl = new JDBCTransactionLog(sc.getDataSource(), sc.getTransactionManager());
      tl.start();

      ms = new PersistentMessageStore("persistent-message-store", tl);

      channel = new Queue("test", ms, tl);
      
      tr.start(tl);
   }

   public void tearDown() throws Exception
   {
      channel.close();
      channel = null;

      tl.stop();
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
      channel = new Queue("test", ms, tl);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
