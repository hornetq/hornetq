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

import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.test.messaging.core.local.base.PagingFilteredQueueTestBase;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class NonRecoverablePagingFilteredQueueTest extends PagingFilteredQueueTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

  
   // Constructors --------------------------------------------------

   public NonRecoverablePagingFilteredQueueTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      
      queue = new PagingFilteredQueue("queue1", 1, ms, pm, true, false, -1, null);
   }
   
   public void tearDown() throws Exception
   {
      queue.close();
      super.tearDown();
   }

   public void crashChannel() throws Exception
   {
      queue.close();
   }

   public void recoverChannel() throws Exception
   {
      queue = new PagingFilteredQueue("queue1", 1, ms, pm, true, false, -1, null);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

