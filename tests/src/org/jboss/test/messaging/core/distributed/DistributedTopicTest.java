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
package org.jboss.test.messaging.core.distributed;

import org.jboss.test.messaging.core.distributed.base.DistributedTopicTestBase;
import org.jboss.messaging.core.distributed.DistributedTopic;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DistributedTopicTest extends DistributedTopicTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public DistributedTopicTest(String name)
   {
      super(name);
   }

   // DistributedQueueTestBase overrides ---------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      topic = new DistributedTopic("test", dispatcher);
      topic2 = new DistributedTopic("test", dispatcher2);
      topic3 = new DistributedTopic("test", dispatcher3);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      ((DistributedTopic)topic).close();
      topic = null;

      topic2.close();
      topic2 = null;

      topic3.close();
      topic3 = null;

      super.tearDown();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
