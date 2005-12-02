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

package org.jboss.test.messaging.core.distributed.topic.base;

import org.jboss.messaging.core.distributed.topic.DistributedTopic;
import org.jboss.messaging.core.distributed.topic.RemoteTopic;
import org.jboss.util.NotImplementedException;

/**
 * Useful for testing package protected features of DistributedTopic
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DistributedTopicPackageProtectedAccess
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private DistributedTopic dt;

   // Constructors --------------------------------------------------

   public DistributedTopicPackageProtectedAccess(DistributedTopic dt)
   {
      this.dt = dt;
   }

   // Public --------------------------------------------------------

   public RemoteTopic getRemoteTopic()
   {
      throw new NotImplementedException("FIXME");
      //return dt.getRemoteTopic();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
