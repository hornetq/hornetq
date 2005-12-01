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
package org.jboss.messaging.core.distributed.topic;

import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.ChannelSupport;
import org.jboss.messaging.core.distributed.replicator.Replicator;


/**
 * A local representative of a distributed topic. Each distributed topic peer instance has a
 * remote topic instance connected to its router.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RemoteTopic extends ChannelSupport

 {
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   protected RemoteTopic(String topicName,
                         MessageStore ms,
                         PersistenceManager pm,
                         Replicator replicator)
   {
      super(topicName + ".RemoteTopic", ms, pm, true);
      this.router = replicator;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "RemoteTopic[" + ((Replicator)router).getGroupID() + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
