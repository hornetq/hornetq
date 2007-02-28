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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.util.Iterator;
import java.util.List;

/**
 * A MessagePullPolicy implementation that chooses the remote queue with the most messages.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class DefaultMessagePullPolicy implements MessagePullPolicy
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // MessagePullPolicy implementation -------------------------------------------------------------

   public ClusteredQueue chooseQueue(List queues)
   {
      int maxMessages = 0;
      ClusteredQueue chosenQueue = null;

      for(Iterator i = queues.iterator(); i.hasNext(); )
      {
         ClusteredQueue queue = (ClusteredQueue)i.next();

         if (!queue.isLocal())
         {
            QueueStats stats = queue.getStats();

            if (stats != null)
            {
               int cnt = stats.getMessageCount();

               if (cnt > maxMessages)
               {
                  maxMessages = cnt;
                  chosenQueue = queue;
               }
            }
         }
      }

      return chosenQueue;
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
