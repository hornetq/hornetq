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
package org.jboss.messaging.core.local;

import org.jboss.messaging.core.ManageableCoreDestination;

/**
 * Manageable interface for a topic.
 *
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public interface ManageableTopic extends ManageableCoreDestination
{
   /**
    * Get all subscription count.
    * @return number of registered subscriptions
    */
   int subscriptionCount();
   
   /**
    * Get subscription count.
    * @param durable If true, return durable subscription count.
    *                If false, return non-durable subscription count.
    * @return either durable or non-durable subscription count.
    */
   int subscriptionCount(boolean durable);
   
   // TODO adding more manageable operations
}
