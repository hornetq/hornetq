/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.management;

import org.jboss.messaging.core.client.management.impl.ManagementHelper;

/**
 * A NotificationService
 *
 * @author jmesnil
 *
 *
 */
public interface NotificationService
{
   /** 
    * the message corresponding to a notification will always contain the properties:
    * <ul>
    *   <li><code>ManagementHelper.HDR_NOTIFICATION_TYPE</code> - the type of notification (SimpleString)</li>
    *   <li><code>ManagementHelper.HDR_NOTIFICATION_MESSAGE</code> - a message contextual to the notification (SimpleString)</li>
    *   <li><code>ManagementHelper.HDR_NOTIFICATION_TIMESTAMP</code> - the timestamp when the notification occured (long)</li>
    * </ul>
    * 
    * in addition to the properties defined in <code>props</code>
    * 
    * @see ManagementHelper
    */
   void sendNotification(Notification notification) throws Exception;

   void enableNotifications(boolean enable);

   void addNotificationListener(NotificationListener listener);

   void removeNotificationListener(NotificationListener listener);

}
