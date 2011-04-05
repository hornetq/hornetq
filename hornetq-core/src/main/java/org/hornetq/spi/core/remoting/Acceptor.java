/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.spi.core.remoting;

import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.management.NotificationService;

/**
 * An Acceptor is used by the Remoting Service to allow clients to connect. It should take care of dispatching client requests
 * to the Remoting Service's Dispatcher.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public interface Acceptor extends HornetQComponent
{
   /**
    * Pause the acceptor and stop it from receiving client requests.
    */
   void pause();

   /**
    * Set the notification service for this acceptor to use.
    *
    * @param notificationService the notification service
    */
   void setNotificationService(NotificationService notificationService);
}
