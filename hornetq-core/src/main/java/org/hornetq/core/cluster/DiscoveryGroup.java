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

package org.hornetq.core.cluster;

import java.util.List;

import org.hornetq.core.server.management.NotificationService;

/**
 * A DiscoveryGroup
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Nov 2008 09:26:54
 *
 *
 */
public interface DiscoveryGroup
{
   void setNotificationService(NotificationService notificationService);

   String getName();

   List<DiscoveryEntry> getDiscoveryEntries();

   void start() throws Exception;

   void stop() throws Exception;

   boolean isStarted();

   boolean waitForBroadcast(long timeout);

   void registerListener(final DiscoveryListener listener);

   void unregisterListener(final DiscoveryListener listener);
}
