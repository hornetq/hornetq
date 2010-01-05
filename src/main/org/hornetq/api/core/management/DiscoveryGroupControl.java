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

package org.hornetq.api.core.management;

/**
 * A DiscoveryGroupControl is used to manage a discovery group.
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public interface DiscoveryGroupControl extends HornetQComponentControl
{
   /**
    * Returns the configuration name of this discovery group.
    */
   String getName();

   /**
    * Returns the address that this discovery group is listening to.
    */
   String getGroupAddress();

   /**
    * Returns the port that this discovery group is listening to.
    */
   int getGroupPort();

   /**
    * Returns the refresh timeout used by this discovery group.
    */
   long getRefreshTimeout();
}
