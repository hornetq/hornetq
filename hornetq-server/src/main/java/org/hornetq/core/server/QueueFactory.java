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

package org.hornetq.core.server;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.postoffice.PostOffice;

/**
 * 
 * A QueueFactory
 * 
 * Implementations of this class know how to create queues with the correct attribute values
 * based on default and overrides
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface QueueFactory
{
   Queue createQueue(long persistenceID,
                     final SimpleString address,
                     SimpleString name,
                     Filter filter,
                     PageSubscription pageSubscription,
                     boolean durable,
                     boolean temporary);

   /**
    * This is required for delete-all-reference to work correctly with paging
    * @param postOffice
    */
   void setPostOffice(PostOffice postOffice);
}
