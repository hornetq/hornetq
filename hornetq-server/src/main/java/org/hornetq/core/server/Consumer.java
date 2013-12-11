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

import java.util.List;

import org.hornetq.core.filter.Filter;

/**
 *
 * A ClientConsumer
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface Consumer
{
   /**
    * There was a change on semantic during 2.3 here.<br/>
    * We now first accept the message, and the actual deliver is done as part of
    * {@link #proceedDeliver(MessageReference)}. This is to avoid holding a lock on the queues while
    * the delivery is being accomplished To avoid a lock on the queue in case of misbehaving
    * consumers.
    * <p>
    * This should return busy if handle is called before proceed deliver is called
    * @param reference
    * @return
    * @throws Exception
    */
   HandleStatus handle(MessageReference reference) throws Exception;

   /**
    * This will proceed with the actual delivery.
    * Notice that handle should hold a readLock and proceedDelivery should release the readLock
    * any lock operation on Consumer should also get a writeLock on the readWriteLock
    * to guarantee there are no pending deliveries
    * @throws Exception
    */
   void proceedDeliver(MessageReference reference) throws Exception;

   Filter getFilter();

   /**
    * Add the in-deliver mode messages to the ref-list passed as parameter
    * @param refList the placeholder for where the output messages will be placed
    */
   List<MessageReference>  getDeliveringMessages();

   String debug();


   /**
    * This method will create a string representation meant for management operations.
    * This is different from the toString method that's meant for debugging and will contain information that regular users won't understand well
    * @return
    */
   String toManagementString();

}
