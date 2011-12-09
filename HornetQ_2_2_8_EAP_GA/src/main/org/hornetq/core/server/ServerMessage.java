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

import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.paging.PagingStore;

/**
 * 
 * A ServerMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface ServerMessage extends MessageInternal, EncodingSupport
{
   void setMessageID(long id);

   MessageReference createReference(Queue queue);

   int incrementRefCount() throws Exception;

   int decrementRefCount() throws Exception;

   int incrementDurableRefCount();

   int decrementDurableRefCount();

   ServerMessage copy(long newID);

   ServerMessage copy();

   int getMemoryEstimate();

   int getRefCount();

   ServerMessage makeCopyForExpiryOrDLA(long newID, boolean expiry) throws Exception;

   void setOriginalHeaders(ServerMessage other, boolean expiry);

   void setPagingStore(PagingStore store);

   PagingStore getPagingStore();
   
   // Is there any _HQ_ property being used
   boolean hasInternalProperties();

   boolean storeIsPaging();

   void encodeMessageIDToBuffer();
   
   byte [] getDuplicateIDBytes();
   
   Object getDuplicateProperty();
}
