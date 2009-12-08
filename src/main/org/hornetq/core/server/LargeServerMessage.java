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

/**
 * A LargeMessage
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created 30-Sep-08 10:58:04 AM
 *
 *
 */
public interface LargeServerMessage extends ServerMessage
{
   void addBytes(byte[] bytes) throws Exception;

   /** When a large message is copied (e.g. ExpiryQueue) instead of copying the file, we specify a link between the messages */
   void setLinkedMessage(LargeServerMessage message);

   boolean isFileExists() throws Exception;

   /** Close the files if opened */
   void releaseResources();

   void deleteFile() throws Exception;

   void incrementDelayDeletionCount();

   void decrementDelayDeletionCount() throws Exception;
}
