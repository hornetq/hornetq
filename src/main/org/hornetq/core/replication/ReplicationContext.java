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

package org.hornetq.core.replication;


/**
 * This represents a set of operations done as part of replication. 
 * When the entire set is done a group of Runnables can be executed.
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public interface ReplicationContext
{
   /** To be called by the replication manager, when new replication is added to the queue */
   void linedUp();

   /** To be called by the replication manager, when data is confirmed on the channel */
   void replicated();
   
   void addReplicationAction(Runnable runnable);
   
   /** To be called when there are no more operations pending */
   void complete();
   
   /** Flush all pending callbacks on the Context */
   void flush();

}
