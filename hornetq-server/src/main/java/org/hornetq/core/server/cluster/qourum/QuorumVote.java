/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.core.server.cluster.qourum;

/**
 * the vote itself. the vote can be decided by the enquirer or sent out to each node in the quorum.
 */
public interface QuorumVote<T>
{
   /**
    * called by the {@link org.hornetq.core.server.cluster.qourum.QuorumManager} when one of teh nodes in the quorum is
    * successfully connected to. The QuorumVote can then decide whether or not a decision can be made with just that information.
    *
    * @return the vote to use
    */
   Vote connected();

   /**
    * called by the {@link org.hornetq.core.server.cluster.qourum.QuorumManager} fails to connect to a node in the quorum.
    * The QuorumVote can then decide whether or not a decision can be made with just that information however the node
    * cannot cannot be asked.
    * @return the vote to use
    */
   Vote notConnected();

   /**
    * called by the {@link org.hornetq.core.server.cluster.qourum.QuorumManager} when a vote can be made, either from the
    * cluster or decided by itself.
    *
    * @param vote the vote to make.
    */
   void vote(Vote vote);

   /**
    * get the decion of the vote
    *
    * @return the voting decision
    */
   T getDecision();
}
