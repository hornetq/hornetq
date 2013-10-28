/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.core.server.group.impl;

import org.hornetq.api.core.SimpleString;

/**
 * A response to a proposal
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class Response
{
   private final boolean accepted;

   private final SimpleString clusterName;

   private final SimpleString alternativeClusterName;

   private final SimpleString groupId;

   public Response(final SimpleString groupId, final SimpleString clusterName)
   {
      this(groupId, clusterName, null);
   }

   public Response(final SimpleString groupId, final SimpleString clusterName, final SimpleString alternativeClusterName)
   {
      this.groupId = groupId;
      accepted = alternativeClusterName == null;
      this.clusterName = clusterName;
      this.alternativeClusterName = alternativeClusterName;
   }

   public boolean isAccepted()
   {
      return accepted;
   }

   public SimpleString getClusterName()
   {
      return clusterName;
   }

   public SimpleString getAlternativeClusterName()
   {
      return alternativeClusterName;
   }

   public SimpleString getChosenClusterName()
   {
      return alternativeClusterName != null ? alternativeClusterName : clusterName;
   }

   @Override
   public String toString()
   {
      return "accepted = " + accepted +
             " groupid = "  + groupId +
             " clusterName = " +
             clusterName +
             " alternativeClusterName = " +
             alternativeClusterName;
   }

   public SimpleString getGroupId()
   {
      return groupId;
   }
}
