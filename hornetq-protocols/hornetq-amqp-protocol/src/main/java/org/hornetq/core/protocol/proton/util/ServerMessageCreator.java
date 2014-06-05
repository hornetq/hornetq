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

package org.hornetq.core.protocol.proton.util;

import org.hornetq.core.protocol.proton.client.MessageCreator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.ServerMessageImpl;

/**
 * @author Clebert Suconic
 */
public class  ServerMessageCreator implements MessageCreator<ServerMessageImpl>
{
   private final HornetQServer server;

   public ServerMessageCreator(HornetQServer server)
   {
      this.server = server;
   }

   @Override
   public ServerMessageImpl createMessage()
   {
      return new ServerMessageImpl(server.getStorageManager().generateUniqueID(), 512);
   }
}

