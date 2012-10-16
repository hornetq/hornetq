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

package org.hornetq.core.client.impl;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.core.message.impl.MessageInternal;

/**
 * A ClientMessageInternal
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 10 Dec 2008 18:05:24
 *
 *
 */
public interface ClientMessageInternal extends ClientMessage, MessageInternal
{
   /** Size used for FlowControl */
   int getFlowControlSize();

   /** Size used for FlowControl */
   void setFlowControlSize(int flowControlSize);

   void onReceipt(ClientConsumerInternal consumer);

   /**
    * Discard unused packets (used on large-message)
    */
   void discardBody();

   void setBuffer(HornetQBuffer buffer);
   
   boolean isCompressed();
}
