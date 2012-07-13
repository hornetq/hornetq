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

package org.hornetq.jms.example;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.Transformer;

/**
 * A HatColourChangeTransformer
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class HatColourChangeTransformer implements Transformer
{
   public ServerMessage transform(final ServerMessage message)
   {
      SimpleString propName = new SimpleString("hat");

      SimpleString oldProp = message.getSimpleStringProperty(propName);

      // System.out.println("Old hat colour is " + oldProp);

      // Change the colour
      message.putStringProperty(propName, new SimpleString("blue"));

      return message;
   }

}
