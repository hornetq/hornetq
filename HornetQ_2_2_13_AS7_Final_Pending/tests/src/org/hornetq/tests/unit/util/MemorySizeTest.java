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

package org.hornetq.tests.unit.util;

import junit.framework.TestCase;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.impl.MessageReferenceImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.utils.MemorySize;

/**
 * A MemorySizeTest
 *
 * @author Tim Fox
 *
 *
 */
public class MemorySizeTest extends TestCase
{
   private static final Logger log = Logger.getLogger(MemorySizeTest.class);

   public void testObjectSizes() throws Exception
   {
      MemorySizeTest.log.info("Server message size is " + MemorySize.calculateSize(new MemorySize.ObjectFactory()
      {
         public Object createObject()
         {
            return new ServerMessageImpl(1, 1000);
         }
      }));

      MemorySizeTest.log.info("Message reference size is " + MemorySize.calculateSize(new MemorySize.ObjectFactory()
      {
         public Object createObject()
         {
            return new MessageReferenceImpl();
         }
      }));
   }
}
