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


package org.hornetq.core.journal.impl;

import org.hornetq.core.logging.Logger;

/**
 * A DummyCallback
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
class DummyCallback extends SyncIOCompletion
{
   private static DummyCallback instance = new DummyCallback();
   
   private static final Logger log = Logger.getLogger(SimpleWaitIOCallback.class);
   
   public static DummyCallback getInstance()
   {
      return instance;
   }

   public void done()
   {
   }

   public void onError(final int errorCode, final String errorMessage)
   {
      log.warn("Error on writing data!" + errorMessage + " code - " + errorCode, new Exception(errorMessage));
   }

   public void waitCompletion() throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.IOCompletion#linedUp()
    */
   public void lineUp()
   {
   }
}

