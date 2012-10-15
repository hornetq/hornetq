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

package org.hornetq.core.asyncio;

/**
 * The interface used for AIO Callbacks.
 * @author clebert.suconic@jboss.com
 *
 */
public interface AIOCallback
{
   /**
    * Method for sync notifications. When this callback method is called, there is a guarantee the data is written on the disk.
    * <br><b>Note:</b><i>Leave this method as soon as possible, or you would be blocking the whole notification thread</i> */
   void done();

   /**
    * Method for error notifications.
    * Observation: The whole file will be probably failing if this happens. Like, if you delete the file, you will start to get errors for these operations*/
   void onError(int errorCode, String errorMessage);
}
