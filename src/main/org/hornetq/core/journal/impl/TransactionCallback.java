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

import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.utils.VariableLatch;

/**
 * A TransactionCallback
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class TransactionCallback implements IOAsyncTask
{
   private final VariableLatch countLatch = new VariableLatch();

   private volatile String errorMessage = null;

   private volatile int errorCode = 0;
   
   private volatile int up = 0;
   
   private volatile int done = 0;
   
   private volatile IOAsyncTask delegateCompletion;

   public void countUp()
   {
      up++;
      countLatch.up();
   }

   public void done()
   {
      countLatch.down();
      if (++done == up && delegateCompletion != null)
      {
         delegateCompletion.done();
         delegateCompletion = null;
      }
   }

   public void waitCompletion() throws InterruptedException
   {
      countLatch.waitCompletion();

      if (errorMessage != null)
      {
         throw new IllegalStateException("Error on Transaction: " + errorCode + " - " + errorMessage);
      }
   }

   public void onError(final int errorCode, final String errorMessage)
   {
      this.errorMessage = errorMessage;

      this.errorCode = errorCode;

      countLatch.down();
      
      if (delegateCompletion != null)
      {
         delegateCompletion.onError(errorCode, errorMessage);
      }
   }

   /**
    * @return the delegateCompletion
    */
   public IOAsyncTask getDelegateCompletion()
   {
      return delegateCompletion;
   }

   /**
    * @param delegateCompletion the delegateCompletion to set
    */
   public void setDelegateCompletion(IOAsyncTask delegateCompletion)
   {
      this.delegateCompletion = delegateCompletion;
   }

   /**
    * @return the errorMessage
    */
   public String getErrorMessage()
   {
      return errorMessage;
   }

   /**
    * @return the errorCode
    */
   public int getErrorCode()
   {
      return errorCode;
   }
   
   
   

}
