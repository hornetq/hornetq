/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */


package org.hornetq.core.journal.impl;

import org.hornetq.core.journal.IOCallback;
import org.hornetq.utils.VariableLatch;

/**
 * A TransactionCallback
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class TransactionCallback implements IOCallback
{
   private final VariableLatch countLatch = new VariableLatch();

   private volatile String errorMessage = null;

   private volatile int errorCode = 0;

   public void countUp()
   {
      countLatch.up();
   }

   public void done()
   {
      countLatch.down();
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
