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

package org.jboss.messaging.core.journal.impl;

import java.util.concurrent.CountDownLatch;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.journal.IOCallback;
import org.jboss.messaging.core.logging.Logger;

/**
 * A SimpleWaitIOCallback
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class SimpleWaitIOCallback implements IOCallback
{

   private static final Logger log = Logger.getLogger(SimpleWaitIOCallback.class);

   private final CountDownLatch latch = new CountDownLatch(1);

   private volatile String errorMessage;

   private volatile int errorCode = 0;

   public static IOCallback getInstance()
   {
      return new SimpleWaitIOCallback();
   }


   public void done()
   {
      latch.countDown();
   }

   public void onError(final int errorCode, final String errorMessage)
   {
      this.errorCode = errorCode;

      this.errorMessage = errorMessage;

      log.warn("Error Message " + errorMessage);

      latch.countDown();
   }

   public void waitCompletion() throws Exception
   {
      latch.await();
      if (errorMessage != null)
      {
         throw new MessagingException(errorCode, errorMessage);
      }
      return;
   }
}
