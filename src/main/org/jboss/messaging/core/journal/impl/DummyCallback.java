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

import org.jboss.messaging.core.journal.IOCallback;
import org.jboss.messaging.core.logging.Logger;

/**
 * A DummyCallback
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public  class DummyCallback implements IOCallback
{
   static DummyCallback instance = new DummyCallback();
   
   private static final Logger log = Logger.getLogger(SimpleWaitIOCallback.class);
   
   public static IOCallback getInstance()
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
}

