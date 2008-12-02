/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.paging.impl;

import org.jboss.messaging.core.paging.Page;
import org.jboss.messaging.core.paging.PagingStore;

/**
 * All the methods required to TestCases on  PageStoreImpl
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface TestSupportPageStore extends PagingStore
{
   /** 
    * Remove the first page from the Writing Queue.
    * The file will still exist until Page.delete is called, 
    * So, case the system is reloaded the same Page will be loaded back if delete is not called.
    * @return
    * @throws Exception
    * 
    * Note: This should still be part of the interface, even though JBossMessaging only uses through the 
    */
   Page depage() throws Exception;

   void forceAnotherPage() throws Exception;
}
