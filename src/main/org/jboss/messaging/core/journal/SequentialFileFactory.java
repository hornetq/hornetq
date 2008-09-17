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

package org.jboss.messaging.core.journal;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 
 * A SequentialFileFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface SequentialFileFactory
{
   SequentialFile createSequentialFile(String fileName, int maxIO) throws Exception;

   List<String> listFiles(String extension) throws Exception;

   boolean isSupportsCallbacks();

   ByteBuffer newBuffer(int size);

   // Avoid using this method in production as it creates an unecessary copy
   ByteBuffer wrapBuffer(byte[] bytes);

   int getAlignment();

   int calculateBlockSize(int bytes);

   /** This method is not being used currently. 
    *  The journal will complete the buffer when reusing the buffer.
    *  Look at JournalImpl#newBuffer for more information about this */
   void clearBuffer(ByteBuffer buffer);

}
