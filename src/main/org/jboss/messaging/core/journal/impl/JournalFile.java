/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

import java.util.HashSet;
import java.util.Set;

import org.jboss.messaging.core.journal.SequentialFile;

/**
 * 
 * A JournalFile
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class JournalFile
{
	private final SequentialFile file;
	
	private final long orderingID;
	
	private int refCount;
	
	private int offset;
		
	public JournalFile(final SequentialFile file, final long orderingID)
	{
		this.file = file;
		
		this.orderingID = orderingID;
	}
	
	public void extendOffset(final int delta)
	{
		offset += delta;
	}
	
	public int getOffset()
	{
		return offset;
	}
	
	public long getOrderingID()
	{
		return orderingID;
	}
	
	public void resetOffset()
	{
		offset = 0;
	}
	
	public SequentialFile getFile()
	{
		return file;
	}
	
	public void incRefCount()
	{
		refCount++;
	}
	
	public void decRefCount()
	{
		refCount--;
	}
	
	public boolean isEmpty()
	{
		return refCount == 0;
	}
	
}
