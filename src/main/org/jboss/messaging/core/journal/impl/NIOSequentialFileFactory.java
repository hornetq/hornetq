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

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.List;

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;

/**
 * 
 * A NIOSequentialFileFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NIOSequentialFileFactory implements SequentialFileFactory
{
	private final String journalDir;
	
	public NIOSequentialFileFactory(final String journalDir)
	{
		this.journalDir = journalDir;
	}	
	
	public SequentialFile createSequentialFile(final String fileName, final boolean sync)
	{
		return new NIOSequentialFile(journalDir, fileName, sync);
	}

	public List<String> listFiles(final String extension) throws Exception
	{
		File dir = new File(journalDir);
		
		FilenameFilter fnf = new FilenameFilter()
		{
			public boolean accept(File file, String name)
			{
				return name.endsWith("." + extension);
			}
		};
		
		String[] fileNames = dir.list(fnf);
		
		return Arrays.asList(fileNames);
	}
}
