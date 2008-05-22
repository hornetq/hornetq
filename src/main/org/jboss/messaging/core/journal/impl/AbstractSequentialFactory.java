/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.journal.impl;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.jboss.messaging.core.journal.SequentialFileFactory;

/**
 * 
 * An abstract SequentialFileFactory containing basic functionality for both AIO and NIO SequentialFactories
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public abstract class AbstractSequentialFactory implements SequentialFileFactory
{
	protected final String journalDir;
	
	public AbstractSequentialFactory(final String journalDir)
	{
		this.journalDir = journalDir;
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
		
		if (fileNames == null)
		{
			throw new IOException("Failed to list: " + journalDir);
		}
		
		return Arrays.asList(fileNames);
	}
	
}
