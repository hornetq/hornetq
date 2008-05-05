/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.journal.impl;

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;

public class AIOSequentialFileFactory extends AbstractSequentialFactory
{
	
	public AIOSequentialFileFactory(String journalDir)
	{
		super(journalDir);
	}
	
	public SequentialFile createSequentialFile(String fileName, boolean sync) throws Exception
	{
		return new AIOSequentialFile(journalDir, fileName);
	}
	
	public static boolean isSupported()
	{
		return AsynchronousFileImpl.isLoaded();
	}
	
}
