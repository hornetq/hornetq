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
package org.jboss.messaging.tests.unit.core.journal.impl.timing;

import org.jboss.messaging.tests.unit.core.journal.impl.JournalImplTestBase;
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A RealJournalImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public abstract class JournalImplTestUnit extends JournalImplTestBase
{
	private static final Logger log = Logger.getLogger(JournalImplTestUnit.class);
	
	
	
	public void testAddUpdateDeleteManyLargeFileSize() throws Exception
	{
		final int numberAdds = 10000;
		
		final int numberUpdates = 5000;
		
		final int numberDeletes = 3000;
						
		long[] adds = new long[numberAdds];
		
		for (int i = 0; i < numberAdds; i++)
		{
			adds[i] = i;
		}
		
		long[] updates = new long[numberUpdates];
		
		for (int i = 0; i < numberUpdates; i++)
		{
			updates[i] = i;
		}
		
		long[] deletes = new long[numberDeletes];
		
		for (int i = 0; i < numberDeletes; i++)
		{
			deletes[i] = i;
		}
		
		setup(10, 10 * 1024 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(adds);
		update(updates);
		delete(deletes);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		
	}
	
	public void testAddUpdateDeleteManySmallFileSize() throws Exception
	{
		final int numberAdds = 10000;
		
		final int numberUpdates = 5000;
		
		final int numberDeletes = 3000;
						
		long[] adds = new long[numberAdds];
		
		for (int i = 0; i < numberAdds; i++)
		{
			adds[i] = i;
		}
		
		long[] updates = new long[numberUpdates];
		
		for (int i = 0; i < numberUpdates; i++)
		{
			updates[i] = i;
		}
		
		long[] deletes = new long[numberDeletes];
		
		for (int i = 0; i < numberDeletes; i++)
		{
			deletes[i] = i;
		}
		
		setup(10, 10 * 1024, true);
		createJournal();
		startJournal();
		load();
		add(adds);
		update(updates);
		delete(deletes);
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		
	}
	
	public void testReclaimAndReload() throws Exception
	{
		setup(5, 10 * 1024 * 1024, true);
		createJournal();
		startJournal();
		load();
		
		journal.startReclaimer();
		
		long start = System.currentTimeMillis();
						
		for (int count = 0; count < 100000; count++)
		{
			add(count);
			
			if (count >= 5000)
			{
				delete(count - 5000);
			}
			
			if (count % 10000 == 0)
			{
				log.info("Done: " + count);
			}
		}
		
		long end = System.currentTimeMillis();
		
		double rate = 1000 * ((double)100000) / (end - start);
		
		log.info("Rate of " + rate + " adds/removes per sec");
					
		stopJournal();
		createJournal();
		startJournal();
		loadAndCheck();
		
		assertEquals(5000, journal.getIDMapSize());
		
		stopJournal();
	}
	
}


