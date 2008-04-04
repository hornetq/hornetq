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
package org.jboss.messaging.core.journal.impl.test.unit;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.impl.JournalFile;
import org.jboss.messaging.core.journal.impl.Reclaimer;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.test.unit.UnitTestCase;

/**
 * 
 * A ReclaimerTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ReclaimerTest extends UnitTestCase
{
	private static final Logger log = Logger.getLogger(ReclaimerTest.class);	
	
	private JournalFile[] files;
	
	private Reclaimer reclaimer;
	
	protected void setUp() throws Exception
	{
		super.setUp();
		
		reclaimer = new Reclaimer();
	}
		
	public void testOneFilePosNegAll() throws Exception
	{
		setup(1);
		
		setupPosNeg(0, 10, 10);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);		
	}
	
	public void testOneFilePosNegNotAll() throws Exception
	{
		setup(1);
		
		setupPosNeg(0, 10, 7);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);		
	}
	
	public void testOneFilePosOnly() throws Exception
	{
		setup(1);
		
		setupPosNeg(0, 10);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);		
	}
	
	public void testOneFileNegOnly() throws Exception
	{
		setup(1);
		
		setupPosNeg(0, 0, 10);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);		
	}
	
	
	public void testTwoFilesPosNegAllDifferentFiles() throws Exception
	{
		setup(2);
		
		setupPosNeg(0, 10);
		setupPosNeg(1, 0, 10);
	
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);
		
	}
	
	public void testTwoFilesPosNegAllSameFiles() throws Exception
	{
		setup(2);
		
		setupPosNeg(0, 10, 10);
		setupPosNeg(1, 10, 0, 10);
	
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);
		
	}
	
	public void testTwoFilesPosNegMixedFiles() throws Exception
	{
		setup(2);
		
		setupPosNeg(0, 10, 7);
		setupPosNeg(1, 10, 3, 10);
	
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);		
	}
	
	public void testTwoFilesPosNegAllFirstFile() throws Exception
	{
		setup(2);
		
		setupPosNeg(0, 10, 10);
		setupPosNeg(1, 10);
	
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCantDelete(1);		
	}
	
	public void testTwoFilesPosNegAllSecondFile() throws Exception
	{
		setup(2);
		
		setupPosNeg(0, 10);
		setupPosNeg(1, 10, 0, 10);
	
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCanDelete(1);		
	}
	
	public void testTwoFilesPosOnly() throws Exception
	{
		setup(2);
		
		setupPosNeg(0, 10);
		setupPosNeg(1, 10);
	
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCantDelete(1);		
	}
	
	public void testTwoFilesxyz() throws Exception
	{
		setup(2);
		
		setupPosNeg(0, 10);
		setupPosNeg(1, 10, 10);
	
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCantDelete(1);		
	}
	
	//Can-can-can
	
	public void testThreeFiles1() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 10, 0, 0);
		setupPosNeg(1, 10, 0, 10, 0);
		setupPosNeg(2, 10, 0, 0, 10);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);
		assertCanDelete(2);
	}
	
	public void testThreeFiles2() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 7, 0, 0);
		setupPosNeg(1, 10, 3, 5, 0);
		setupPosNeg(2, 10, 0, 5, 10);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);
		assertCanDelete(2);
	}
	
	public void testThreeFiles3() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 1, 0, 0);
		setupPosNeg(1, 10, 6, 5, 0);
		setupPosNeg(2, 10, 3, 5, 10);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);
		assertCanDelete(2);
	}
	
	public void testThreeFiles3_1() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 1, 0, 0);
		setupPosNeg(1, 10, 6, 5, 0);
		setupPosNeg(2, 0, 3, 5, 0);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);
		assertCanDelete(2);
	}
	
	public void testThreeFiles3_2() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 1, 0, 0);
		setupPosNeg(1, 0, 6, 0, 0);
		setupPosNeg(2, 0, 3, 0, 0);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);
		assertCanDelete(2);
	}
	
	
	
	
	//Cant-can-can
	
	
	public void testThreeFiles4() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 10, 0, 5, 0);
		setupPosNeg(2, 10, 0, 5, 10);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCanDelete(1);
		assertCanDelete(2);
	}
	
	public void testThreeFiles5() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 10, 0, 5, 0);
		setupPosNeg(2, 0, 0, 5, 0);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCanDelete(1);
		assertCanDelete(2);
	}
	
	public void testThreeFiles6() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 0, 0, 0);
		setupPosNeg(1, 10, 0, 5, 0);
		setupPosNeg(2, 0, 0, 5, 10);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCanDelete(1);
		assertCanDelete(2);
	}
	
	public void testThreeFiles7() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 0, 0, 0);
		setupPosNeg(1, 10, 0, 5, 0);
		setupPosNeg(2, 0, 0, 5, 0);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCanDelete(1);
		assertCanDelete(2);
	}
	
	
	//Cant can cant
	
	public void testThreeFiles8() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 10, 0, 10, 0);
		setupPosNeg(2, 10, 0, 0, 2);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCanDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles9() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 10, 0, 10, 0);
		setupPosNeg(2, 10, 1, 0, 2);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCanDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles10() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 10, 0, 10, 0);
		setupPosNeg(2, 10, 1, 0, 0);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCanDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles11() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 0, 0, 0);
		setupPosNeg(1, 10, 0, 10, 0);
		setupPosNeg(2, 10, 0, 0, 0);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCanDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles12() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 0, 0, 0);
		setupPosNeg(1, 10, 0, 10, 0);
		setupPosNeg(2, 0, 3, 0, 0);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCanDelete(1);
		assertCantDelete(2);
	}
	
	//Cant-cant-cant
	
	public void testThreeFiles13() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 10, 2, 3, 0);
		setupPosNeg(2, 10, 1, 5, 7);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles14() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 0, 2, 0, 0);
		setupPosNeg(2, 10, 1, 0, 7);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles15() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 10, 2, 3, 0);
		setupPosNeg(2, 0, 1, 5, 0);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles16() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 0, 2, 0, 0);
		setupPosNeg(2, 0, 1, 0, 0);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles17() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 10, 0, 3, 0);
		setupPosNeg(2, 10, 1, 5, 7);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles18() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 10, 0, 3, 0);
		setupPosNeg(2, 10, 1, 0, 7);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles19() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 10, 0, 3, 0);
		setupPosNeg(2, 10, 1, 0, 0);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles20() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 3, 0, 0);
		setupPosNeg(1, 10, 0, 0, 0);
		setupPosNeg(2, 10, 1, 0, 0);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles21() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 0, 0, 0);
		setupPosNeg(1, 10, 0, 0, 0);
		setupPosNeg(2, 10, 0, 0, 0);
		
		reclaimer.scan(files);
		
		assertCantDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	// Can-can-cant
	
	public void testThreeFiles22() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 10, 0, 0);
		setupPosNeg(1, 10, 0, 10, 0);
		setupPosNeg(2, 10, 0, 0, 0);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles23() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 10, 0, 0);
		setupPosNeg(1, 10, 0, 10, 0);
		setupPosNeg(2, 10, 3, 0, 0);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles24() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 7, 0, 0);
		setupPosNeg(1, 10, 3, 10, 0);
		setupPosNeg(2, 10, 3, 0, 0);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles25() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 7, 0, 0);
		setupPosNeg(1, 0, 3, 10, 0);
		setupPosNeg(2, 10, 3, 0, 0);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles26() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 7, 0, 0);
		setupPosNeg(1, 0, 3, 10, 0);
		setupPosNeg(2, 10, 0, 0, 0);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCanDelete(1);
		assertCantDelete(2);
	}
	

	//Can-cant-cant
	
	public void testThreeFiles27() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 10, 0, 0);
		setupPosNeg(1, 10, 0, 0, 0);
		setupPosNeg(2, 10, 0, 0, 0);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles28() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 10, 0, 0);
		setupPosNeg(1, 10, 0, 3, 0);
		setupPosNeg(2, 10, 0, 0, 5);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles29() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 10, 0, 0);
		setupPosNeg(1, 10, 0, 3, 0);
		setupPosNeg(2, 10, 0, 6, 5);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	public void testThreeFiles30() throws Exception
	{
		setup(3);
		
		setupPosNeg(0, 10, 10, 0, 0);
		setupPosNeg(1, 10, 0, 3, 0);
		setupPosNeg(2, 0, 0, 6, 0);
		
		reclaimer.scan(files);
		
		assertCanDelete(0);
		assertCantDelete(1);
		assertCantDelete(2);
	}
	
	
	// Private ------------------------------------------------------------------------
		
	private void setup(int numFiles)
	{
		files = new JournalFile[numFiles];
		
		for (int i = 0; i < numFiles; i++)
		{
			files[i] = new MockJournalFile();
		}		                       
	}
		
	private void setupPosNeg(int fileNumber, int pos, int... neg)
	{
		JournalFile file = files[fileNumber];
		
		for (int i = 0; i < pos; i++)
		{
			file.incPosCount();
		}
		
		for (int i = 0; i < neg.length; i++)
		{
			JournalFile reclaimable2 = files[i];
			
			for (int j = 0; j < neg[i]; j++)
			{
				file.incNegCount(reclaimable2);
			}
		}
	}
	
	private void assertCanDelete(int... fileNumber)
	{
		for (int num: fileNumber)
		{
			assertTrue(files[num].isCanReclaim());
		}
	}
	
	private void assertCantDelete(int... fileNumber)
	{
		for (int num: fileNumber)
		{
			assertFalse(files[num].isCanReclaim());
		}
	}
	
	class MockJournalFile implements JournalFile
	{
	   private Set<Long> transactionIDs = new HashSet<Long>();
		
		private Set<Long> transactionTerminationIDs = new HashSet<Long>();
		
		private Set<Long> transactionPrepareIDs = new HashSet<Long>();

		private Map<JournalFile, Integer> negCounts = new HashMap<JournalFile, Integer>();
		
		private int posCount;
		
		private boolean canDelete;
		
		public void extendOffset(int delta)
		{
		}

		public SequentialFile getFile()
		{
			return null;
		}

		public int getOffset()
		{
			return 0;
		}

		public long getOrderingID()
		{
			return 0;
		}

		public void setOffset(int offset)
		{
		}
		
		public int getNegCount(final JournalFile file)
		{
			Integer count = negCounts.get(file);
			
			if (count != null)
			{
				return count.intValue();
			}
			else
			{
				return 0;
			}
		}
		
		public void incNegCount(final JournalFile file)
		{
			Integer count = negCounts.get(file);
			
			int c = count == null ? 1 : count.intValue() + 1;
			
			negCounts.put(file, c);
		}
		
		public int getPosCount()
		{
			return posCount;
		}
		
		public void incPosCount()
		{
			this.posCount++;
		}
		
		public void decPosCount()
		{
			this.posCount--;
		}

		public boolean isCanReclaim()
		{
			return canDelete;
		}

		public void setCanReclaim(boolean canDelete)
		{
			this.canDelete = canDelete;
		}		
		
		public void addTransactionID(long id)
		{
			transactionIDs.add(id);
		}

		public void addTransactionPrepareID(long id)
		{
			transactionPrepareIDs.add(id);
		}

		public void addTransactionTerminationID(long id)
		{
			transactionTerminationIDs.add(id);
		}

		public boolean containsTransactionID(long id)
		{
			return transactionIDs.contains(id);
		}

		public boolean containsTransactionPrepareID(long id)
		{
			return transactionPrepareIDs.contains(id);
		}

		public boolean containsTransactionTerminationID(long id)
		{
			return transactionTerminationIDs.contains(id);
		}
		
		public Set<Long> getTranactionTerminationIDs()
		{
			return transactionTerminationIDs;
		}

		public Set<Long> getTransactionPrepareIDs()
		{
			return transactionPrepareIDs;
		}

		public Set<Long> getTransactionsIDs()
		{
			return transactionIDs;
		}
	}
}
