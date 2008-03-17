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
package org.jboss.messaging.core.journal;

import java.util.List;

import org.jboss.messaging.core.server.MessagingComponent;

/**
 * 
 * A Journal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface Journal extends MessagingComponent
{
	// Non transactional operations
	
	void appendAddRecord(long id, byte[] record) throws Exception;
	
	void appendUpdateRecord(long id, byte[] record) throws Exception;
	
	void appendDeleteRecord(long id) throws Exception;
	
	// Transactional operations
	
	void appendAddRecordTransactional(long txID, long id, byte[] record, boolean done) throws Exception;
	
	void appendUpdateRecordTransactional(long txID, long id, byte[] record, boolean done) throws Exception;
	
	void appendDeleteRecordTransactional(long txID, long id, boolean done) throws Exception;
	
	//XA operations
	
	void appendAddRecordPrepare(long txID, long id, byte[] record, boolean done) throws Exception;
	
	void appendUpdateRecordPrepare(long txID, long id, byte[] record, boolean done) throws Exception;
	
	void appendDeleteRecordPrepare(long txID, long id, boolean done) throws Exception;
	
	void appendXACommitRecord(long txID) throws Exception;
	
	void appendXARollbackRecord(long txID) throws Exception;

	
	// Load
	
	void load(List<RecordInfo> committedRecords,
			    List<PreparedTransactionInfo> preparedTransactions) throws Exception;
	
}
