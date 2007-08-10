package org.jboss.test.messaging;

import org.jboss.test.messaging.tools.container.ServiceContainer;

/**
 * 
 * This test only exists so we have a way of dropping the tables on Hudson for QA runs
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>10 Aug 2007
 *
 * $Id: $
 *
 */
public class DropTablesTest extends MessagingTestCase
{

	public DropTablesTest(String name)
	{
		super(name);
	}
	
	public void testDropTables() throws Exception
	{
		ServiceContainer sc = null;
		
		try
		{		
			sc = new ServiceContainer("all");
			
			sc.start();
			
			sc.dropTables();
		}
		finally
		{			
			if (sc != null)
			{
				sc.stop();
			}
		}
	}

}
