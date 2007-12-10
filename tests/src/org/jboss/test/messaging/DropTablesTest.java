package org.jboss.test.messaging;

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
public class DropTablesTest extends JBMServerTestCase
{

	public DropTablesTest(String name)
	{
		super(name);
	}
	
	public void testDropTables() throws Exception
	{
	
			dropTables();

	}

}
