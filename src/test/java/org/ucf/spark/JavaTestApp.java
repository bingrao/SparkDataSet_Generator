package org.ucf.spark;

/**
 * @author 
 */
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class JavaTestApp extends TestCase{
	  /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public JavaTestApp( String testName )
    {
        super( testName );
    }
    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( JavaTestApp.class );
    }
    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
    	System.out.println("Hello World From Java");
        assertTrue( true );
    }
}