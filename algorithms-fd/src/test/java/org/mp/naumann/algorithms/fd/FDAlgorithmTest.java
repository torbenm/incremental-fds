package org.mp.naumann.algorithms.fd;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mp.naumann.algorithms.exceptions.AlgorithmConfigurationException;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.exceptions.CouldNotReceiveResultException;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmFixture;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture1;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture10;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture12;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture15;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture17;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture4;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture5;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture6;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture7;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture8;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture9;
import org.mp.naumann.algorithms.fd.fixtures.BridgesFixture;

import org.mp.naumann.algorithms.fd.fixtures.AbaloneFixture;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture11;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture13;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture14;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture16;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture2;
import org.mp.naumann.algorithms.fd.fixtures.AlgorithmTestFixture3;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.InputReadException;

public abstract class FDAlgorithmTest {

	protected FunctionalDependencyAlgorithm algo;
	
	@Before
	public void setUp(){
        algo = getNewInstance();
    }

    protected abstract FunctionalDependencyAlgorithm getNewInstance();

    @After
	public void tearDown(){

    }
	
	protected void executeAndVerifyWithFixture(AlgorithmFixture fixture) throws ConnectionException, InputReadException, AlgorithmExecutionException {
        algo.configure(fixture.getInputGenerator(), fixture.getFunctionalDependencyResultReceiver());
        algo.execute();
        fixture.verifyFunctionalDependencyResultReceiver();
    }
    @Test 
    public void testExecute1() throws AlgorithmExecutionException, ConnectionException, InputReadException, CouldNotReceiveResultException {
        AlgorithmTestFixture1 fixture = new AlgorithmTestFixture1();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute2() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture2 fixture = new AlgorithmTestFixture2();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute3() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture3 fixture = new AlgorithmTestFixture3();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute4() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture4 fixture = new AlgorithmTestFixture4();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute5() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture5 fixture = new AlgorithmTestFixture5();
        executeAndVerifyWithFixture(fixture);
    }

    @Test 
    public void testExecute6() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture6 fixture = new AlgorithmTestFixture6();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute7() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture7 fixture = new AlgorithmTestFixture7();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute8() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture8 fixture = new AlgorithmTestFixture8();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute9() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture9 fixture = new AlgorithmTestFixture9();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute10() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture10 fixture = new AlgorithmTestFixture10();
        executeAndVerifyWithFixture(fixture);
    }

    @Test 
    public void testExecute11() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture11 fixture = new AlgorithmTestFixture11();
        executeAndVerifyWithFixture(fixture);
    }

    @Test 
    public void testExecute12() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture12 fixture = new AlgorithmTestFixture12();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute13() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture13 fixture = new AlgorithmTestFixture13();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute14() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture14 fixture = new AlgorithmTestFixture14();
        executeAndVerifyWithFixture(fixture);
    }

    @Test 
    public void testExecute15() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture15 fixture = new AlgorithmTestFixture15();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute16() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture16 fixture = new AlgorithmTestFixture16();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute17() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture17 fixture = new AlgorithmTestFixture17();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute18() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture14 fixture = new AlgorithmTestFixture14();
        executeAndVerifyWithFixture(fixture);
    }

    @Test 
    public void testExecute19() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture15 fixture = new AlgorithmTestFixture15();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test 
    public void testExecute20() throws AlgorithmExecutionException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture16 fixture = new AlgorithmTestFixture16();
        executeAndVerifyWithFixture(fixture);
    }
    
    @Test
    public void testAbaloneFixture() throws AlgorithmExecutionException, UnsupportedEncodingException, FileNotFoundException, AlgorithmConfigurationException, ConnectionException, InputReadException {
        AbaloneFixture fixture = new AbaloneFixture();
        executeAndVerifyWithFixture(fixture);
    }

	@Test
    public void testBridgesFixture() throws AlgorithmExecutionException, UnsupportedEncodingException, FileNotFoundException, AlgorithmConfigurationException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        BridgesFixture fixture = new BridgesFixture();
        executeAndVerifyWithFixture(fixture);
    }

	@Test
    public void testAlgorithmFixture() throws AlgorithmExecutionException, AlgorithmConfigurationException, CouldNotReceiveResultException, ConnectionException, InputReadException {
        AlgorithmTestFixture fixture = new AlgorithmTestFixture();
        executeAndVerifyWithFixture(fixture);
    }

}
