package org.mp.naumann.utils;

import org.junit.Test;

import java.io.Serializable;
import java.util.HashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mp.naumann.utils.GenericHelper.*;

public class GenericHelperTest {

    @Test
    public void testCorrectCast(){
        Object s = "demo string";
        assertTrue(cast(s, String.class) != null);
    }

    @Test(expected=ClassCastException.class)
    public void testNullCast(){
        cast(null, Object.class);
    }

    @Test
    public void testInterfaceCast(){
        Object s = "demo string";
        assertTrue(cast(s, Serializable.class) != null);
    }

    @Test
    public void testIntegerToDoubleCast(){
        assertTrue(cast(5, Double.class) != null);
    }

    @Test
    public void testIsNumberCast(){
        assertTrue(isNumberCast(5, Double.class));
        assertFalse(isNumberCast(5, String.class));
        assertTrue(isNumberCast(5.0f, Integer.class));
    }

    @Test
    public void testCreateGenericMap(){
        assertTrue(createGenericMap(HashMap.class, String.class, Integer.class) instanceof HashMap);
    }
}
