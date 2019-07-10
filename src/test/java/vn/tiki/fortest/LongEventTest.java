package vn.tiki.fortest;

import org.junit.Test;

import static org.junit.Assert.*;

public class LongEventTest {

    @Test
    public void set_Value() {
        LongEvent longEvent = new LongEvent();
        longEvent.set(1L);
    }

    @Test public void test() {
        Integer i1 = Integer.valueOf(1000);
        Integer i2 = Integer.valueOf(1000);
        assertFalse(i1 == i2);
    }


}