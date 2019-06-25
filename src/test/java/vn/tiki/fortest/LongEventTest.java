package vn.tiki.fortest;

import org.junit.Test;

import static org.junit.Assert.*;

public class LongEventTest {

    @Test
    public void set_Value() {
        LongEvent longEvent = new LongEvent();
        longEvent.set(1L);
    }
}