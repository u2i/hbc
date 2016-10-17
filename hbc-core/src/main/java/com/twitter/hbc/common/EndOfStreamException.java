package com.twitter.hbc.common;

import java.io.IOException;

public class EndOfStreamException extends IOException {
    public EndOfStreamException(String s) {
       super(s);
    }
}
