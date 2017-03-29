package net.greypanther.javaadvent.regex;

import java.util.Iterator;

public interface Regex {
    boolean containsMatch(String string);
    Iterator<String[]> getMatches(String string, int[] groups);
}
