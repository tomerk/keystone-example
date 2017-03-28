package net.greypanther.javaadvent.regex.factories;

import net.greypanther.javaadvent.regex.Regex;

public final class ComKarneimRegexFactory extends RegexFactory {

    @Override
    public Regex create(String pattern) {
        final com.karneim.util.collection.regex.Pattern p = new com.karneim.util.collection.regex.Pattern(pattern);
        return new Regex() {

            @Override
            public boolean containsMatch(String string) {
                return p.contains(string);
            }

            @Override
            public Iterable<String[]> getMatches(String string, int[] groups) {
                throw new UnsupportedOperationException();
            }
        };
    }

}
