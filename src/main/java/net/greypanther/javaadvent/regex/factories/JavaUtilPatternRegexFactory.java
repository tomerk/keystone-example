package net.greypanther.javaadvent.regex.factories;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.greypanther.javaadvent.regex.Regex;

public final class JavaUtilPatternRegexFactory extends RegexFactory {
    @Override
    public Regex create(String pattern) {
        final Pattern rx = Pattern.compile(pattern);

        return new Regex() {
            @Override
            public boolean containsMatch(String string) {
                return rx.matcher(string).find();
            }

            @Override
            public Iterator<String[]> getMatches(String string, int[] groups) {
                int numGroups = groups.length;
                Matcher matcher = rx.matcher(string);
                return new Iterator<String[]>() {
                    private boolean hasNextBool = matcher.find();

                    @Override
                    public boolean hasNext() {
                        return hasNextBool;
                    }

                    @Override
                    public String[] next() {
                        String[] matchArray = new String[numGroups];
                        for (int i = 0; i < numGroups; i++) {
                            matchArray[i] = matcher.group(groups[i]);
                        }
                        hasNextBool = matcher.find();
                        return matchArray;
                    }
                };

            }
        };
    }
}
