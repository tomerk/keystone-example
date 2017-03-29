package net.greypanther.javaadvent.regex.factories;

import com.basistech.tclre.*;

import net.greypanther.javaadvent.regex.Regex;

import java.util.ArrayList;
import java.util.Iterator;

public final class ComBasistechTclRegexFactory extends RegexFactory {

    @Override
    public Regex create(String pattern) {
        final RePattern r;
        try {
            r = HsrePattern.compile(pattern, PatternFlags.ADVANCED);
        } catch (RegexException e) {
            throw new IllegalArgumentException(e);
        }
        return new Regex() {
            @Override
            public boolean containsMatch(String string) {
                return r.matcher(string).matches();
            }

            @Override
            public Iterator<String[]> getMatches(String string, int[] groups) {
                int numGroups = groups.length;
                ReMatcher matcher = r.matcher(string);

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
