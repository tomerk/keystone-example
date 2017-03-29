package net.greypanther.javaadvent.regex.factories;

import org.apache.oro.text.regex.MalformedPatternException;

import net.greypanther.javaadvent.regex.Regex;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.PatternMatcherInput;

import java.util.ArrayList;
import java.util.Iterator;

import static org.apache.oro.text.regex.Perl5Compiler.SINGLELINE_MASK;

public final class OroRegexFactory extends RegexFactory {

    @Override
    public Regex create(String pattern) {
        org.apache.oro.text.regex.Perl5Compiler perl5Compiler = new org.apache.oro.text.regex.Perl5Compiler();
        final org.apache.oro.text.regex.Perl5Matcher perl5Matcher = new org.apache.oro.text.regex.Perl5Matcher();
        final org.apache.oro.text.regex.Pattern regexpr;
        try {
            regexpr = perl5Compiler.compile(pattern, SINGLELINE_MASK);
        } catch (MalformedPatternException e) {
            throw new IllegalArgumentException(e);
        }
        
        return new Regex() {
            @Override
            public boolean containsMatch(String string) {
                return perl5Matcher.matches(string, regexpr);
            }

            @Override
            public Iterator<String[]> getMatches(String string, int[] groups) {
                int numGroups = groups.length;
                PatternMatcherInput input = new PatternMatcherInput(string);
                return new Iterator<String[]>() {
                    private boolean hasNextBool = perl5Matcher.contains(input, regexpr);

                    @Override
                    public boolean hasNext() {
                        return hasNextBool;
                    }

                    @Override
                    public String[] next() {
                        MatchResult result = perl5Matcher.getMatch();

                        String[] matchArray = new String[numGroups];
                        for (int i = 0; i < numGroups; i++) {
                            matchArray[i] = result.group(groups[i]);
                        }
                        hasNextBool = perl5Matcher.contains(input, regexpr);
                        return matchArray;
                    }
                };

            }
        };
    }

}
