package net.greypanther.javaadvent.regex.factories;

import jregex.Matcher;
import jregex.REFlags;
import net.greypanther.javaadvent.regex.Regex;

import java.util.ArrayList;

public final class JRegexFactory extends RegexFactory {

    @Override
    public Regex create(String pattern) {
        final jregex.Pattern regexpr = new jregex.Pattern(pattern, REFlags.DOTALL);

        return new Regex() {
            @Override
            public boolean containsMatch(String string) {
                return regexpr.matcher(string).matches();
            }

            @Override
            public Iterable<String[]> getMatches(String string, int[] groups) {
                int numGroups = groups.length;
                Matcher matcher = regexpr.matcher(string);
                ArrayList<String[]> matches = new ArrayList<>();
                while (matcher.find()) {
                    String[] matchArray = new String[numGroups];
                    for (int i = 0; i < numGroups; i++) {
                        matchArray[i] = matcher.group(groups[i]);
                    }
                    matches.add(matchArray);
                }

                return matches;
            }
        };
    }

}
