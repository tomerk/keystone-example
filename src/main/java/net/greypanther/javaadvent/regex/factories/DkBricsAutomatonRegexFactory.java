package net.greypanther.javaadvent.regex.factories;

import dk.brics.automaton.Automaton;

import dk.brics.automaton.AutomatonMatcher;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;
import net.greypanther.javaadvent.regex.Regex;

import java.util.ArrayList;
import java.util.Iterator;

public final class DkBricsAutomatonRegexFactory extends RegexFactory {

    @Override
    public Regex create(String pattern) {
        RegExp regexpr = new RegExp(pattern);
        Automaton auto = regexpr.toAutomaton();
        final RunAutomaton runauto = new RunAutomaton(auto, true);

        return new Regex() {
            @Override
            public boolean containsMatch(String string) {
                return runauto.run(string);
            }

            @Override
            public Iterator<String[]> getMatches(String string, int[] groups) {
                int numGroups = groups.length;
                AutomatonMatcher matcher = runauto.newMatcher(string);
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
