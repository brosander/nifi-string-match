package com.github.brosander.nifi.string.match;

import me.xdrop.fuzzywuzzy.FuzzySearch;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"String", "Fuzzy", "Match"})
@CapabilityDescription("Runs a fuzzy match against an attribute of the flowfile utilizing https://github.com/xdrop/fuzzywuzzy.")
public class FuzzyMatch extends AbstractProcessor {
    public static final String MATCH_TYPE_RATIO = "ratio";

    private static final Map<String, BiFunction<String, String, Integer>> matchTypes = initMatchTypes();

    private static Map<String, BiFunction<String, String, Integer>> initMatchTypes() {
        Map<String, BiFunction<String, String, Integer>> result = new HashMap<>();
        result.put(MATCH_TYPE_RATIO, FuzzySearch::ratio);
        result.put("partialRatio", FuzzySearch::partialRatio);
        result.put("tokenSortPartialRatio", FuzzySearch::tokenSortPartialRatio);
        result.put("tokenSortRatio", FuzzySearch::tokenSortRatio);
        result.put("tokenSetRatio", FuzzySearch::tokenSetRatio);
        result.put("tokenSetPartialRatio", FuzzySearch::tokenSetPartialRatio);
        result.put("weightedRatio", FuzzySearch::weightedRatio);
        return Collections.unmodifiableMap(result);
    }

    public static final PropertyDescriptor MATCH_TYPE = new PropertyDescriptor.Builder()
            .name("Match Type")
            .description("The matching algorithm to apply to the value.")
            .required(true)
            .allowableValues(matchTypes.keySet().stream().sorted().toArray(String[]::new))
            .defaultValue(MATCH_TYPE_RATIO)
            .build();

    public static final PropertyDescriptor MATCH_THRESHOLD = new PropertyDescriptor.Builder()
            .name("Match Threshold")
            .description("The lowest ratio that should be considered a match.")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("80")
            .build();

    public static final PropertyDescriptor MATCH_AGAINST = new PropertyDescriptor.Builder()
            .name("Match Against")
            .description("The string to compare the input to.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MATCH_INPUT = new PropertyDescriptor.Builder()
            .name("Match Input")
            .description("The input string to compare.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles are routed to this relationship when the fuzzy match is above the threshold.")
            .build();

    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles are routed to this relationship when the fuzzy match is below the threshold")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        this.relationships = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(REL_MATCH, REL_NO_MATCH)));

        this.properties = Collections.unmodifiableList(Arrays.asList(MATCH_TYPE, MATCH_THRESHOLD, MATCH_AGAINST, MATCH_INPUT));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String matchTypeKey = context.getProperty(MATCH_TYPE).evaluateAttributeExpressions(flowFile).getValue();

        BiFunction<String, String, Integer> matchFunction = matchTypes.get(matchTypeKey);
        if (matchFunction == null) {
            throw new ProcessException("Unable to find match type function for key " + matchTypeKey);
        }

        String matchAgainst = context.getProperty(MATCH_AGAINST).evaluateAttributeExpressions(flowFile).getValue();
        String matchInput = context.getProperty(MATCH_INPUT).evaluateAttributeExpressions(flowFile).getValue();

        int matchThreshold = context.getProperty(MATCH_THRESHOLD).evaluateAttributeExpressions(flowFile).asInteger();

        int ratio = matchFunction.apply(matchAgainst, matchInput);

        flowFile = session.putAttribute(flowFile, "fuzzy.match.ratio", Integer.toString(ratio));
        session.getProvenanceReporter().modifyAttributes(flowFile);

        if (ratio >= matchThreshold) {
            session.transfer(flowFile, REL_MATCH);
        } else {
            session.transfer(flowFile, REL_NO_MATCH);
        }
    }
}
