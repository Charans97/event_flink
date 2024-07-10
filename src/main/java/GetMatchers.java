import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GetMatchers {

    private List<String> fields = new ArrayList<>();
    private List<String> operators = new ArrayList<>();
    private List<String> values = new ArrayList<>();
    private String combinator;

    public void CheckCondition(String jsonString) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(jsonString);

            JsonNode matchersNode = jsonNode.get("matchers");
            if (matchersNode.isArray()) {
                Iterator<JsonNode> matchersIterator = matchersNode.elements();
                while (matchersIterator.hasNext()) {
                    JsonNode matcherNode = matchersIterator.next();
                    String field = matcherNode.get("field").asText();
                    String operator = matcherNode.get("operator").asText();
                    String value = matcherNode.get("value").asText();
                    fields.add(field);
                    operators.add(operator);
                    values.add(value);
                }
            }
            this.combinator = jsonNode.get("combinator").asText();

        } catch (Exception e) {
            System.out.println("\n\t" + e);
        }
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public List<String> getOperators() {
        return operators;
    }

    public void setOperators(List<String> operators) {
        this.operators = operators;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public String getCombinator() {
        return combinator;
    }

    public void setCombinator(String combinator) {
        this.combinator = combinator;
    }
}