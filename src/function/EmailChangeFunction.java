package function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Created by pgautam on 5/5/17.
 */
public class EmailChangeFunction extends BaseOperation implements Function {
    private Domains toDomain;

    EmailChangeFunction(Domains newDomain) {
        super(Fields.ARGS);
        toDomain = newDomain;
    }

    EmailChangeFunction(Domains newDomain, String s) {
        super(3, new Fields("first_name", "email"));
        toDomain = newDomain;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry entry = functionCall.getArguments();
        TupleEntry outEntry = new TupleEntry(entry);

        //extracting old and new email
        String initialEmail = entry.getString("email");
        String initialDomain = initialEmail.split("\\@")[1].split("\\.")[0];
        String finalEmail = initialEmail.replace(initialDomain, toDomain.getDomain());

        //setting new email
        entry.setString("email", finalEmail);

        functionCall.getOutputCollector().add(entry);

    }

    //defining email domains
    public enum Domains {
        GOOGLE("gmail"),
        MICROSOFT("hotmail"),
        YAHOO("yahoo");

        String domain;

        Domains(String dom) {
            this.domain = dom;
        }

        String getDomain() {
            return this.domain;
        }
    }
}
