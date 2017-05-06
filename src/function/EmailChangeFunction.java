package function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Created by pgautam on 5/5/17.
 */
public class EmailChangeFunction extends BaseOperation implements Function {
    private Domains toDomain;

    public EmailChangeFunction(Domains newDomain) {
        super(Fields.ARGS);
        toDomain = newDomain;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        System.out.println("prepare");
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry entry = functionCall.getArguments();
        TupleEntry outEntry = new TupleEntry(entry);

        String initialEmail = entry.getString("email");
        String initialDomain = initialEmail.split("\\@")[1].split("\\.")[0];
        String finalEmail = initialEmail.replace(initialDomain, toDomain.getDomain());

        System.out.println("initialDomain = " + initialDomain);

        System.out.println("finalEmail = " + finalEmail);


        outEntry.setString("email", finalEmail);

        functionCall.getOutputCollector().add(outEntry);

    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
        System.out.println("cleanup");
    }

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
