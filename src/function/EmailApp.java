package function;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * Created by pramesh on 5/6/17.
 */
public class EmailApp {
    //defining delimiters and file locations
    private static String DELIMITER_SOURCE = ",";
    private static String DELIMITER_SINK = "|";
    private static String FILE_SOURCE = "resources/javas/tutorials/cascading/cascading_intro_file.csv";
    private static String FILE_SINK = "build/tutorials/cascading/function1/cascading_intro_file_sink.csv";
    private static FlowDef flowDef = new FlowDef();

    public static void main(String[] args) {
        //fields that match our data layout
        Fields sourceFields = new Fields("id", "first_name", "last_name", "email", "gender", "salary");

        //source and sink taps definition
        Tap sourceTap = new Hfs(new TextDelimited(sourceFields, true, DELIMITER_SOURCE), FILE_SOURCE);

        //to call while all fields are input and all are output
        //Tap sinkTap = new Hfs(new TextDelimited(sourceFields, false, DELIMITER_SINK), FILE_SINK, SinkMode.REPLACE);

        //to call while custom number of fields are input and output
        Tap sinkTap = new Hfs(new TextDelimited(new Fields("first_name", "email"), false, DELIMITER_SINK), FILE_SINK, SinkMode.REPLACE);

        Pipe pipe = new Pipe("personData");

        //pipe debug before function
        pipe = new Each(pipe, new Debug("beforeFunction", true));

        //function calls

        //to call while all fields are input and all are output
        //pipe = new Each(pipe, new EmailChangeFunction(EmailChangeFunction.Domains.GOOGLE), Fields.RESULTS); //to call while all fields are input and all are output

        //to call while custom number of fields are input and output
        pipe = new Each(pipe, new Fields("first_name", "last_name", "email"), new EmailChangeFunction(EmailChangeFunction.Domains.GOOGLE, ""), Fields.RESULTS);

        //pipe debug after function
        pipe = new Each(pipe, new Debug("afterFunction", true));

        //completing the flow
        flowDef.addSource(pipe, sourceTap);
        flowDef.addTailSink(pipe, sinkTap);

        Flow flow = new HadoopFlowConnector().connect(flowDef);
        flow.complete();
    }

}
