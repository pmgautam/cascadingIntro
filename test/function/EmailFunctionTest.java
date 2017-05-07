package function;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.LocalPlatform;
import cascading.test.PlatformRunner;
import cascading.tuple.Fields;
import org.junit.Test;

/**
 * Created by pramesh on 5/7/17.
 */
@PlatformRunner.Platform(LocalPlatform.class)
public class EmailFunctionTest extends PlatformTestCase {
    String inputFile = "resources/javas/tutorials/cascading/cascading_intro_file.csv";
    String outputFile = "build/javas/tutorials/cascading/function/email_function_output.csv";

    Fields sourceFields = new Fields("id", "first_name", "last_name", "email", "gender", "salary");
    Fields sinkFields = new Fields("first_name", "email");


    @Test
    public void emailTest(){
        Pipe inputPipe = new Pipe("inputPipe");
        Tap sourceTap = getPlatform().getDelimitedFile(sourceFields, true, ",", null, null, inputFile, SinkMode.REPLACE);
        Tap sinkTap = getPlatform().getDelimitedFile(sinkFields, ",", outputFile, SinkMode.REPLACE);

        inputPipe = new Each(inputPipe, new Debug("pipeData", true));

        inputPipe = new Each(inputPipe, new Fields("first_name", "last_name", "email"), new EmailChangeFunction(EmailChangeFunction.Domains.GOOGLE, ""), Fields.RESULTS);

        FlowDef flowDef = new FlowDef();
        flowDef.addSource(inputPipe, sourceTap);
        flowDef.addTailSink(inputPipe, sinkTap);

        Flow flow = getPlatform().getFlowConnector().connect(flowDef);
        flow.complete();
    }
}
