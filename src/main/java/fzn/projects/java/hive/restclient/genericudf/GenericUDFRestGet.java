package fzn.projects.java.hive.restclient.genericudf;

import fzn.projects.java.hive.restclient.RestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.io.IOException;

@Description(value = "_FUNC_(url) - Returns response of the request to url.", extended = "Example:\n > SELECT _FUNC_('http://localhost');\n <html><body><h1>It works!</h1></body></html>", name = GenericUDFRestGet.FUNC_NAME)
public class GenericUDFRestGet extends GenericUDF {
    private static final Log LOG = LogFactory.getLog(GenericUDFRestGet.class);
    static final String FUNC_NAME = "rest_get";

    private transient StringObjectInspector urlInspector;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length > 1) {
            throw new UDFArgumentLengthException("Number of arguments exceeds.");
        }
        if (!(arguments[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "Only string type is accepted.");
        }
        urlInspector = (StringObjectInspector) arguments[0];
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String url = urlInspector.getPrimitiveJavaObject(arguments[0].get());
        if (url == null) {
            return null;
        }
        Text content = null;
        try {
            content = new Text(RestUtils.restGet(url));
        } catch (IOException e) {
            LOG.error(e);
        }
        return content;
    }

    public String getDisplayString(String[] children) {
        return FUNC_NAME + "(" + children[0] + ")";
    }
}
