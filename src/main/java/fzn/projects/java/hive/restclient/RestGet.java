package fzn.projects.java.hive.restclient;

import jodd.io.StringOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;

@Description(value = "_FUNC_(url) - Returns response of the request to url.", extended = "Example:\n > SELECT _FUNC_('http://localhost');\n <html><body><h1>It works!</h1></body></html>", name = RestGet.FUNC_NAME)
public class RestGet extends GenericUDF {
    private static final Log LOG = LogFactory.getLog(RestGet.class);
    static final String FUNC_NAME = "rest_get";

    private StringObjectInspector urlInspector;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length > 1) {
            throw new UDFArgumentLengthException("Number of arguments exceeds.");
        }
        if (!(arguments[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "Only string type is accepted.");
        }
        urlInspector = (StringObjectInspector) arguments[0];
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String url = urlInspector.getPrimitiveJavaObject(arguments[0].get());
        if (url == null) {
            return null;
        }
        HttpGet httpGet = new HttpGet(url);
        HttpClient httpClient = new DefaultHttpClient();
        Text content = null;
        try {
            HttpResponse httpResponse = httpClient.execute(httpGet);
            try (StringOutputStream contentStream = new StringOutputStream()) {
                HttpEntity responseEntity = httpResponse.getEntity();
                responseEntity.writeTo(contentStream);
                content = new Text(contentStream.toString());
            } catch (IOException e) {
                LOG.error(e);
            } finally {
                HttpClientUtils.closeQuietly(httpResponse);
            }
        } catch (IOException e) {
            LOG.error(e);
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
        return content;
    }

    public String getDisplayString(String[] children) {
        return FUNC_NAME + "(" + children[0] + ")";
    }
}
