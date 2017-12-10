package fzn.projects.java.hive.restclient.udf;

import fzn.projects.java.hive.restclient.RestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.IOException;

@Description(value = "_FUNC_(url) - Returns response of the request to url.", extended = "Example:\n > SELECT _FUNC_('http://localhost');\n <html><body><h1>It works!</h1></body></html>", name = UDFRestGet.FUNC_NAME)
public class UDFRestGet extends UDF {
    private static final Log LOG = LogFactory.getLog(UDFRestGet.class);
    static final String FUNC_NAME = "rest_get";

    public Text evaluate(Text url) {
        if (url == null) {
            return null;
        }
        String content = null;
        try {
            content = RestUtils.restGet(url.toString());
        } catch (IOException e) {
            LOG.error(e);
        }
        return new Text(content);
    }
}
