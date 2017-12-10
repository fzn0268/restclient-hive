package fzn.projects.java.hive.restclient;

import jodd.io.StringOutputStream;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;

public class RestUtils {
    public static String restGet(String url) throws IOException {
        HttpGet httpGet = new HttpGet(url);
        HttpClient httpClient = new DefaultHttpClient();
        String content;
        try {
            HttpResponse httpResponse = httpClient.execute(httpGet);
            try (StringOutputStream contentStream = new StringOutputStream()) {
                HttpEntity responseEntity = httpResponse.getEntity();
                responseEntity.writeTo(contentStream);
                content = contentStream.toString();
            } finally {
                HttpClientUtils.closeQuietly(httpResponse);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
        return content;
    }
}
