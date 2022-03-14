package org.mariem.kafka.elasticsearchConsumer;
 

public class ElasticsearchConsumer {
/*    public static RestHighLevelClient createClient() {
        String hostname = "";
        String username = "";
        String password = "";

        //Don't do on local elasticsearch
        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("user", "test-user-password"));

        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("localhost", 9200))
                .setHttpClientConfigCallback(new HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
    }*/

    public static void main(String[] args) {

    }
}
