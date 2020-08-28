package com.kuang.config;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetAddress;

//import org.elasticsearch.common.transport.InetSocketTransportAddress;

@Configuration
@EnableElasticsearchRepositories(basePackages = "com.kuang")
public class ElasticConfigration {
	
	@Value("${elasticsearch.host}")
    private String esHost;

//    @Value("${elasticsearch.port}")
//    private int esPort;

    @Value("${elasticsearch.clusterName}")
    private String esClusterName;
    
    private TransportClient client;
    
//    @PostConstruct
//    public void initialize() throws Exception {
//	   Settings esSettings = Settings.builder()
//                  .put("cluster.name", esClusterName)
//                  .put("client.transport.sniff", true).build();
//	   client = new PreBuiltTransportClient(esSettings);
//
//	   String[] esHosts = esHost.trim().split(",");
//	   for (String host : esHosts) {
//	       client.addTransportAddress(new TransportAddress(InetAddress.getByName(host), 
//           esPort));
//	   }
//    }
//    
//    @Bean
//    public Client client() {
//	   return client;
//    }
//    
//    
    @Bean
    public EsTemplate elasticsearchTemplate(@Qualifier("esClient") Client client) throws Exception {
	   return new EsTemplate(client);//new ElasticsearchTemplate(client);
    }
//    
//    
//    @PreDestroy
//    public void destroy() {
//	  if (client != null) {
//	       client.close();
//	   }
//    }
    
    /**

     * 注入的ElasticSearch实例

     */

    @Bean(name = "esClient")
    @PostConstruct
    public TransportClient getclient()throws Exception {

 

        Settings settings = Settings.builder()

                // 本地开发使用
                //.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH, ResourceUtils.getFile(resourcesLocations1))
                //.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH, ResourceUtils.getFile(resourcesLocations2))
                //.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH, ResourceUtils.getFile(resourcesLocations3))
                //.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_PASSWORD, password)
                .put("cluster.name",esClusterName)
                .build();
        client = new PreBuiltTransportClient(settings);
        String[] esHosts = esHost.trim().split(",");
        for (String host : esHosts) {
        	String ip = host.split(":")[0];
        	int port = Integer.parseInt(host.split(":")[1]);
        	//client.addTransportAddress(new TransportAddress(InetAddress.getByName(host), esPort));
        	client.addTransportAddress(new TransportAddress(InetAddress.getByName(ip), port));
 	   }
        
        

        // 获取连接

//        client.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet();

 

        return client;

    }
    
    
    @PreDestroy
    public void destroy() {
	  if (client != null) {
	       client.close();
	   }
    }



}
