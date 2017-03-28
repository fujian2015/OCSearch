package com.asiainfo.ocsearch.db.solr;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;

/**
 * Created by mac on 2017/3/22.
 */
public class SolrServer {

    CloudSolrClient solrServer;

    SolrConfig solrConfig;


    public SolrServer(SolrConfig solrConfig) {

        this.solrConfig = solrConfig;

        solrServer=careateSolrClient();
    }

    private CloudSolrClient careateSolrClient() {

        CloudSolrClient solrClient = new CloudSolrClient.Builder().withZkHost(solrConfig.getZookeeper()).build();

        solrClient.setSoTimeout(solrConfig.getSoTimeout());
        solrClient.setZkConnectTimeout(solrConfig.getZkConnectTimeout());
        solrClient.setZkClientTimeout(solrConfig.getZkClientTimeout());
        solrClient.connect();
        return solrClient;
    }

    private boolean createCollection(String collection, String config, int numShards, int numReplicas) {

//        solrServer.uploadConfig();

//        solrServer.getZkStateReader().getConfigManager().uploadConfigDir();

        boolean isSucess = false;
        CollectionAdminRequest.Create request = CollectionAdminRequest.createCollection(collection, config, numShards, numReplicas);
        try {
            NamedList result = solrServer.request(request);

            isSucess = result.get("sucess") != null ? true : false;
            System.out.println(request);

        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isSucess;
    }

    private  synchronized void  query(String collection, SolrParams solrParams) {
        try {
            QueryResponse queryResponse=solrServer.query(collection,solrParams);
            for(SolrDocument solrDocument:queryResponse.getResults()){
                solrDocument.getChildDocuments();
            }
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
