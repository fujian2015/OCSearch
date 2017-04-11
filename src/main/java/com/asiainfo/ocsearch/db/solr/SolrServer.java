package com.asiainfo.ocsearch.db.solr;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by mac on 2017/3/22.
 */
public class SolrServer {

    public static final String configFile = "solr.properties";

    private CloudSolrClient solrServer;

    private SolrConfig solrConfig;

    private static SolrServer instance;

    private SolrServer() {

        this.solrConfig = new SolrConfig(configFile);

        solrServer = careateSolrClient();
    }

    public static SolrServer getInstance() {
        if (instance == null)
            instance = new SolrServer();
        return instance;
    }

    private CloudSolrClient careateSolrClient() {

        CloudSolrClient solrClient = new CloudSolrClient.Builder().withZkHost(solrConfig.getZookeeper()).build();

        solrClient.setSoTimeout(solrConfig.getSoTimeout());
        solrClient.setZkConnectTimeout(solrConfig.getZkConnectTimeout());
        solrClient.setZkClientTimeout(solrConfig.getZkClientTimeout());
        solrClient.connect();
        return solrClient;
    }

    private void createCollection(String collection, String config, int numShards, int numReplicas) throws Exception {

        CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collection, config, numShards, numReplicas);

        create.setAutoAddReplicas(true);
        create.setMaxShardsPerNode(2);
        CollectionAdminResponse response = create.process(solrServer);

        if (!response.isSuccess()) {
            throw new Exception(response.getErrorMessages().toString());
        }

    }

    public void createCollection(String collection, String config, int shards) throws Exception {

        this.createCollection(collection,config,shards,solrConfig.getReplicas());
    }


    private synchronized void query(String collection, SolrParams solrParams) {
        try {
            QueryResponse queryResponse = solrServer.query(collection, solrParams);
            for (SolrDocument solrDocument : queryResponse.getResults()) {
                solrDocument.getChildDocuments();
            }
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void uploadConfig(Path p, String name) throws IOException {
        this.solrServer.uploadConfig(p, name);
    }

    public void deleteConfig(String name) throws IOException {
        this.solrServer.getZkStateReader().getConfigManager().deleteConfigDir(name);
    }

    /**
     * close the solr client
     */
    public void close() {
        if (this.solrServer != null) {
            try {
                this.solrServer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        SolrServer.instance = null;
    }

    public void deleteCollection(String collection) throws Exception {

        CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collection);

        CollectionAdminResponse response = delete.process(solrServer);

        if (!response.isSuccess()) {
            throw new Exception(response.getErrorMessages().toString());
        }
    }

    public boolean existCollection(String collection) throws Exception {

        return solrServer.getZkStateReader().getClusterState().getCollectionsMap().containsKey(collection);

    }

    public boolean existConfig(String config) throws IOException {
        return this.solrServer.getZkStateReader().getConfigManager().configExists(config);
    }

    public boolean existsFields(String collection,Set<String> solrFieldNames) throws IOException, SolrServerException {

        SchemaRequest.Fields fields=new SchemaRequest.Fields();

        SchemaResponse.FieldsResponse response =fields.process(solrServer,collection);

        Set<String> allFields=response.getFields().stream().map(map->map.get("name").toString()).collect(Collectors.toSet());

        for(String name:solrFieldNames){
            if(!allFields.contains(name)){
                return false;
            }
        }
        return true;
    }
}
