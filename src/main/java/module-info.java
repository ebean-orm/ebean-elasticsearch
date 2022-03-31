module io.ebean.elastic {

    requires io.ebean.api;
    requires io.ebean.core;
    requires com.fasterxml.jackson.core;
    requires okhttp3;

    provides io.ebeanservice.docstore.api.DocStoreFactory with io.ebeanservice.elastic.ElasticDocStoreFactory;
}