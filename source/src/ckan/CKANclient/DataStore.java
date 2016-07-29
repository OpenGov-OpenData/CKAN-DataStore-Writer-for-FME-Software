package ckan.CKANclient;

import java.util.LinkedHashMap;
import java.util.List;

public class DataStore{

    public class Response {
        public boolean success;
        public DataStore result;
    }

    private String resource_id;
    private Resource resource;
    private List<Field> fields;
    private List<LinkedHashMap<String, Object>> records;
    private String method;
    private String force;
    private List<String> aliases;
    private List<String> primary_key;
    private List<String> indexes;
    
    
    public DataStore() {}

    public void setResource_id(String resource_id) {
        this.resource_id = resource_id;
    }
    
    public String getResource_id() {
        return resource_id;
    }
    
    public Resource getResource() {
        return resource;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public List<Field> getFields() {
        return fields;
    }
    
    public void setRecords(List<LinkedHashMap<String, Object>> records) {
        this.records = records;
    }

    public List<LinkedHashMap<String, Object>> getRecords() {
        return records;
    }
    
    public void setMethod(String method) {
    	if (method.equals("upsert") || method.equals("insert") || method.equals("update")) {
    		this.method = method;
    	}
    }
    
    public String getMethod() {
        return method;
    }
    
    public void setForce(String force) {
    	if (force.equals("True") || force.equals("False")) {
    		this.force = force;
    	}
    }
    
    public String getForce() {
        return force;
    }
    
    public List<String> getAliases() {
        return aliases;
    }

    public void setAliases( List<String> aliases ) {
        this.aliases = aliases;
    }
    
    public List<String> getPrimary_key() {
        return primary_key;
    }

    public void setPrimary_key( List<String> primary_key ) {
        this.primary_key = primary_key;
    }
    
    public List<String> getIndexes() {
        return indexes;
    }

    public void setIndexes( List<String> indexes ) {
        this.indexes = indexes;
    }
}
