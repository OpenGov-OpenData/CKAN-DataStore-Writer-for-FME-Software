package ckan.CKANclient;

public class Field {

    private String id;
    private String value;

    public String getValue() { return value; }
    public void setValue(String v) { value = v; }

    public Field() {}

    public Field(String v) {
        id = v;
    }

    public String toString() {
        return "<Field: id=" + this.getValue() + ">";
    }
}
