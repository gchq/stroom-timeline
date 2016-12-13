package stroom.timeline.hbase;

public final class HBaseConnectionImplBuilder {
    private int zkClientPort = 2181;
    private String zkQuorum = "localhost";
    private String zkParent = "/hbase";
    private String hbaseHost = "localhost";
    private int hbasePort = 60000;

    private HBaseConnectionImplBuilder() {
    }

    public static HBaseConnectionImplBuilder instance() {
        return new HBaseConnectionImplBuilder();
    }

    /**
     * Defaults to '2181'
     */
    public HBaseConnectionImplBuilder withZkClientPort(int zkClientPort) {
        this.zkClientPort = zkClientPort;
        return this;
    }

    /**
     * Defaults to 'localhost'
     */
    public HBaseConnectionImplBuilder withZkQuorum(String zkQuorum) {
        this.zkQuorum = zkQuorum;
        return this;
    }

    /**
     * Defaults to '/hbase'
     */
    public HBaseConnectionImplBuilder withZkParent(String zkParent) {
        this.zkParent = zkParent;
        return this;
    }

    /**
     * Defaults to 'localhost'
     */
    public HBaseConnectionImplBuilder withHbaseHost(String hbaseHost) {
        this.hbaseHost = hbaseHost;
        return this;
    }

    /**
     * Defaults to '60000'
     */
    public HBaseConnectionImplBuilder withHbasePort(int hbasePort) {
        this.hbasePort = hbasePort;
        return this;
    }

    public HBaseConnectionImpl build() {
        return new HBaseConnectionImpl(zkClientPort, zkQuorum, zkParent, hbaseHost, hbasePort);
    }
}