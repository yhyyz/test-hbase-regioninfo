package test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.ByteStringer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.net.URI;

import static org.apache.hadoop.hbase.HRegionInfo.DEFAULT_REPLICA_ID;
import static org.apache.hadoop.hbase.HRegionInfo.FIRST_META_REGIONINFO;

public class ReadRegionInfoFromS3 {

    public static void main(String[] args) throws IOException {

        if (args.length!=3){
            System.out.println("please input : .regioninfo path, s3 sccessId , s3 accesskey");
            System.exit(-1);
        }
        String regionfile = args[0];
        String accessID = args[1];
        String accessKey = args[2];

        Configuration configuration = new Configuration();

        configuration.set("fs.s3n.awsAccessKeyId",accessID);
        configuration.set("fs.s3n.awsSecretAccessKey",accessKey);
        FileSystem fs = FileSystem.get(URI.create(regionfile), configuration);
        FSDataInputStream in = fs.open(new Path(regionfile));
        int pblen = ProtobufUtil.lengthOfPBMagic();
        byte [] pbuf = new byte[pblen];
        if (in.markSupported()) { //read it with mark()
            in.mark(pblen);
        }
        //assumption: if Writable serialization, it should be longer than pblen.
        int read = in.read(pbuf);
        if (read != pblen) throw new IOException("read=" + read + ", wanted=" + pblen);
        if (ProtobufUtil.isPBMagicPrefix(pbuf)) {
           System.out.println("hbase .regioninfo content: "+convert(HBaseProtos.RegionInfo.parseDelimitedFrom(in)));
        } else {
            // Presume Writables.  Need to reset the stream since it didn't start w/ pb.
            if (in.markSupported()) {

                in.reset();
                HRegionInfo hri = new HRegionInfo();
                hri.readFields(in);
                System.out.println(hri);
            } else {
                //we cannot use BufferedInputStream, it consumes more than we read from the underlying IS
                ByteArrayInputStream bais = new ByteArrayInputStream(pbuf);
                SequenceInputStream sis = new SequenceInputStream(bais, in); //concatenate input streams
                HRegionInfo hri = new HRegionInfo();
                hri.readFields(new DataInputStream(sis));
                System.out.println(hri);
            }
    }}

    public static HBaseProtos.RegionInfo convert(final HRegionInfo info) {
        if (info == null) return null;
        HBaseProtos.RegionInfo.Builder builder = HBaseProtos.RegionInfo.newBuilder();
        builder.setTableName(ProtobufUtil.toProtoTableName(info.getTable()));
        builder.setRegionId(info.getRegionId());
        if (info.getStartKey() != null) {
            builder.setStartKey(ByteStringer.wrap(info.getStartKey()));
        }
        if (info.getEndKey() != null) {
            builder.setEndKey(ByteStringer.wrap(info.getEndKey()));
        }
        builder.setOffline(info.isOffline());
        builder.setSplit(info.isSplit());
        builder.setReplicaId(info.getReplicaId());
        return builder.build();
    }


    public static HRegionInfo convert(final HBaseProtos.RegionInfo proto) {
        if (proto == null) return null;
        TableName tableName =
                ProtobufUtil.toTableName(proto.getTableName());
        if (tableName.equals(TableName.META_TABLE_NAME)) {
            return RegionReplicaUtil.getRegionInfoForReplica(FIRST_META_REGIONINFO,
                    proto.getReplicaId());
        }
        long regionId = proto.getRegionId();
        int replicaId = proto.hasReplicaId() ? proto.getReplicaId() : DEFAULT_REPLICA_ID;
        byte[] startKey = null;
        byte[] endKey = null;
        if (proto.hasStartKey()) {
            startKey = proto.getStartKey().toByteArray();
        }
        if (proto.hasEndKey()) {
            endKey = proto.getEndKey().toByteArray();
        }
        boolean split = false;
        if (proto.hasSplit()) {
            split = proto.getSplit();
        }
        HRegionInfo hri = new HRegionInfo(
                tableName,
                startKey,
                endKey, split, regionId, replicaId);
        if (proto.hasOffline()) {
            hri.setOffline(proto.getOffline());
        }
        return hri;
    }

}
