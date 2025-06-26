/*
 *  Copyright (C) 2018 Australian Institute of Marine Science
 *
 *  Contact: Gael Lafond <g.lafond@aims.gov.au>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package au.gov.aims.aws.s3.entity;

import au.gov.aims.aws.s3.S3Utils;
import au.gov.aims.aws.s3.manager.BucketManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.Grant;

import java.util.Date;
import java.util.List;

public class S3Bucket {
    private static final Logger LOGGER = Logger.getLogger(S3Bucket.class);
    private static final int S3_ATTEMPT = 5;

    private final S3Uri s3Uri;

    private Long executionTime = null;

    public S3Bucket(String bucket) {
        this.s3Uri = S3Utils.getS3URI(bucket);
    }

    public String getBucket() {
        return this.s3Uri.bucket().orElse(null);
    }

    public void setExecutionTime(Long executionTime) {
        this.executionTime = executionTime;
    }
    public Long getExecutionTime() {
        return this.executionTime;
    }

    public boolean isPublic(S3ClientWrapper client) {
        List<Grant> bucketAcl = this.getACL(client);
        if (bucketAcl == null || bucketAcl.isEmpty()) {
            return false;
        }

        return S3Utils.isPublic(bucketAcl);
    }

    public List<Grant> getACL(S3ClientWrapper client) {
        if (this.s3Uri == null) {
            return null;
        }

        String bucket = this.s3Uri.bucket().orElse(null);
        if (bucket == null || !BucketManager.bucketExists(client, bucket)) {
            return null;
        }

        GetBucketAclRequest aclRequest = GetBucketAclRequest.builder()
                    .bucket(bucket)
                    .build();

        GetBucketAclResponse aclResponse = null;
        for (int i=0; i<S3_ATTEMPT; i++) {
            try {
                aclResponse = client.getS3Client().getBucketAcl(aclRequest);
                return aclResponse.grants();
            } catch(Throwable ex) {
                LOGGER.warn(String.format("Error occurred while accessing a bucket ACL on S3: %s. Attempting to reconnect.",
                        bucket), ex);
                client.reconnect();
            }
        }
        // Try a last time, to throw the exception
        aclResponse = client.getS3Client().getBucketAcl(aclRequest);
        return aclResponse.grants();
    }

}
