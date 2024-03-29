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
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AccessControlList;
import org.apache.log4j.Logger;

import java.util.Date;

public class S3Bucket {
    private static final Logger LOGGER = Logger.getLogger(S3Bucket.class);
    private static final int S3_ATTEMPT = 5;

    private final AmazonS3URI s3Uri;

    private Long executionTime = null;
    private Long creationTime = null;

    public S3Bucket(String bucketId) {
        this.s3Uri = S3Utils.getS3URI(bucketId);
    }

    public String getBucketId() {
        return this.s3Uri.getBucket();
    }

    public void setExecutionTime(Long executionTime) {
        this.executionTime = executionTime;
    }
    public Long getExecutionTime() {
        return this.executionTime;
    }

    public void setCreationTime(Long creationTime) {
        this.creationTime = creationTime;
    }
    public Long getCreationTime() {
        return this.creationTime;
    }
    public void setCreationTime(Date creationDate) {
        if (creationDate == null) {
            this.creationTime = null;
        } else {
            this.setCreationTime(creationDate.getTime());
        }
    }

    public boolean isPublic(S3Client client) {
        AccessControlList bucketAcl = this.getACL(client);
        if (bucketAcl == null) {
            return false;
        }

        return S3Utils.isPublic(bucketAcl);
    }

    public AccessControlList getACL(S3Client client) {
        if (this.s3Uri == null) {
            return null;
        }

        String bucketId = this.s3Uri.getBucket();
        if (bucketId == null || !BucketManager.doesBucketExist(client, bucketId)) {
            return null;
        }

        for (int i=0; i<S3_ATTEMPT; i++) {
            try {
                return client.getS3().getBucketAcl(bucketId);
            } catch(Throwable ex) {
                LOGGER.warn(String.format("Error occurred while accessing a bucket ACL on S3: %s. Attempting to reconnect.",
                        bucketId), ex);
                client.reconnect();
            }
        }
        // Try a last time, to throw the exception
        return client.getS3().getBucketAcl(bucketId);
    }

}
